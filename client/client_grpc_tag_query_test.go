package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/largepayload"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/internal/tagcodec"
	"github.com/microsoft/durabletask-go/payload"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// pagedQueryServer serves a deterministic, unbounded stream of single-instance
// query pages so the client-side tag filter scan cap can be observed exactly.
type pagedQueryServer struct {
	protos.UnimplementedTaskHubSidecarServiceServer

	// matchEvery makes every Nth served instance carry the searched tag. Zero
	// means no instance ever matches.
	matchEvery int
	// totalPages bounds the stream. Zero means the stream never ends.
	totalPages int
	pageTags   []map[string]string
	pageStates [][]*protos.OrchestrationState

	mu       sync.Mutex
	requests []string
}

func (s *pagedQueryServer) QueryInstances(
	_ context.Context,
	req *protos.QueryInstancesRequest,
) (*protos.QueryInstancesResponse, error) {
	token := req.GetQuery().GetContinuationToken().GetValue()
	s.mu.Lock()
	s.requests = append(s.requests, token)
	served := len(s.requests)
	s.mu.Unlock()

	tags := map[string]string{"group": "other"}
	if s.matchEvery > 0 && served%s.matchEvery == 0 {
		tags = map[string]string{"group": "wanted"}
	}
	if served <= len(s.pageTags) {
		tags = s.pageTags[served-1]
	}
	resp := &protos.QueryInstancesResponse{
		OrchestrationState: []*protos.OrchestrationState{{
			InstanceId:          fmt.Sprintf("instance-%04d", served),
			Name:                "Paged",
			OrchestrationStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
			Tags:                tags,
		}},
	}
	if served <= len(s.pageStates) {
		resp.OrchestrationState = s.pageStates[served-1]
	}
	if s.totalPages == 0 || served < s.totalPages {
		resp.ContinuationToken = wrapperspb.String(fmt.Sprintf("token-%04d", served))
	}
	return resp, nil
}

func (s *pagedQueryServer) requestTokens() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.requests...)
}

func startQueryClient(t *testing.T, server protos.TaskHubSidecarServiceServer) *TaskHubGrpcClient {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	protos.RegisterTaskHubSidecarServiceServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(grpcServer.Stop)

	connection, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })
	return NewTaskHubGrpcClient(connection, api.DefaultLogger())
}

// TestQueryInstancesTagFilterHonoursScanPageCap asserts the documented remote
// tag filter contract: because the wire query has no tag predicate, the client
// scans at most api.MaxRemoteTagFilterScanPages service pages per call and then
// returns a short page plus a continuation token the caller can resume from.
func TestQueryInstancesTagFilterHonoursScanPageCap(t *testing.T) {
	for _, test := range []struct {
		name             string
		matchEvery       int
		pageSize         int
		wantMatches      int
		wantServedPages  int
		wantTokenPresent bool
	}{
		{
			name:             "no-matches-stops-at-cap",
			pageSize:         5,
			wantMatches:      0,
			wantServedPages:  api.MaxRemoteTagFilterScanPages,
			wantTokenPresent: true,
		},
		{
			name:             "sparse-matches-stop-at-cap",
			matchEvery:       50,
			pageSize:         5,
			wantMatches:      2,
			wantServedPages:  api.MaxRemoteTagFilterScanPages,
			wantTokenPresent: true,
		},
		{
			name:             "dense-matches-fill-the-page-before-the-cap",
			matchEvery:       1,
			pageSize:         5,
			wantMatches:      5,
			wantServedPages:  5,
			wantTokenPresent: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := &pagedQueryServer{matchEvery: test.matchEvery}
			client := startQueryClient(t, server)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			result, err := client.QueryInstances(ctx, api.OrchestrationQuery{
				PageSize: test.pageSize,
				Tags:     map[string]string{"group": "wanted"},
			})
			require.NoError(t, err)
			require.Len(t, result.Orchestrations, test.wantMatches)
			require.Len(t, server.requestTokens(), test.wantServedPages)
			require.Equal(t, test.wantTokenPresent, result.ContinuationToken != "")
		})
	}
}

// TestQueryInstancesTagFilterResumesAfterScanPageCap asserts the continuation
// token returned by a capped scan resumes exactly where the previous scan
// stopped, so callers can keep paging without losing or repeating instances.
func TestQueryInstancesTagFilterResumesAfterScanPageCap(t *testing.T) {
	server := &pagedQueryServer{}
	client := startQueryClient(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	query := api.OrchestrationQuery{PageSize: 5, Tags: map[string]string{"group": "wanted"}}
	first, err := client.QueryInstances(ctx, query)
	require.NoError(t, err)
	require.Empty(t, first.Orchestrations)
	require.Equal(t, fmt.Sprintf("token-%04d", api.MaxRemoteTagFilterScanPages), first.ContinuationToken)

	query.ContinuationToken = first.ContinuationToken
	second, err := client.QueryInstances(ctx, query)
	require.NoError(t, err)
	require.Empty(t, second.Orchestrations)
	require.Equal(t, fmt.Sprintf("token-%04d", 2*api.MaxRemoteTagFilterScanPages), second.ContinuationToken)

	tokens := server.requestTokens()
	require.Len(t, tokens, 2*api.MaxRemoteTagFilterScanPages)
	// The resumed scan starts from the token the capped scan handed back.
	require.Equal(t, "", tokens[0])
	require.Equal(t, first.ContinuationToken, tokens[api.MaxRemoteTagFilterScanPages])
}

// TestQueryInstancesWithoutTagsIgnoresScanPageCap asserts the scan cap only
// exists to bound client-side tag filtering: an untagged query pages until the
// requested page size is filled.
func TestQueryInstancesWithoutTagsIgnoresScanPageCap(t *testing.T) {
	server := &pagedQueryServer{}
	client := startQueryClient(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pageSize := api.MaxRemoteTagFilterScanPages + 7
	result, err := client.QueryInstances(ctx, api.OrchestrationQuery{PageSize: pageSize})
	require.NoError(t, err)
	require.Len(t, result.Orchestrations, pageSize)
	require.Len(t, server.requestTokens(), pageSize)
	require.NotEmpty(t, result.ContinuationToken)
}

// TestQueryInstancesTagFilterStopsWhenServiceExhausted asserts a capped tag scan
// that reaches the end of the service results returns no continuation token.
func TestQueryInstancesTagFilterStopsWhenServiceExhausted(t *testing.T) {
	server := &pagedQueryServer{matchEvery: 3, totalPages: 10}
	client := startQueryClient(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := client.QueryInstances(ctx, api.OrchestrationQuery{
		PageSize: 50,
		Tags:     map[string]string{"group": "wanted"},
	})
	require.NoError(t, err)
	require.Len(t, result.Orchestrations, 3)
	require.Empty(t, result.ContinuationToken)
	require.Len(t, server.requestTokens(), 10)
}

func TestQueryInstancesEmptyTagRequiresPresenceAcrossPages(t *testing.T) {
	server := &pagedQueryServer{
		totalPages: 3,
		pageTags: []map[string]string{
			nil,
			{"flag": ""},
			{"flag": "set"},
		},
	}
	client := startQueryClient(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	query := api.OrchestrationQuery{PageSize: 1, Tags: map[string]string{"flag": ""}}
	first, err := client.QueryInstances(ctx, query)
	require.NoError(t, err)
	require.Len(t, first.Orchestrations, 1)
	require.Equal(t, api.InstanceID("instance-0002"), first.Orchestrations[0].InstanceID)
	require.NotEmpty(t, first.ContinuationToken)

	query.ContinuationToken = first.ContinuationToken
	second, err := client.QueryInstances(ctx, query)
	require.NoError(t, err)
	require.Empty(t, second.Orchestrations)
	require.Empty(t, second.ContinuationToken)
	require.Len(t, server.requestTokens(), 3)
}

// nonAdvancingQueryServer always echoes the same continuation token, which the
// client must reject instead of looping forever.
type nonAdvancingQueryServer struct {
	protos.UnimplementedTaskHubSidecarServiceServer
}

func (*nonAdvancingQueryServer) QueryInstances(
	_ context.Context,
	req *protos.QueryInstancesRequest,
) (*protos.QueryInstancesResponse, error) {
	token := req.GetQuery().GetContinuationToken().GetValue()
	if token == "" {
		token = "stuck"
	}
	return &protos.QueryInstancesResponse{ContinuationToken: wrapperspb.String(token)}, nil
}

func TestQueryInstancesRejectsNonAdvancingContinuationToken(t *testing.T) {
	client := startQueryClient(t, &nonAdvancingQueryServer{})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := client.QueryInstances(ctx, api.OrchestrationQuery{
		PageSize:          5,
		ContinuationToken: "stuck",
	})
	require.ErrorContains(t, err, "non-advancing continuation token")
}

type queryPayloadResolver struct {
	calls atomic.Int32
	err   error
}

func (r *queryPayloadResolver) Resolve(context.Context, string) ([]byte, error) {
	r.calls.Add(1)
	return nil, r.err
}

func TestQueryInstancesFiltersTagsBeforePayloadHydration(t *testing.T) {
	for _, tags := range []struct {
		name               string
		rejected, matching map[string]string
		query              map[string]string
	}{
		{"plain", map[string]string{"group": "other"}, map[string]string{"group": "wanted"}, map[string]string{"group": "wanted"}},
		{"encoded", map[string]string{tagcodec.UserTagPrefix + "group": "other"}, map[string]string{tagcodec.UserTagPrefix + "group": "wanted"}, map[string]string{"group": "wanted"}},
		{"context only", map[string]string{tagcodec.ContextFieldPrefix + "group": "wanted"}, map[string]string{"group": "wanted"}, map[string]string{"group": "wanted"}},
		{"empty value", nil, map[string]string{"flag": ""}, map[string]string{"flag": ""}},
	} {
		for _, unreadable := range []string{"rejected row", "matching input", "matching output", "matching status"} {
			t.Run(tags.name+"/"+unreadable, func(t *testing.T) {
				store := payload.NewMemoryStore()
				options := &api.LargePayloadOptions{Store: store, Resolver: store, ThresholdBytes: 1, MaxPayloadBytes: 1024}
				reference, err := largepayload.Externalize(t.Context(), options, wrapperspb.String(`"external"`))
				require.NoError(t, err)
				rejected := &protos.OrchestrationState{
					InstanceId: "rejected", Tags: tags.rejected,
					Input: reference, Output: reference, CustomStatus: reference,
				}
				matching := &protos.OrchestrationState{
					InstanceId: "matching", Tags: tags.matching,
					Input: wrapperspb.String(`"input"`), Output: wrapperspb.String(`"output"`),
					CustomStatus: wrapperspb.String(`"status"`),
				}
				switch unreadable {
				case "matching input":
					matching.Input = reference
				case "matching output":
					matching.Output = reference
				case "matching status":
					matching.CustomStatus = reference
				}
				resolver := &queryPayloadResolver{err: errors.New("unreadable query payload")}
				options.Resolver = resolver
				server := &pagedQueryServer{
					totalPages: 1,
					pageStates: [][]*protos.OrchestrationState{{rejected, matching}},
				}
				client := startQueryClient(t, server)
				client.largePayloads = options
				result, err := client.QueryInstances(t.Context(), api.OrchestrationQuery{
					PageSize: 2, Tags: tags.query, FetchInputsAndOutputs: true,
				})
				if unreadable != "rejected row" {
					require.ErrorIs(t, err, resolver.err)
					require.Nil(t, result)
					require.EqualValues(t, 1, resolver.calls.Load())
					return
				}
				require.NoError(t, err)
				require.Zero(t, resolver.calls.Load())
				require.Len(t, result.Orchestrations, 1)
				require.Equal(t, api.InstanceID("matching"), result.Orchestrations[0].InstanceID)
				require.Equal(t, tags.query, result.Orchestrations[0].Tags)
				require.Equal(t, `"input"`, result.Orchestrations[0].SerializedInput)
				require.Equal(t, `"output"`, result.Orchestrations[0].SerializedOutput)
				require.Equal(t, `"status"`, result.Orchestrations[0].SerializedCustomStatus)
				require.Empty(t, result.ContinuationToken)
				require.Len(t, server.requestTokens(), 1)
			})
		}
	}
}

type queryResponseClient struct {
	protos.TaskHubSidecarServiceClient
	response *protos.QueryInstancesResponse
}

func (c *queryResponseClient) QueryInstances(context.Context, *protos.QueryInstancesRequest, ...grpc.CallOption) (*protos.QueryInstancesResponse, error) {
	return c.response, nil
}

func TestQueryInstancesRejectsNilResponsesAndRowsBeforeFiltering(t *testing.T) {
	for _, test := range []struct {
		name     string
		response *protos.QueryInstancesResponse
		message  string
	}{
		{"response", nil, "nil response"},
		{"row", &protos.QueryInstancesResponse{OrchestrationState: []*protos.OrchestrationState{nil}}, "orchestration state is nil"},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &TaskHubGrpcClient{client: &queryResponseClient{response: test.response}}
			result, err := client.QueryInstances(t.Context(), api.OrchestrationQuery{Tags: map[string]string{"group": "wanted"}})
			require.ErrorContains(t, err, test.message)
			require.Nil(t, result)
		})
	}
}
