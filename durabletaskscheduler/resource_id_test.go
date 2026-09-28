package durabletaskscheduler

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

var resourceIDCases = []struct {
	name       string
	region     string
	resourceID string
	want       string
}{
	{name: "unset region", want: DefaultResourceID},
	{name: "empty region", want: DefaultResourceID},
	{name: "public", region: "westus2", want: DefaultResourceID},
	{name: "china is not inferred", region: "chinaeast2", want: DefaultResourceID},
	{name: "government substring", region: "notusgov", want: DefaultResourceID},
	{name: "DoD substring", region: "notusdod", want: DefaultResourceID},
	{name: "government", region: "usgovvirginia", want: GovernmentResourceID},
	{name: "government uppercase", region: "USGOVARIZONA", want: GovernmentResourceID},
	{name: "government mixed case", region: "UsGovTexas", want: GovernmentResourceID},
	{name: "DoD", region: "usdodcentral", want: GovernmentResourceID},
	{name: "DoD uppercase", region: "USDODEAST", want: GovernmentResourceID},
	{name: "DoD mixed case", region: "UsDodCentral", want: GovernmentResourceID},
	{name: "exact government prefix", region: "usgov", want: GovernmentResourceID},
	{name: "exact DoD prefix", region: "usdod", want: GovernmentResourceID},
	{name: "leading space is not a prefix", region: " usgovvirginia", want: DefaultResourceID},
	{name: "public override government", region: "usgovvirginia", resourceID: DefaultResourceID, want: DefaultResourceID},
	{name: "public override DoD", region: "usdodcentral", resourceID: DefaultResourceID, want: DefaultResourceID},
	{name: "government override public", region: "westus2", resourceID: GovernmentResourceID, want: GovernmentResourceID},
	{name: "custom", region: "chinaeast2", resourceID: "https://Custom.Example/Resource", want: "https://Custom.Example/Resource"},
	{name: "trailing slash", resourceID: GovernmentResourceID + "/", want: GovernmentResourceID},
	{name: "existing suffix", resourceID: GovernmentResourceID + "/.default", want: GovernmentResourceID},
	{name: "uppercase suffix and slashes", resourceID: GovernmentResourceID + "//.DEFAULT//", want: GovernmentResourceID},
	{name: "surrounding whitespace", resourceID: " \t\n" + GovernmentResourceID + "/.default/ \r\n", want: GovernmentResourceID},
	{name: "custom URI casing", region: "usgovvirginia", resourceID: "api://CustomAudience/resource/.DEFAULT/", want: "api://CustomAudience/resource"},
	{name: "strip only one suffix", resourceID: "api://custom/.default/.default", want: "api://custom/.default"},
	{name: "preserve meaningful suffix casing", resourceID: "api://custom/.DEFAULT//.default/", want: "api://custom/.DEFAULT"},
	{name: "preserve equals", resourceID: "api://Custom/resource=value", want: "api://Custom/resource=value"},
}

func setResourceRegion(t *testing.T, name, region string) {
	t.Helper()
	t.Setenv("REGION_NAME", region)
	if name == "unset region" {
		require.NoError(t, os.Unsetenv("REGION_NAME"))
	}
}

func recordedTokenOptions(c *recordingCredential) []policy.TokenRequestOptions {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]policy.TokenRequestOptions(nil), c.options...)
}

func assertResourceScopeAndRefresh(t *testing.T, options *Options, want string) {
	t.Helper()
	for _, role := range []connectionRole{clientRole, workerRole} {
		credential := &recordingCredential{}
		prepared, err := prepareOptionsWith(options, func(credentialSpec) (azcore.TokenCredential, error) {
			return credential, nil
		})
		require.NoError(t, err)
		if options.Authentication == AuthenticationTokenCredential {
			var ok bool
			credential, ok = prepared.Credential.(*recordingCredential)
			require.True(t, ok)
		}
		before := len(recordedTokenOptions(credential))
		perRPC := newPerRPCCredentials(&prepared, role, "worker")
		require.Len(t, recordedTokenOptions(credential), before, "construction must not acquire a token")
		for range 2 {
			values, err := perRPC.GetRequestMetadata(context.Background())
			require.NoError(t, err)
			if options.Authentication == AuthenticationNone {
				require.NotContains(t, values, "authorization")
			} else {
				require.Equal(t, "Bearer token", values["authorization"])
			}
		}
		if options.Authentication == AuthenticationNone {
			require.Empty(t, recordedTokenOptions(credential))
			continue
		}
		require.Len(t, recordedTokenOptions(credential), before+1, "fresh tokens must be cached")
		perRPC.token.Store(&schedulerCredentialState{
			refreshAfter: time.Now().Add(-time.Hour),
			validUntil:   time.Now().Add(-time.Minute),
		})
		errs := callSchedulerCredentialsConcurrently(perRPC, 16)
		for range 16 {
			require.NoError(t, <-errs)
		}
		calls := recordedTokenOptions(credential)[before:]
		require.Len(t, calls, 2)
		for _, call := range calls {
			require.Equal(t, []string{want + "/.default"}, call.Scopes)
			require.Empty(t, call.TenantID)
		}
	}
}

func TestResourceIDScopesForEveryAuthenticationMode(t *testing.T) {
	for _, tt := range resourceIDCases {
		t.Run(tt.name, func(t *testing.T) {
			setResourceRegion(t, tt.name, tt.region)
			for _, authentication := range allAuthenticationTypes {
				t.Run(string(authentication), func(t *testing.T) {
					for _, source := range []string{"options", "connection string"} {
						if source == "connection string" && authentication == AuthenticationTokenCredential {
							continue
						}
						t.Run(source, func(t *testing.T) {
							options := NewOptions("https://scheduler.example.com", "hub")
							options.Authentication = authentication
							options.ResourceID = tt.resourceID
							if source == "connection string" {
								var err error
								options, err = NewOptionsFromConnectionString(
									"Endpoint=https://scheduler.example.com;TaskHub=hub;Authentication=" +
										string(authentication) + ";ResourceId=" + tt.resourceID,
								)
								require.NoError(t, err)
							}
							if authentication == AuthenticationTokenCredential {
								options.Credential = &recordingCredential{}
							}
							original := options.ResourceID
							assertResourceScopeAndRefresh(t, options, tt.want)
							require.Equal(t, original, options.ResourceID, "preparation must not mutate caller options")
							require.Equal(t, "https://scheduler.example.com", options.EndpointAddress)
							require.Empty(t, options.AuthorityHost)
						})
					}
				})
			}
		})
	}
}

func TestResourceIDDefaultsArePerInstance(t *testing.T) {
	for _, source := range []string{"NewOptions", "NewOptionsWithCredential", "connection string", "connection string empty", "literal", "reset empty"} {
		t.Run(source, func(t *testing.T) {
			newOptions := func() *Options {
				credential := &recordingCredential{}
				switch source {
				case "NewOptions":
					return NewOptions("scheduler.example.com", "hub")
				case "NewOptionsWithCredential":
					return NewOptionsWithCredential("scheduler.example.com", "hub", credential)
				case "connection string", "connection string empty":
					connectionString := "Endpoint=scheduler.example.com;TaskHub=hub;Authentication=DefaultAzure"
					if source == "connection string empty" {
						connectionString += ";ResourceId="
					}
					options, err := NewOptionsFromConnectionString(connectionString)
					require.NoError(t, err)
					return options
				case "literal":
					return &Options{EndpointAddress: "scheduler.example.com", TaskHubName: "hub"}
				default:
					options := NewOptions("scheduler.example.com", "hub")
					options.ResourceID = ""
					return options
				}
			}
			var instances []Options
			for _, region := range []string{"UsGovVirginia", "westus2", "USDODEAST"} {
				t.Setenv("REGION_NAME", region)
				options := newOptions()
				if source != "literal" && source != "reset empty" {
					t.Setenv("REGION_NAME", "changed-after-options")
				}
				prepared, err := prepareOptionsWith(options, func(credentialSpec) (azcore.TokenCredential, error) {
					return &recordingCredential{}, nil
				})
				require.NoError(t, err)
				instances = append(instances, prepared)
			}
			t.Setenv("REGION_NAME", "chinaeast2")
			for i := range instances {
				want := GovernmentResourceID
				if i == 1 {
					want = DefaultResourceID
				}
				assertResourceScopeAndRefresh(t, &instances[i], want)
			}
		})
	}
}

func TestInvalidResourceIDsFailBeforeAuthentication(t *testing.T) {
	for _, resourceID := range []string{" \t ", "\r\n", "///", "/.default", "/.DEFAULT///", " /.DEFAULT/// "} {
		t.Run(resourceID, func(t *testing.T) {
			for _, authentication := range allAuthenticationTypes {
				options := NewOptions("scheduler.example.com", "hub")
				options.Authentication = authentication
				options.ResourceID = resourceID
				require.ErrorContains(t, options.Validate(), "resource ID cannot be empty after normalization")
				_, err := prepareOptionsWith(options, func(credentialSpec) (azcore.TokenCredential, error) {
					t.Fatal("invalid ResourceID must be rejected before credential construction")
					return nil, nil
				})
				require.ErrorContains(t, err, "resource ID cannot be empty after normalization")
				client, err := NewClient(context.Background(), options, nil)
				require.Nil(t, client)
				require.ErrorContains(t, err, "resource ID cannot be empty after normalization")
				worker, err := NewWorker(options, task.NewTaskRegistry(), nil)
				require.Nil(t, worker)
				require.ErrorContains(t, err, "resource ID cannot be empty after normalization")
				if authentication != AuthenticationTokenCredential {
					_, err = NewOptionsFromConnectionString("Endpoint=scheduler.example.com;TaskHub=hub;Authentication=" +
						string(authentication) + ";ResourceId=" + resourceID)
					require.ErrorContains(t, err, "resource ID cannot be empty after normalization")
				}
			}
		})
	}
}

func TestResourceIDConnectionStringKeysAndDuplicates(t *testing.T) {
	t.Setenv("REGION_NAME", "usgovvirginia")
	for _, key := range []string{"ResourceId", "resourceid", "RESOURCEID", "ReSoUrCeId"} {
		for _, value := range []string{"", "api://Custom/.default/.DEFAULT/"} {
			options, err := NewOptionsFromConnectionString(
				"Endpoint=scheduler.example.com;TaskHub=hub;Authentication=DefaultAzure;ResourceId=///;" + key + "=" + value,
			)
			require.NoError(t, err)
			want := "api://Custom/.default"
			if value == "" {
				want = GovernmentResourceID
			}
			assertResourceScopeAndRefresh(t, options, want)
		}
		_, err := NewOptionsFromConnectionString(
			"Endpoint=scheduler.example.com;TaskHub=hub;Authentication=None;ResourceId=api://valid;" + key + "=  ",
		)
		require.ErrorContains(t, err, "resource ID cannot be empty after normalization")
	}
}

// Called only in an isolated test subprocess because fallback roots are
// process-global and cannot be restored. No system certificate is installed.
func audienceTestTLS() *tls.Config {
	server := httptest.NewTLSServer(nil)
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	x509.SetFallbackRoots(roots)
	return server.TLS.Clone()
}

type audienceServer struct {
	metadataServer
	disconnect chan struct{}
}

func (s *audienceServer) RaiseEvent(context.Context, *protos.RaiseEventRequest) (*protos.RaiseEventResponse, error) {
	return nil, status.Error(codes.Unavailable, "replace the test channel")
}

func (s *audienceServer) GetWorkItems(_ *protos.GetWorkItemsRequest, stream protos.TaskHubSidecarService_GetWorkItemsServer) error {
	incoming, _ := metadata.FromIncomingContext(stream.Context())
	s.metadata <- incoming
	select {
	case <-s.disconnect:
		return status.Error(codes.Unavailable, "reconnect the test worker")
	case <-stream.Context().Done():
		return nil
	}
}

func awaitAudienceMetadata(t *testing.T, server *audienceServer) metadata.MD {
	t.Helper()
	select {
	case incoming := <-server.metadata:
		require.Equal(t, []string{"Bearer token"}, incoming.Get("authorization"))
		return incoming
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for authenticated RPC")
		return nil
	}
}

func TestPublicAuthenticationPathsPreserveResourceIDAcrossReconnects(t *testing.T) {
	if os.Getenv("DTS_AUDIENCE_TLS_TEST") != "1" {
		command := exec.Command(os.Args[0], "-test.run=^TestPublicAuthenticationPathsPreserveResourceIDAcrossReconnects$", "-test.timeout=2m")
		command.Env = append(os.Environ(), "DTS_AUDIENCE_TLS_TEST=1")
		output, err := command.CombinedOutput()
		require.NoError(t, err, "%s", output)
		return
	}
	t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",x509usefallbackroots=1")
	tlsConfig := audienceTestTLS()
	for _, tt := range resourceIDCases {
		t.Run(tt.name, func(t *testing.T) {
			for _, path := range []string{"client", "worker", "compatibility listener"} {
				t.Run(path, func(t *testing.T) {
					setResourceRegion(t, tt.name, tt.region)
					server := &audienceServer{
						metadataServer: metadataServer{metadata: make(chan metadata.MD, 16)},
						disconnect:     make(chan struct{}, 1),
					}
					listener := bufconn.Listen(1024 * 1024)
					grpcServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)))
					protos.RegisterTaskHubSidecarServiceServer(grpcServer, server)
					go func() { _ = grpcServer.Serve(listener) }()
					defer grpcServer.Stop()
					defer func() { require.NoError(t, listener.Close()) }()

					credential := &recordingCredential{}
					if tt.name == "government" {
						credential.refreshOn = time.Now().Add(-time.Hour)
					}
					options := NewOptionsWithCredential("https://127.0.0.1", "hub", credential)
					options.ResourceID = tt.resourceID
					options.dialer = bufconnDialer(listener)
					options.ChannelRecreateFailureThreshold = 1
					options.ChannelRecreateMinInterval = 0
					connections := make(chan *grpc.ClientConn, 1)
					options.UnaryInterceptors = []grpc.UnaryClientInterceptor{
						func(ctx context.Context, method string, request, reply any, connection *grpc.ClientConn, invoker grpc.UnaryInvoker, callOptions ...grpc.CallOption) error {
							select {
							case connections <- connection:
							default:
							}
							return invoker(ctx, method, request, reply, connection, callOptions...)
						},
					}
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					wantTokenCalls := 2
					refreshTransport := func() {
						if tt.name != "government" {
							return
						}
						connection := <-connections
						// Exercise refresh on the same public transport after its
						// minimum one-second cache interval, before recreating it.
						require.Eventually(t, func() bool {
							_, err := protos.NewTaskHubSidecarServiceClient(connection).Hello(ctx, &emptypb.Empty{})
							require.NoError(t, err)
							awaitAudienceMetadata(t, server)
							return len(recordedTokenOptions(credential)) == 2
						}, 3*time.Second, 20*time.Millisecond)
						wantTokenCalls++
					}

					if path == "worker" {
						worker, err := NewWorker(options, task.NewTaskRegistry(), nil,
							durabletaskclient.WithWorkerReconnectBackoff(time.Millisecond, time.Millisecond))
						require.NoError(t, err)
						require.Empty(t, recordedTokenOptions(credential), "worker token acquisition stays lazy")
						t.Setenv("REGION_NAME", "changed-after-worker-construction")
						options.ResourceID = "api://changed-after-worker-construction"
						require.NoError(t, worker.Start(ctx))
						defer func() { require.NoError(t, worker.Shutdown(ctx)) }()
						hello := awaitAudienceMetadata(t, server)
						stream := awaitAudienceMetadata(t, server)
						require.Equal(t, hello.Get("workerid"), stream.Get("workerid"))
						require.Len(t, recordedTokenOptions(credential), 1)
						refreshTransport()
						server.disconnect <- struct{}{}
						reconnectedHello := awaitAudienceMetadata(t, server)
						reconnectedStream := awaitAudienceMetadata(t, server)
						require.Equal(t, hello.Get("workerid"), reconnectedHello.Get("workerid"))
						require.Equal(t, hello.Get("workerid"), reconnectedStream.Get("workerid"))
					} else {
						client, err := NewClient(ctx, options, nil)
						require.NoError(t, err)
						defer func() { require.NoError(t, client.Close()) }()
						awaitAudienceMetadata(t, server)
						require.Len(t, recordedTokenOptions(credential), 1, "client acquires a token for its eager Hello")
						if path == "compatibility listener" {
							require.NoError(t, client.StartWorkItemListener(ctx, task.NewTaskRegistry(),
								durabletaskclient.WithWorkerReconnectBackoff(time.Millisecond, time.Millisecond)))
							awaitAudienceMetadata(t, server)
							awaitAudienceMetadata(t, server)
							require.Len(t, recordedTokenOptions(credential), 1, "listener shares the client's cached token")
						}
						t.Setenv("REGION_NAME", "changed-after-client-construction")
						options.ResourceID = "api://changed-after-client-construction"
						refreshTransport()
						client.connection.mu.Lock()
						old := client.connection.current
						client.connection.mu.Unlock()
						require.Error(t, client.RaiseEvent(ctx, api.InstanceID("test"), "recreate"))
						awaitAudienceMetadata(t, server)
						// The replacement Hello can arrive before the swap completes.
						require.Eventually(t, func() bool {
							client.connection.mu.Lock()
							defer client.connection.mu.Unlock()
							return !client.connection.recreateInFlight && client.connection.current != old
						}, 5*time.Second, time.Millisecond)
						if path == "compatibility listener" {
							server.disconnect <- struct{}{}
							awaitAudienceMetadata(t, server)
							awaitAudienceMetadata(t, server)
						}
					}
					calls := recordedTokenOptions(credential)
					require.Len(t, calls, wantTokenCalls)
					for _, call := range calls {
						require.Equal(t, []string{tt.want + "/.default"}, call.Scopes)
					}
					require.Empty(t, options.AuthorityHost)
					require.Equal(t, "https://127.0.0.1", options.EndpointAddress)
				})
			}
		})
	}
}

func TestResourceIDDoesNotInferAudienceFromEndpoint(t *testing.T) {
	t.Setenv("REGION_NAME", "")
	for _, endpoint := range []string{"https://scheduler.durabletask.azure.us", "https://scheduler.durabletask.io"} {
		options := NewOptions(endpoint, "hub")
		assertResourceScopeAndRefresh(t, options, DefaultResourceID)
	}
}
