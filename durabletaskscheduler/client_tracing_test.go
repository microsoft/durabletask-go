package durabletaskscheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
)

type startRecordingServer struct {
	protos.UnimplementedTaskHubSidecarServiceServer
	mu    sync.Mutex
	start *protos.CreateInstanceRequest
}

func (*startRecordingServer) Hello(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (s *startRecordingServer) StartInstance(
	_ context.Context,
	req *protos.CreateInstanceRequest,
) (*protos.CreateInstanceResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.start = req
	return &protos.CreateInstanceResponse{InstanceId: req.InstanceId}, nil
}

func TestNewClientAppliesTrailingClientOptions(t *testing.T) {
	server := new(startRecordingServer)
	listener, stop := startBufconnServer(t, server)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	options := insecureBufconnOptions(t, listener)
	options.Versioning = &task.VersioningOptions{DefaultVersion: "1.0"}
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	client, err := NewClient(ctx, options, api.DefaultLogger(), durabletaskclient.WithTracerProvider(provider))
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()
	_, err = client.ScheduleNewOrchestration(ctx, "Orch", api.WithInstanceID("instance-1"))
	require.NoError(t, err)

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	// The default version comes from Options, so both option sources apply.
	require.Equal(t, "create_orchestration:Orch@(1.0)", spans[0].Name())
	server.mu.Lock()
	defer server.mu.Unlock()
	require.Equal(t, "1.0", server.start.GetVersion().GetValue())
	propagated := helpers.SpanContextFromTraceContext(server.start.ParentTraceContext)
	require.Equal(t, spans[0].SpanContext().SpanID(), propagated.SpanID())
}
