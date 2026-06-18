package client

import (
	"context"
	"net"
	"testing"

	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// captureSidecar is a minimal TaskHubSidecarService implementation that records
// the CreateInstanceRequest it receives so a test can assert on the propagated
// parent trace context.
type captureSidecar struct {
	protos.UnimplementedTaskHubSidecarServiceServer
	lastStart *protos.CreateInstanceRequest
}

func (s *captureSidecar) StartInstance(_ context.Context, req *protos.CreateInstanceRequest) (*protos.CreateInstanceResponse, error) {
	s.lastStart = req
	return &protos.CreateInstanceResponse{InstanceId: req.InstanceId}, nil
}

// Test_ScheduleNewOrchestration_PropagatesParentTraceContext verifies that the
// gRPC client attaches the caller's trace context to CreateInstanceRequest.
// ParentTraceContext, which the server (the Azure Functions Durable Task
// extension, or a durabletask-go task hub) uses to parent the orchestration
// span — linking the caller's trace to the orchestration it starts.
func Test_ScheduleNewOrchestration_PropagatesParentTraceContext(t *testing.T) {
	// A sampled provider makes the client emit a sampled span whose context is
	// propagated; an unsampled span would intentionally carry no trace context.
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	srv := &captureSidecar{}
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	protos.RegisterTaskHubSidecarServiceServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.Dial(
		"bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	c := NewTaskHubGrpcClient(conn, backend.DefaultLogger())

	// Start an outer "caller" span; the propagated trace context must carry its
	// trace ID, proving the schedule operation joins the caller's trace.
	ctx, caller := otel.Tracer("test").Start(context.Background(), "caller")
	defer caller.End()
	wantTraceID := caller.SpanContext().TraceID().String()

	if _, err := c.ScheduleNewOrchestration(ctx, "MyOrchestrator"); err != nil {
		t.Fatalf("schedule: %v", err)
	}

	require.NotNil(t, srv.lastStart, "server should have received a StartInstance request")
	tc := srv.lastStart.GetParentTraceContext()
	require.NotNil(t, tc, "client should propagate a parent trace context")
	assert.Contains(t, tc.GetTraceParent(), wantTraceID,
		"propagated traceparent %q should carry the caller's trace ID %q", tc.GetTraceParent(), wantTraceID)
}
