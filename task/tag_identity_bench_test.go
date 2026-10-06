package task

import (
	"context"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func activityIdentityWireMessages(t testing.TB, includeIdentity bool) []proto.Message {
	t.Helper()
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorNVersion("parent", "v1", func(ctx *OrchestrationContext) (any, error) {
		options := []CallActivityOption{WithRawActivityInput(`"input"`)}
		if includeIdentity {
			options = append(options, WithActivityOrchestrationIdentity())
		}
		ctx.CallActivity("inspect", options...)
		return nil, nil
	}))
	started := helpers.NewExecutionStartedEvent("parent", "instance", nil,
		helpers.NewParentInfo(7, "root", "root-instance"), nil, nil, wrapperspb.String("v1"))
	result, err := NewTaskExecutor(registry).ExecuteOrchestrator(context.Background(), "instance", nil,
		[]*protos.HistoryEvent{started}, nil)
	require.NoError(t, err)
	action := scheduledActivityAction(t, result.Response).GetScheduleTask()
	if includeIdentity {
		require.Equal(t, legacyIdentityTags(), action.Tags, "opt-in must reproduce the legacy identity payload")
	} else {
		require.Nil(t, action.Tags)
	}
	trace := &protos.TraceContext{TraceParent: "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"}
	action.ParentTraceContext = trace
	event := helpers.NewTaskScheduledEvent(0, action.Name, action.Version, action.Input, trace)
	event.Timestamp = timestamppb.New(time.Unix(1_700_000_000, 0).UTC())
	event.GetTaskScheduled().Tags = action.Tags
	request := &protos.ActivityRequest{
		Name: action.Name, Version: action.Version, Input: action.Input,
		OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: "instance"},
		ParentTraceContext:    trace, Tags: action.Tags,
	}
	return []proto.Message{action, event, request}
}

func TestActivityIdentitySerializedSize(t *testing.T) {
	defaultMessages := activityIdentityWireMessages(t, false)
	legacyMessages := activityIdentityWireMessages(t, true)
	for index, name := range []string{"action", "history", "request"} {
		defaultBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(defaultMessages[index])
		require.NoError(t, err)
		legacyBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(legacyMessages[index])
		require.NoError(t, err)
		require.Less(t, len(defaultBytes), len(legacyBytes))
		t.Logf("%s: default=%d legacy/opt-in=%d delta=%d protobuf bytes",
			name, len(defaultBytes), len(legacyBytes), len(legacyBytes)-len(defaultBytes))
	}
}

func BenchmarkActivityIdentityWireSize(b *testing.B) {
	for _, includeIdentity := range []bool{false, true} {
		messages := activityIdentityWireMessages(b, includeIdentity)
		for index, name := range []string{"action", "history", "request"} {
			b.Run(name+"/identity="+boolName(includeIdentity), func(b *testing.B) {
				message := messages[index]
				b.ReportAllocs()
				for b.Loop() {
					_, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
					if err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(proto.Size(message)), "wire-bytes")
			})
		}
	}
}
