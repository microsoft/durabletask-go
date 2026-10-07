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

type activityWireSizeCase struct {
	name     string
	current  proto.Message
	previous proto.Message
}

func activityIdentityWireMessages(t testing.TB) []activityWireSizeCase {
	t.Helper()
	registry := NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorNVersion("parent", "v1", func(ctx *OrchestrationContext) (any, error) {
		ctx.CallActivity("inspect", WithRawActivityInput(`"input"`))
		return nil, nil
	}))
	started := helpers.NewExecutionStartedEvent("parent", "instance", nil,
		helpers.NewParentInfo(7, "root", "root-instance"), nil, nil, wrapperspb.String("v1"))
	result, err := NewTaskExecutor(registry).ExecuteOrchestrator(context.Background(), "instance", nil,
		[]*protos.HistoryEvent{started}, nil)
	require.NoError(t, err)
	action := scheduledActivityAction(t, result.Response).GetScheduleTask()
	require.Nil(t, action.Tags)
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
	previousTags := map[string]string{
		"__durabletask.context.encoding":              "1",
		"__durabletask.context.instance_id":           "instance",
		"__durabletask.context.orchestration_name":    "parent",
		"__durabletask.context.orchestration_version": "v1",
		"__durabletask.context.parent_instance_id":    "root-instance",
	}
	previousAction := proto.Clone(action).(*protos.ScheduleTaskAction)
	previousAction.Tags = previousTags
	previousEvent := proto.Clone(event).(*protos.HistoryEvent)
	previousEvent.GetTaskScheduled().Tags = previousTags
	previousRequest := proto.Clone(request).(*protos.ActivityRequest)
	previousRequest.Tags = previousTags
	return []activityWireSizeCase{
		{"action", action, previousAction},
		{"history", event, previousEvent},
		{"request", request, previousRequest},
	}
}

func TestActivityIdentitySerializedSize(t *testing.T) {
	for _, test := range activityIdentityWireMessages(t) {
		currentBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(test.current)
		require.NoError(t, err)
		previousBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(test.previous)
		require.NoError(t, err)
		require.Less(t, len(currentBytes), len(previousBytes))
		t.Logf("%s: current=%d previous=%d delta=%d protobuf bytes",
			test.name, len(currentBytes), len(previousBytes), len(previousBytes)-len(currentBytes))
	}
}

func BenchmarkActivityIdentityWireSize(b *testing.B) {
	for _, test := range activityIdentityWireMessages(b) {
		for _, variant := range []struct {
			name    string
			message proto.Message
		}{
			{"current", test.current},
			{"previous", test.previous},
		} {
			b.Run(test.name+"/"+variant.name, func(b *testing.B) {
				message := variant.message
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
