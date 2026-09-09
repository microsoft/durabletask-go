package client

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type historySchedulerClient struct {
	protos.TaskHubSidecarServiceClient
	request *protos.StreamInstanceHistoryRequest
	stream  protos.TaskHubSidecarService_StreamInstanceHistoryClient
	err     error
}

func (c *historySchedulerClient) StreamInstanceHistory(
	_ context.Context,
	request *protos.StreamInstanceHistoryRequest,
	_ ...grpc.CallOption,
) (protos.TaskHubSidecarService_StreamInstanceHistoryClient, error) {
	c.request = request
	return c.stream, c.err
}

type historyClientStream struct {
	protos.TaskHubSidecarService_StreamInstanceHistoryClient
	chunks []*protos.HistoryChunk
	err    error
	index  int
}

func (s *historyClientStream) Recv() (*protos.HistoryChunk, error) {
	if s.index < len(s.chunks) {
		chunk := s.chunks[s.index]
		s.index++
		return chunk, nil
	}
	if s.err != nil {
		return nil, s.err
	}
	return nil, io.EOF
}

func TestTaskHubGrpcClientStreamsHistoryInOrder(t *testing.T) {
	scheduler := &historySchedulerClient{stream: &historyClientStream{
		chunks: []*protos.HistoryChunk{
			{Events: []*protos.HistoryEvent{historyGenericEvent(1, `"one"`)}},
			{},
			{Events: []*protos.HistoryEvent{historyGenericEvent(2, `"two"`)}},
		},
	}}
	client := &TaskHubGrpcClient{
		client:    scheduler,
		logger:    api.DefaultLogger(),
		converter: api.DefaultDataConverter(),
	}
	var values []string
	err := client.StreamOrchestrationHistory(
		context.Background(),
		"instance",
		api.HistoryQuery{ExecutionID: "execution", MaxEvents: 1},
		func(event *api.HistoryEvent) error {
			var value string
			require.NoError(t, event.ReadData(&value))
			values = append(values, value)
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"one", "two"}, values)
	require.Equal(t, "instance", scheduler.request.InstanceId)
	require.Equal(t, "execution", scheduler.request.ExecutionId.GetValue())
	require.False(t, scheduler.request.ForWorkItemProcessing)
}

func TestTaskHubGrpcClientHistoryLimitAndErrors(t *testing.T) {
	tests := []struct {
		name      string
		scheduler *historySchedulerClient
		query     api.HistoryQuery
		expected  error
	}{
		{
			name: "limit",
			scheduler: &historySchedulerClient{stream: &historyClientStream{chunks: []*protos.HistoryChunk{
				{Events: []*protos.HistoryEvent{historyGenericEvent(1, "one"), historyGenericEvent(2, "two")}},
			}}},
			query:    api.HistoryQuery{MaxEvents: 1},
			expected: api.ErrHistoryLimitExceeded,
		},
		{
			name: "byte limit",
			scheduler: &historySchedulerClient{stream: &historyClientStream{chunks: []*protos.HistoryChunk{
				{Events: []*protos.HistoryEvent{historyGenericEvent(1, "payload")}},
			}}},
			query:    api.HistoryQuery{MaxBytes: 1},
			expected: api.ErrHistoryLimitExceeded,
		},
		{
			name:      "not found",
			scheduler: &historySchedulerClient{err: status.Error(codes.NotFound, "missing")},
			expected:  api.ErrInstanceNotFound,
		},
		{
			name:      "unimplemented",
			scheduler: &historySchedulerClient{err: status.Error(codes.Unimplemented, "unsupported")},
			expected:  api.ErrFeatureNotSupported,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &TaskHubGrpcClient{
				client:    test.scheduler,
				logger:    api.DefaultLogger(),
				converter: api.DefaultDataConverter(),
			}
			_, err := client.GetOrchestrationHistory(context.Background(), "instance", test.query)
			require.ErrorIs(t, err, test.expected)
		})
	}
}

func TestTaskHubGrpcClientHistoryValidationAndCallbackError(t *testing.T) {
	client := &TaskHubGrpcClient{
		client:    &historySchedulerClient{},
		logger:    api.DefaultLogger(),
		converter: api.DefaultDataConverter(),
	}
	err := client.StreamOrchestrationHistory(context.Background(), "", api.HistoryQuery{}, func(*api.HistoryEvent) error {
		return nil
	})
	require.ErrorIs(t, err, api.ErrInvalidArgument)

	err = client.StreamOrchestrationHistory(context.Background(), "instance", api.HistoryQuery{}, nil)
	require.ErrorIs(t, err, api.ErrInvalidArgument)

	callbackErr := errors.New("stop")
	scheduler := &historySchedulerClient{stream: &historyClientStream{
		chunks: []*protos.HistoryChunk{{Events: []*protos.HistoryEvent{historyGenericEvent(1, "one")}}},
	}}
	client.client = scheduler
	err = client.StreamOrchestrationHistory(
		context.Background(),
		"instance",
		api.HistoryQuery{},
		func(*api.HistoryEvent) error { return callbackErr },
	)
	require.ErrorIs(t, err, callbackErr)
}

func TestTaskHubGrpcClientHistoryMapsCanceledReceive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	scheduler := &historySchedulerClient{stream: &historyClientStream{
		err: status.Error(codes.Canceled, "canceled"),
	}}
	client := &TaskHubGrpcClient{
		client:    scheduler,
		logger:    api.DefaultLogger(),
		converter: api.DefaultDataConverter(),
	}
	err := client.StreamOrchestrationHistory(ctx, "instance", api.HistoryQuery{}, func(*api.HistoryEvent) error {
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestTaskHubGrpcClientHistoryExecutionIdentity(t *testing.T) {
	for _, test := range []struct {
		name      string
		requested string
		events    []*protos.HistoryEvent
		observed  string
		wantError bool
	}{
		{"pinned match", "A", []*protos.HistoryEvent{historyStartedEvent("A")}, "A", false},
		{"pinned mismatch", "A", []*protos.HistoryEvent{historyStartedEvent("B")}, "", true},
		{"pinned mixed chunks", "A", []*protos.HistoryEvent{historyStartedEvent("A"), historyStartedEvent("B")}, "", true},
		{"unpinned mixed chunks", "", []*protos.HistoryEvent{historyStartedEvent("A"), historyStartedEvent("B")}, "", true},
		{"pinned repeated match", "A", []*protos.HistoryEvent{historyStartedEvent("A"), historyStartedEvent("A")}, "A", false},
		{"pinned empty", "A", nil, "", true},
		{"pinned generic", "A", []*protos.HistoryEvent{historyGenericEvent(1, "value")}, "", true},
		{"pinned missing ID", "A", []*protos.HistoryEvent{historyStartedEvent("")}, "", true},
		{"pinned missing instance", "A", []*protos.HistoryEvent{{EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{},
		}}}, "", true},
		{"unpinned missing ID", "", []*protos.HistoryEvent{historyStartedEvent("")}, "", true},
		{"unpinned empty", "", nil, "", false},
		{"unpinned generic", "", []*protos.HistoryEvent{historyGenericEvent(1, "value")}, "", false},
		{"unpinned observed", "", []*protos.HistoryEvent{historyStartedEvent("A")}, "A", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			stream := &historyClientStream{}
			for _, event := range test.events {
				stream.chunks = append(stream.chunks, &protos.HistoryChunk{Events: []*protos.HistoryEvent{event}})
			}
			scheduler := &historySchedulerClient{stream: stream}
			client := &TaskHubGrpcClient{client: scheduler, logger: api.DefaultLogger(), converter: api.DefaultDataConverter()}
			result, err := client.GetOrchestrationHistory(
				context.Background(), "instance", api.HistoryQuery{ExecutionID: test.requested})
			if test.wantError {
				require.ErrorContains(t, err, "execution")
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.observed, result.ExecutionID)
				require.Len(t, result.Events, len(test.events))
			}
			require.Equal(t, test.requested, scheduler.request.GetExecutionId().GetValue())
		})
	}
}

func TestTaskHubGrpcClientHistoryRetainedContentByteLimit(t *testing.T) {
	large := strings.Repeat("x", 4096)
	details := &protos.TaskFailureDetails{InnerFailure: &protos.TaskFailureDetails{
		Properties: map[string]*structpb.Value{"nested": structpb.NewListValue(&structpb.ListValue{
			Values: []*structpb.Value{structpb.NewStructValue(&structpb.Struct{
				Fields: map[string]*structpb.Value{"value": structpb.NewStringValue(large)},
			})},
		})},
	}}
	for name, event := range map[string]*protos.HistoryEvent{
		"execution failure": {EventType: &protos.HistoryEvent_ExecutionCompleted{
			ExecutionCompleted: &protos.ExecutionCompletedEvent{FailureDetails: details},
		}},
		"task failure": {EventType: &protos.HistoryEvent_TaskFailed{
			TaskFailed: &protos.TaskFailedEvent{FailureDetails: details},
		}},
		"sub-orchestration failure": {EventType: &protos.HistoryEvent_SubOrchestrationInstanceFailed{
			SubOrchestrationInstanceFailed: &protos.SubOrchestrationInstanceFailedEvent{FailureDetails: details},
		}},
		"entity failure": {EventType: &protos.HistoryEvent_EntityOperationFailed{
			EntityOperationFailed: &protos.EntityOperationFailedEvent{FailureDetails: details},
		}},
		"history state failure": {EventType: &protos.HistoryEvent_HistoryState{
			HistoryState: &protos.HistoryStateEvent{OrchestrationState: &protos.OrchestrationState{FailureDetails: details}},
		}},
		"history state input": {EventType: &protos.HistoryEvent_HistoryState{
			HistoryState: &protos.HistoryStateEvent{OrchestrationState: &protos.OrchestrationState{Input: wrapperspb.String(large)}},
		}},
		"history state output": {EventType: &protos.HistoryEvent_HistoryState{
			HistoryState: &protos.HistoryStateEvent{OrchestrationState: &protos.OrchestrationState{Output: wrapperspb.String(large)}},
		}},
		"history state status": {EventType: &protos.HistoryEvent_HistoryState{
			HistoryState: &protos.HistoryStateEvent{OrchestrationState: &protos.OrchestrationState{CustomStatus: wrapperspb.String(large)}},
		}},
	} {
		t.Run(name, func(t *testing.T) {
			scheduler := &historySchedulerClient{stream: &historyClientStream{
				chunks: []*protos.HistoryChunk{{Events: []*protos.HistoryEvent{event}}},
			}}
			client := &TaskHubGrpcClient{client: scheduler, logger: api.DefaultLogger(), converter: api.DefaultDataConverter()}
			result, err := client.GetOrchestrationHistory(context.Background(), "instance", api.HistoryQuery{MaxBytes: 1024})
			require.ErrorIs(t, err, api.ErrHistoryLimitExceeded)
			require.Nil(t, result)
		})
	}
}

func historyStartedEvent(executionID string) *protos.HistoryEvent {
	instance := &protos.OrchestrationInstance{InstanceId: "instance"}
	if executionID != "" {
		instance.ExecutionId = wrapperspb.String(executionID)
	}
	return &protos.HistoryEvent{EventType: &protos.HistoryEvent_ExecutionStarted{
		ExecutionStarted: &protos.ExecutionStartedEvent{OrchestrationInstance: instance},
	}}
}

func historyGenericEvent(id int32, value string) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId: id,
		EventType: &protos.HistoryEvent_GenericEvent{
			GenericEvent: &protos.GenericEvent{Data: wrapperspb.String(value)},
		},
	}
}
