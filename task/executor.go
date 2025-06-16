package task

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
)

type taskExecutor struct {
	Registry *TaskRegistry
}

// NewTaskExecutor returns a [backend.Executor] implementation that executes orchestrator and activity functions in-memory.
func NewTaskExecutor(registry *TaskRegistry) backend.Executor {
	return &taskExecutor{
		Registry: registry,
	}
}

// ExecuteActivity implements backend.Executor and executes an activity function in the current goroutine.
func (te *taskExecutor) ExecuteActivity(ctx context.Context, id api.InstanceID, e *protos.HistoryEvent) (response *protos.HistoryEvent, err error) {
	ts := e.GetTaskScheduled()
	if ts == nil {
		// No clean way to deal with this other than to abandon it
		return nil, fmt.Errorf("Unexpected event type for ExecuteActivity: %v", e.EventType)
	}
	invoker, ok := te.Registry.activities[ts.Name]
	if !ok {
		// try the wildcard match
		invoker, ok = te.Registry.activities["*"]
		if !ok {
			return &protos.HistoryEvent{
				EventId:   -1,
				Timestamp: timestamppb.Now(),
				EventType: &protos.HistoryEvent_TaskFailed{
					TaskFailed: &protos.TaskFailedEvent{
						TaskScheduledId: e.EventId,
						TaskExecutionId: ts.GetTaskExecutionId(),
						FailureDetails: &protos.TaskFailureDetails{
							ErrorType:    "TaskActivityNotRegistered",
							ErrorMessage: fmt.Sprintf("no task activity named '%s' was registered", ts.Name),
						},
					},
				},
			}, nil
		}
	}
	activityCtx := newTaskActivityContext(ctx, e.EventId, ts)

	// convert panics into activity failures
	defer func() {
		panicVal := recover()
		if panicVal != nil {
			response = &protos.HistoryEvent{
				EventId:   -1,
				Timestamp: timestamppb.Now(),
				EventType: &protos.HistoryEvent_TaskFailed{
					TaskFailed: &protos.TaskFailedEvent{
						TaskScheduledId: e.EventId,
						FailureDetails: &protos.TaskFailureDetails{
							ErrorType:    "TaskActivityPanic",
							ErrorMessage: fmt.Sprintf("panic: %v", panicVal),
						},
					},
				},
			}
		}
	}()

	result, err := invoker(activityCtx)
	if err != nil {
		return &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: timestamppb.Now(),
			EventType: &protos.HistoryEvent_TaskFailed{
				TaskFailed: &protos.TaskFailedEvent{
					TaskScheduledId: e.EventId,
					TaskExecutionId: ts.GetTaskExecutionId(),
					FailureDetails: &protos.TaskFailureDetails{
						ErrorType:    fmt.Sprintf("%T", err),
						ErrorMessage: fmt.Sprintf("%+v", err),
					},
				},
			},
		}, nil
	}

	bytes, err := marshalData(result)
	if err != nil {
		return &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: timestamppb.Now(),
			EventType: &protos.HistoryEvent_TaskFailed{
				TaskFailed: &protos.TaskFailedEvent{
					TaskScheduledId: e.EventId,
					TaskExecutionId: ts.GetTaskExecutionId(),
					FailureDetails: &protos.TaskFailureDetails{
						ErrorType:    fmt.Sprintf("%T", err),
						ErrorMessage: fmt.Sprintf("%+v", err),
					},
				},
			},
		}, nil
	}
	var rawResult *wrapperspb.StringValue
	if len(bytes) > 0 {
		rawResult = wrapperspb.String(string(bytes))
	}
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_TaskCompleted{
			TaskCompleted: &protos.TaskCompletedEvent{
				TaskScheduledId: e.EventId,
				TaskExecutionId: ts.GetTaskExecutionId(),
				Result:          rawResult,
			},
		},
	}, nil
}

// ExecuteOrchestrator implements backend.Executor and executes an orchestrator function in the current goroutine.
func (te *taskExecutor) ExecuteOrchestrator(ctx context.Context, id api.InstanceID, oldEvents []*protos.HistoryEvent, newEvents []*protos.HistoryEvent) (*backend.ExecutionResults, error) {
	orchestrationCtx := NewOrchestrationContext(te.Registry, id, oldEvents, newEvents)
	actions := orchestrationCtx.start()

	results := &backend.ExecutionResults{
		Response: &protos.OrchestratorResponse{
			InstanceId:   string(id),
			Actions:      actions,
			CustomStatus: wrapperspb.String(orchestrationCtx.customStatus),
		},
	}
	return results, nil
}

func (te taskExecutor) Shutdown(ctx context.Context) error {
	// Nothing to do
	return nil
}

func unmarshalData(data []byte, v any) error {
	if v == nil {
		return nil
	} else if len(data) == 0 {
		v = nil
		return nil
	} else {
		return json.Unmarshal(data, v)
	}
}

func marshalData(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return json.Marshal(v)
}
