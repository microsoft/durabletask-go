package task

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"
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
		return nil, fmt.Errorf("unexpected event type for ExecuteActivity: %v", e.EventType)
	}
	invoker, ok := te.Registry.activities[ts.Name]
	if !ok {
		// try the wildcard match
		invoker, ok = te.Registry.activities["*"]
		if !ok {
			return helpers.NewTaskFailedEvent(e.EventId, &protos.TaskFailureDetails{
				ErrorType:    "TaskActivityNotRegistered",
				ErrorMessage: fmt.Sprintf("no task activity named '%s' was registered", ts.Name),
			}), nil
		}
	}
	activityCtx := newTaskActivityContext(ctx, e.EventId, ts)

	// convert panics into activity failures
	defer func() {
		panicVal := recover()
		if panicVal != nil {
			response = helpers.NewTaskFailedEvent(e.EventId, &protos.TaskFailureDetails{
				ErrorType:    "TaskActivityPanic",
				ErrorMessage: fmt.Sprintf("panic: %v", panicVal),
			})
		}
	}()

	result, err := invoker(activityCtx)
	if err != nil {
		return helpers.NewTaskFailedEvent(e.EventId, &protos.TaskFailureDetails{
			ErrorType:    fmt.Sprintf("%T", err),
			ErrorMessage: fmt.Sprintf("%+v", err),
		}), nil
	}

	bytes, err := marshalData(result)
	if err != nil {
		return helpers.NewTaskFailedEvent(e.EventId, &protos.TaskFailureDetails{
			ErrorType:    fmt.Sprintf("%T", err),
			ErrorMessage: fmt.Sprintf("%+v", err),
		}), nil
	}
	var rawResult *wrapperspb.StringValue
	if len(bytes) > 0 {
		rawResult = wrapperspb.String(string(bytes))
	}
	return helpers.NewTaskCompletedEvent(e.EventId, rawResult), nil
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

// ExecuteEntity implements backend.Executor and executes an entity batch in the current goroutine.
func (te *taskExecutor) ExecuteEntity(ctx context.Context, id api.InstanceID, req *protos.EntityBatchRequest) (result *protos.EntityBatchResult, err error) {
	entityID, parseErr := api.EntityIDFromString(req.InstanceId)
	if parseErr != nil {
		return nil, fmt.Errorf("invalid entity instance ID: %w", parseErr)
	}

	invoker, ok := te.Registry.entities[entityID.Name]
	if !ok {
		// try the wildcard match
		invoker, ok = te.Registry.entities["*"]
		if !ok {
			return &protos.EntityBatchResult{
				FailureDetails: &protos.TaskFailureDetails{
					ErrorType:    "EntityNotRegistered",
					ErrorMessage: fmt.Sprintf("no entity named '%s' was registered", entityID.Name),
				},
			}, nil
		}
	}

	// Initialize entity state from the batch request
	var state entityState
	if req.EntityState != nil {
		state = entityState{
			value:    []byte(req.EntityState.GetValue()),
			hasValue: true,
		}
	}

	results := make([]*protos.OperationResult, 0, len(req.Operations))
	var allActions []*protos.OperationAction

	for _, op := range req.Operations {
		entityCtx := &EntityContext{
			ID:        entityID,
			Operation: op.Operation,
			rawInput:  []byte(op.Input.GetValue()),
			state:     state,
		}

		// Execute the entity function, converting panics to failures
		opResult := func() (opResult *protos.OperationResult) {
			defer func() {
				panicVal := recover()
				if panicVal != nil {
					opResult = &protos.OperationResult{
						ResultType: &protos.OperationResult_Failure{
							Failure: &protos.OperationResultFailure{
								FailureDetails: &protos.TaskFailureDetails{
									ErrorType:    "EntityOperationPanic",
									ErrorMessage: fmt.Sprintf("panic: %v", panicVal),
								},
							},
						},
					}
				}
			}()

			output, opErr := invoker(entityCtx)
			if opErr != nil {
				// Operation failed - rollback state
				return &protos.OperationResult{
					ResultType: &protos.OperationResult_Failure{
						Failure: &protos.OperationResultFailure{
							FailureDetails: &protos.TaskFailureDetails{
								ErrorType:    fmt.Sprintf("%T", opErr),
								ErrorMessage: fmt.Sprintf("%+v", opErr),
							},
						},
					},
				}
			}

			// Operation succeeded - commit state
			state = entityCtx.state
			allActions = append(allActions, entityCtx.actions...)

			var rawResult *wrapperspb.StringValue
			if output != nil {
				bytes, marshalErr := marshalData(output)
				if marshalErr != nil {
					return &protos.OperationResult{
						ResultType: &protos.OperationResult_Failure{
							Failure: &protos.OperationResultFailure{
								FailureDetails: &protos.TaskFailureDetails{
									ErrorType:    fmt.Sprintf("%T", marshalErr),
									ErrorMessage: fmt.Sprintf("failed to marshal entity result: %+v", marshalErr),
								},
							},
						},
					}
				}
				if len(bytes) > 0 {
					rawResult = wrapperspb.String(string(bytes))
				}
			}

			return &protos.OperationResult{
				ResultType: &protos.OperationResult_Success{
					Success: &protos.OperationResultSuccess{
						Result: rawResult,
					},
				},
			}
		}()

		results = append(results, opResult)
	}

	batchResult := &protos.EntityBatchResult{
		Results: results,
		Actions: allActions,
	}

	if state.hasValue {
		batchResult.EntityState = wrapperspb.String(string(state.value))
	}

	return batchResult, nil
}

func unmarshalData(data []byte, v any) error {
	switch {
	case v == nil:
		return nil
	case len(data) == 0:
		return nil
	default:
		return json.Unmarshal(data, v)
	}
}

func marshalData(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	return json.Marshal(v)
}
