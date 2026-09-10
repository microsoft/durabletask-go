package task

import (
	"encoding/hex"
	"fmt"
	"slices"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func isRewindRequest(oldEvents, newEvents []*protos.HistoryEvent) bool {
	return slices.ContainsFunc(newEvents, func(event *protos.HistoryEvent) bool {
		return event.GetExecutionRewound() != nil
	}) && slices.ContainsFunc(oldEvents, func(event *protos.HistoryEvent) bool {
		return event.GetExecutionCompleted() != nil
	})
}

// A terminal rewind first returns replacement history; DTS persists it and
// rewinds failed children. The subsequent leaf wake-up has no old completion
// marker, so normal replay reissues only the removed activity schedules.
// Keep this transformation aligned with durabletask-python's
// _OrchestrationExecutor._build_rewind_result and the DTS backend contract.
func buildRewindResult(id api.InstanceID, oldEvents, newEvents []*protos.HistoryEvent) (*ExecutionResults, error) {
	if len(newEvents) != 2 || newEvents[0].GetOrchestratorStarted() == nil || newEvents[1].GetExecutionRewound() == nil {
		return nil, fmt.Errorf("rewind requires exactly two new events: orchestrator started and execution rewound")
	}
	rewound := newEvents[1].GetExecutionRewound()
	executionID, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("failed to generate rewind execution ID: %w", err)
	}

	failedTasks := make(map[int32]struct{})
	for _, events := range [][]*protos.HistoryEvent{oldEvents, newEvents} {
		for _, event := range events {
			if failed := event.GetTaskFailed(); failed != nil {
				failedTasks[failed.TaskScheduledId] = struct{}{}
			}
		}
	}

	history := make([]*protos.HistoryEvent, 0, len(oldEvents)+len(newEvents))
	for _, events := range [][]*protos.HistoryEvent{oldEvents, newEvents} {
		for _, event := range events {
			if event.GetTaskFailed() != nil ||
				event.GetSubOrchestrationInstanceFailed() != nil ||
				event.GetExecutionCompleted() != nil {
				continue
			}
			if event.GetTaskScheduled() != nil {
				if _, failed := failedTasks[event.EventId]; failed {
					continue
				}
			}
			if event.GetExecutionStarted() != nil {
				event = proto.CloneOf(event)
				started := event.GetExecutionStarted()
				if started.OrchestrationInstance == nil {
					started.OrchestrationInstance = &protos.OrchestrationInstance{}
				}
				started.OrchestrationInstance.ExecutionId = wrapperspb.String(hex.EncodeToString(executionID[:]))
				if rewound.GetParentExecutionId().GetValue() != "" && started.ParentInstance != nil {
					if started.ParentInstance.OrchestrationInstance == nil {
						started.ParentInstance.OrchestrationInstance = &protos.OrchestrationInstance{}
					}
					started.ParentInstance.OrchestrationInstance.ExecutionId = proto.CloneOf(rewound.ParentExecutionId)
				}
			}
			history = append(history, event)
		}
	}
	return &ExecutionResults{Response: &protos.OrchestratorResponse{
		InstanceId: string(id),
		Actions: []*protos.OrchestratorAction{{
			Id: -1,
			OrchestratorActionType: &protos.OrchestratorAction_RewindOrchestration{
				RewindOrchestration: &protos.RewindOrchestrationAction{NewHistory: history},
			},
		}},
	}}, nil
}
