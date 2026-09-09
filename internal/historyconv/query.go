package historyconv

import (
	"errors"
	"fmt"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
)

// NormalizeStreamRequest validates the arguments shared by every
// StreamOrchestrationHistory implementation and applies query defaults.
func NormalizeStreamRequest(
	id api.InstanceID,
	query api.HistoryQuery,
	handler api.HistoryEventHandler,
) (api.HistoryQuery, error) {
	if id == api.EmptyInstanceID {
		return api.HistoryQuery{}, api.WrapInvalidArgument(errors.New("instance ID cannot be empty"))
	}
	if err := helpers.ValidateOrchestrationInstanceID(string(id)); err != nil {
		return api.HistoryQuery{}, api.WrapInvalidArgument(err)
	}
	if handler == nil {
		return api.HistoryQuery{}, api.WrapInvalidArgument(errors.New("history event handler is required"))
	}
	return api.NormalizeHistoryQuery(query)
}

// Collect buffers a bounded history snapshot from a streaming history read.
func Collect(
	id api.InstanceID,
	query api.HistoryQuery,
	stream func(api.HistoryEventHandler) error,
) (*api.OrchestrationHistory, error) {
	normalized, err := api.NormalizeHistoryQuery(query)
	if err != nil {
		return nil, err
	}
	result := &api.OrchestrationHistory{InstanceID: id}
	totalBytes := 0
	err = stream(func(event *api.HistoryEvent) error {
		if event == nil {
			return errors.New("history event must not be nil")
		}
		if len(result.Events) >= normalized.MaxEvents {
			return fmt.Errorf("%w: limit %d", api.ErrHistoryLimitExceeded, normalized.MaxEvents)
		}
		if event.ExecutionStarted != nil || event.Type == api.HistoryEventExecutionStarted {
			started := event.ExecutionStarted
			if started == nil || started.ExecutionID == "" {
				return errors.New("history ExecutionStarted event is missing an execution ID")
			}
			if normalized.ExecutionID != "" && started.ExecutionID != normalized.ExecutionID {
				return fmt.Errorf("history execution ID %q does not match requested execution %q",
					started.ExecutionID, normalized.ExecutionID)
			}
			if result.ExecutionID != "" && started.ExecutionID != result.ExecutionID {
				return fmt.Errorf("history contains conflicting execution IDs %q and %q",
					result.ExecutionID, started.ExecutionID)
			}
			result.ExecutionID = started.ExecutionID
		}
		totalBytes = addSize(totalBytes, ApproximateEventSize(event))
		if totalBytes > normalized.MaxBytes {
			return fmt.Errorf("%w: byte limit %d", api.ErrHistoryLimitExceeded, normalized.MaxBytes)
		}
		result.Events = append(result.Events, event)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if normalized.ExecutionID != "" && result.ExecutionID == "" {
		return nil, fmt.Errorf("history is missing an ExecutionStarted event for requested execution %q",
			normalized.ExecutionID)
	}
	return result, nil
}
