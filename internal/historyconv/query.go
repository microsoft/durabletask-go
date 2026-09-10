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
	result := &api.OrchestrationHistory{InstanceID: id}
	executionID, _, err := StreamValidated(query, stream, func(event *api.HistoryEvent) error {
		result.Events = append(result.Events, event)
		return nil
	})
	if err != nil {
		return nil, err
	}
	result.ExecutionID = executionID
	return result, nil
}

// StreamValidated applies the same aggregate limits and observed execution
// identity checks as Collect without retaining events. stream must invoke its
// handler serially and stop on a handler error. Success includes validation of
// the end of the stream; callers must not publish output before this returns.
func StreamValidated(
	query api.HistoryQuery,
	stream func(api.HistoryEventHandler) error,
	handler api.HistoryEventHandler,
) (executionID string, eventCount int, err error) {
	normalized, err := api.NormalizeHistoryQuery(query)
	if err != nil {
		return "", 0, err
	}
	totalBytes := 0
	validate := func(event *api.HistoryEvent) error {
		if event == nil {
			return errors.New("history event must not be nil")
		}
		if eventCount >= normalized.MaxEvents {
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
			if executionID != "" && started.ExecutionID != executionID {
				return fmt.Errorf("history contains conflicting execution IDs %q and %q",
					executionID, started.ExecutionID)
			}
			executionID = started.ExecutionID
		}
		totalBytes = addSize(totalBytes, ApproximateEventSize(event))
		if totalBytes > normalized.MaxBytes {
			return fmt.Errorf("%w: byte limit %d", api.ErrHistoryLimitExceeded, normalized.MaxBytes)
		}
		eventCount++
		return handler(event)
	}
	var handlerErr error
	err = stream(func(event *api.HistoryEvent) error {
		if handlerErr == nil {
			handlerErr = validate(event)
		}
		return handlerErr
	})
	if handlerErr != nil {
		return "", eventCount, handlerErr
	}
	if err != nil {
		return "", eventCount, err
	}
	if normalized.ExecutionID != "" && executionID == "" {
		return "", eventCount, fmt.Errorf("history is missing an ExecutionStarted event for requested execution %q",
			normalized.ExecutionID)
	}
	return executionID, eventCount, nil
}
