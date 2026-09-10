package task

import (
	"errors"
	"fmt"
	"time"

	"github.com/microsoft/durabletask-go/api"
)

// DefaultMaximumTimerInterval is the default maximum duration of one physical
// durable timer action.
const DefaultMaximumTimerInterval = 3 * 24 * time.Hour

// ErrHistoryLimitExceeded is the sentinel error for deterministic history-budget failures.
var ErrHistoryLimitExceeded = errors.New("orchestration history limit exceeded")

// HistoryLimitError describes the history budget that was exceeded.
type HistoryLimitError struct {
	InstanceID       api.InstanceID
	HistoryLength    int
	MaxHistoryEvents int
	PolicyError      error
}

func (e *HistoryLimitError) Error() string {
	message := fmt.Sprintf(
		"%v for instance %q: history=%d max_history=%d",
		ErrHistoryLimitExceeded,
		e.InstanceID,
		e.HistoryLength,
		e.MaxHistoryEvents,
	)
	if e.PolicyError != nil {
		return fmt.Sprintf("%s: history limit handler failed: %v", message, e.PolicyError)
	}
	return message
}

func (e *HistoryLimitError) Unwrap() error {
	return ErrHistoryLimitExceeded
}

func (*HistoryLimitError) DurableTaskErrorType() api.ErrorType {
	return api.ErrorTypeHistoryLimitExceeded
}

func (*HistoryLimitError) NonRetriable() bool {
	return true
}

// HistoryLimitInfo is the immutable input passed to a history limit handler.
type HistoryLimitInfo struct {
	InstanceID            api.InstanceID
	OrchestrationName     string
	OrchestrationVersion  string
	HistoryLength         int
	MaxHistoryEvents      int
	UnprocessedEventCount int
	SerializedInput       string
	Converter             api.DataConverter
}

// GetInput unmarshals the current orchestration input.
func (info HistoryLimitInfo) GetInput(v any) error {
	if v == nil || info.SerializedInput == "" {
		return nil
	}
	return api.NormalizeDataConverter(info.Converter).Deserialize(info.SerializedInput, v)
}

// HistoryLimitHandler supplies safe input for a ContinueAsNew transition.
// It must be deterministic. Unconsumed external events are preserved automatically.
type HistoryLimitHandler func(HistoryLimitInfo) (any, error)

// OrchestrationOptions configures deterministic orchestration engine policies.
type OrchestrationOptions struct {
	// MaximumTimerInterval limits one physical durable timer action. Longer
	// timers are split into deterministic sequential actions that retain the
	// original deadline. Zero uses [DefaultMaximumTimerInterval].
	//
	// Upgrading from a release that did not split long timers is replay-compatible
	// once the historical logical deadline has fired. Changing this value between
	// two splitting configurations remains replay-breaking for in-flight
	// orchestrations that have timers longer than either the old or new value.
	MaximumTimerInterval time.Duration

	// MaxEventsPerTurn limits new DTS work-item events processed in one
	// execution turn. Orchestration control markers are outside DTS's processed
	// event count. Remaining events are left for DTS to redeliver. Zero
	// disables the limit. Old replay history is always processed.
	MaxEventsPerTurn int

	// MaxHistoryEvents limits the total old and new history supplied to an execution.
	// Zero disables the limit.
	MaxHistoryEvents int

	// OnHistoryLimitExceeded handles MaxHistoryEvents when the orchestration
	// remains incomplete after the turn. It opts into ContinueAsNew by supplying
	// serializable state; when nil, the orchestration fails with a non-retriable
	// HistoryLimitExceeded failure.
	OnHistoryLimitExceeded HistoryLimitHandler
}

func normalizeOrchestrationOptions(options OrchestrationOptions) OrchestrationOptions {
	if options.MaximumTimerInterval < 0 {
		panic("maximum timer interval cannot be negative")
	}
	if options.MaximumTimerInterval == 0 {
		options.MaximumTimerInterval = DefaultMaximumTimerInterval
	}
	if options.MaxEventsPerTurn < 0 {
		panic("maximum events per turn cannot be negative")
	}
	if options.MaxHistoryEvents < 0 {
		panic("maximum history events cannot be negative")
	}
	return options
}
