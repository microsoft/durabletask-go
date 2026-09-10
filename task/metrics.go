package task

import (
	"time"

	"github.com/microsoft/durabletask-go/api"
)

// WorkItemKind identifies an orchestration, activity, or entity work item.
type WorkItemKind string

const (
	WorkItemKindOrchestration WorkItemKind = "orchestration"
	WorkItemKindActivity      WorkItemKind = "activity"
	WorkItemKindEntity        WorkItemKind = "entity"
)

// RetryMetric reports a durable task retry that was scheduled.
type RetryMetric struct {
	InstanceID           api.InstanceID
	OrchestrationName    string
	OrchestrationVersion string
	TaskKind             WorkItemKind
	TaskName             string
	TaskVersion          string
	FailedAttempt        int
	NextAttempt          int
	MaxAttempts          int
	Delay                time.Duration
	ErrorType            string
	ErrorMessage         string
}

// HistoryMetric reports orchestration history usage for one execution turn.
type HistoryMetric struct {
	InstanceID           api.InstanceID
	OrchestrationName    string
	OrchestrationVersion string
	HistoryLength        int
	ProcessedEvents      int
	HistoryLimitExceeded bool
}

// MetricsHooks contains optional transport-neutral metric callbacks.
// Callbacks must return quickly and must not block worker progress.
type MetricsHooks struct {
	Retry   func(RetryMetric)
	History func(HistoryMetric)
}
