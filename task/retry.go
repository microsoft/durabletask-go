package task

import (
	"fmt"
	"time"
)

type retryTaskInfo struct {
	kind    WorkItemKind
	name    string
	version string
}

func (ctx *OrchestrationContext) reportRetry(
	info retryTaskInfo,
	failedAttempt int,
	policy RetryPolicy,
	delay time.Duration,
	err error,
) {
	engine := ctx.engineContext()
	if engine.IsReplaying || engine.metrics.Retry == nil {
		return
	}
	metric := RetryMetric{
		InstanceID:           engine.ID,
		OrchestrationName:    engine.Name,
		OrchestrationVersion: engine.Version,
		TaskKind:             info.kind,
		TaskName:             info.name,
		TaskVersion:          info.version,
		FailedAttempt:        failedAttempt,
		NextAttempt:          failedAttempt + 1,
		MaxAttempts:          policy.MaxAttempts,
		Delay:                delay,
		ErrorType:            fmt.Sprintf("%T", err),
		ErrorMessage:         err.Error(),
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			engine.Logger().Error("retry metrics callback panicked", "error", recovered)
		}
	}()
	engine.metrics.Retry(metric)
}
