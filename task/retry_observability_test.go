package task

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
)

func TestRetryObservabilityReportsNewRetryMetric(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("retry", func(ctx *OrchestrationContext) (any, error) {
		return nil, ctx.CallActivity("flaky", WithActivityRetryPolicy(&RetryPolicy{
			MaxAttempts:          2,
			InitialRetryInterval: time.Second,
		})).Await(nil)
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("retry-observability")
	metrics := make([]RetryMetric, 0, 1)
	executor := NewTaskExecutor(registry, WithMetricsHooks(MetricsHooks{
		Retry: func(metric RetryMetric) {
			metrics = append(metrics, metric)
		},
	}))

	oldEvents := []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewExecutionStartedEvent("retry", string(instanceID), nil, nil, nil, nil),
		helpers.NewTaskScheduledEvent(0, "flaky", nil, nil, nil),
	}
	newEvents := []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewTaskFailedEvent(0, &protos.TaskFailureDetails{
			ErrorType:    "flaky",
			ErrorMessage: "try again",
		}),
	}
	result, err := executor.ExecuteOrchestrator(context.Background(), instanceID, oldEvents, newEvents, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) != 1 {
		t.Fatalf("retry metrics = %d, want 1", len(metrics))
	}
	metric := metrics[0]
	if metric.TaskName != "flaky" || metric.FailedAttempt != 1 || metric.NextAttempt != 2 || metric.Delay != time.Second {
		t.Fatalf("unexpected retry metric: %+v", metric)
	}
	if len(result.Response.Actions) != 1 || result.Response.Actions[0].GetCreateTimer() == nil {
		t.Fatalf("expected retry timer action, got %v", result.Response.Actions)
	}

}

func TestRetryMetricIsSuppressedDuringReplay(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("retry", func(ctx *OrchestrationContext) (any, error) {
		return nil, ctx.CallActivity("flaky", WithActivityRetryPolicy(&RetryPolicy{
			MaxAttempts:          2,
			InitialRetryInterval: time.Second,
		})).Await(nil)
	}); err != nil {
		t.Fatal(err)
	}

	metricCount := 0
	instanceID := api.InstanceID("retry-replay")
	_, err := NewTaskExecutor(registry, WithMetricsHooks(MetricsHooks{
		Retry: func(RetryMetric) {
			metricCount++
		},
	})).ExecuteOrchestrator(

		context.Background(),
		instanceID,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("retry", string(instanceID), nil, nil, nil, nil),
			helpers.NewTaskScheduledEvent(0, "flaky", nil, nil, nil),
			helpers.NewTaskFailedEvent(0, &protos.TaskFailureDetails{
				ErrorType:    "flaky",
				ErrorMessage: "try again",
			}),
			helpers.NewTimerCreatedEvent(1, nil),
		},
		nil, nil)

	if err != nil && !errors.Is(err, ErrTaskBlocked) {
		t.Fatal(err)
	}
	if metricCount != 0 {
		t.Fatalf("retry metric replay count = %d, want 0", metricCount)
	}
}
