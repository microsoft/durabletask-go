package task

import (
	"log/slog"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/durationpb"
)

func supportedEntityParameters() *protos.OrchestratorEntityParameters {
	return &protos.OrchestratorEntityParameters{
		EntityMessageReorderWindow: durationpb.New(0),
	}
}

func newTestOrchestrationContext(
	registry *TaskRegistry,
	id api.InstanceID,
	oldEvents []*protos.HistoryEvent,
	newEvents []*protos.HistoryEvent,
) *OrchestrationContext {
	return newOrchestrationContext(
		registry,
		id,
		oldEvents,
		newEvents,
		OrchestrationOptions{},
		slog.Default(),
		MetricsHooks{},
		nil,
		"",
		api.DefaultDataConverter(),
		true,
	)
}

func Test_computeNextDelay(t *testing.T) {
	time1 := time.Now()
	time2 := time.Now().Add(1 * time.Minute)
	basePolicy := RetryPolicy{
		MaxAttempts:          3,
		InitialRetryInterval: 2 * time.Second,
		BackoffCoefficient:   2,
		MaxRetryInterval:     10 * time.Second,
		Handle:               func(RetryContext) bool { return true },
		RetryTimeout:         2 * time.Minute,
	}
	tests := []struct {
		name        string
		attempt     int
		coefficient float64
		timeout     time.Duration
		want        time.Duration
	}{
		{"first attempt", 0, 2, 2 * time.Minute, 2 * time.Second},
		{"second attempt", 1, 2, 2 * time.Minute, 4 * time.Second},
		{"third attempt", 2, 2, 2 * time.Minute, 8 * time.Second},
		{"fourth attempt", 3, 2, 2 * time.Minute, 10 * time.Second},
		{"expired", 3, 2, 30 * time.Second, 0},
		{"fourth attempt backoff 1", 3, 1, 2 * time.Minute, 2 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := basePolicy
			policy.BackoffCoefficient = tt.coefficient
			policy.RetryTimeout = tt.timeout
			err := &TaskFailedError{
				TaskName:       "activity",
				FailureDetails: &api.FailureDetails{ErrorType: "TestError", ErrorMessage: "failed"},
			}
			if got := computeNextDelay(time2, policy, tt.attempt, time1, err); got != tt.want {
				t.Errorf("computeNextDelay() = %v, want %v", got, tt.want)
			}
		})
	}
}
