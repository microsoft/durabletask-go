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
	type args struct {
		currentTimeUtc time.Time
		policy         RetryPolicy
		attempt        int
		firstAttempt   time.Time
	}
	tests := []struct {
		name string
		args args
		want time.Duration
	}{
		{
			name: "first attempt",
			args: args{
				currentTimeUtc: time2,
				policy: RetryPolicy{
					MaxAttempts:          3,
					InitialRetryInterval: 2 * time.Second,
					BackoffCoefficient:   2,
					MaxRetryInterval:     10 * time.Second,
					Handle:               func(RetryContext) bool { return true },
					RetryTimeout:         2 * time.Minute,
				},
				attempt:      0,
				firstAttempt: time1,
			},
			want: 2 * time.Second,
		},
		{
			name: "second attempt",
			args: args{
				currentTimeUtc: time2,
				policy: RetryPolicy{
					MaxAttempts:          3,
					InitialRetryInterval: 2 * time.Second,
					BackoffCoefficient:   2,
					MaxRetryInterval:     10 * time.Second,
					Handle:               func(RetryContext) bool { return true },
					RetryTimeout:         2 * time.Minute,
				},
				attempt:      1,
				firstAttempt: time1,
			},
			want: 4 * time.Second,
		},
		{
			name: "third attempt",
			args: args{
				currentTimeUtc: time2,
				policy: RetryPolicy{
					MaxAttempts:          3,
					InitialRetryInterval: 2 * time.Second,
					BackoffCoefficient:   2,
					MaxRetryInterval:     10 * time.Second,
					Handle:               func(RetryContext) bool { return true },
					RetryTimeout:         2 * time.Minute,
				},
				attempt:      2,
				firstAttempt: time1,
			},
			want: 8 * time.Second,
		},
		{
			name: "fourth attempt",
			args: args{
				currentTimeUtc: time2,
				policy: RetryPolicy{
					MaxAttempts:          3,
					InitialRetryInterval: 2 * time.Second,
					BackoffCoefficient:   2,
					MaxRetryInterval:     10 * time.Second,
					Handle:               func(RetryContext) bool { return true },
					RetryTimeout:         2 * time.Minute,
				},
				attempt:      3,
				firstAttempt: time1,
			},
			want: 10 * time.Second,
		},
		{
			name: "expired",
			args: args{
				currentTimeUtc: time2,
				policy: RetryPolicy{
					MaxAttempts:          3,
					InitialRetryInterval: 2 * time.Second,
					BackoffCoefficient:   2,
					MaxRetryInterval:     10 * time.Second,
					Handle:               func(RetryContext) bool { return true },
					RetryTimeout:         30 * time.Second,
				},
				attempt:      3,
				firstAttempt: time1,
			},
			want: 0,
		},
		{
			name: "fourth attempt backoff 1",
			args: args{
				currentTimeUtc: time2,
				policy: RetryPolicy{
					MaxAttempts:          3,
					InitialRetryInterval: 2 * time.Second,
					BackoffCoefficient:   1,
					MaxRetryInterval:     10 * time.Second,
					Handle:               func(RetryContext) bool { return true },
					RetryTimeout:         2 * time.Minute,
				},
				attempt:      3,
				firstAttempt: time1,
			},
			want: 2 * time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &TaskFailedError{
				TaskName:       "activity",
				FailureDetails: &api.FailureDetails{ErrorType: "TestError", ErrorMessage: "failed"},
			}
			if got := computeNextDelay(tt.args.currentTimeUtc, tt.args.policy, tt.args.attempt, tt.args.firstAttempt, err); got != tt.want {
				t.Errorf("computeNextDelay() = %v, want %v", got, tt.want)
			}
		})
	}
}
