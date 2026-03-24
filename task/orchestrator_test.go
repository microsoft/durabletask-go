package task

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_computeNextDelay(t *testing.T) {
	time1 := time.Now()
	time2 := time.Now().Add(1 * time.Minute)
	type args struct {
		currentTimeUtc time.Time
		policy         RetryPolicy
		attempt        int
		firstAttempt   time.Time
		err            error
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
					Handle:               func(err error) bool { return true },
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
					Handle:               func(err error) bool { return true },
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
					Handle:               func(err error) bool { return true },
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
					Handle:               func(err error) bool { return true },
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
					Handle:               func(err error) bool { return true },
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
					Handle:               func(err error) bool { return true },
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
			if got := computeNextDelay(tt.args.currentTimeUtc, tt.args.policy, tt.args.attempt, tt.args.firstAttempt, tt.args.err); got != tt.want {
				t.Errorf("computeNextDelay() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_NewGuid_Deterministic(t *testing.T) {
	ctx := &OrchestrationContext{
		ID:             "test-instance-123",
		CurrentTimeUtc: time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC),
	}

	// Generate two GUIDs and verify they're different
	guid1 := ctx.NewGuid()
	guid2 := ctx.NewGuid()
	if guid1 == guid2 {
		t.Errorf("expected different GUIDs, got same: %s", guid1)
	}

	// Verify determinism by resetting the counter
	ctx.newGuidCounter = 0
	guid1Again := ctx.NewGuid()
	if guid1 != guid1Again {
		t.Errorf("expected deterministic GUID, got %s vs %s", guid1, guid1Again)
	}
}

func Test_NewGuid_Format(t *testing.T) {
	ctx := &OrchestrationContext{
		ID:             "test-instance",
		CurrentTimeUtc: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
	}

	guid := ctx.NewGuid()
	// UUID format: 8-4-4-4-12 hex chars
	if len(guid) != 36 {
		t.Errorf("expected UUID length 36, got %d: %s", len(guid), guid)
	}
	if guid[8] != '-' || guid[13] != '-' || guid[18] != '-' || guid[23] != '-' {
		t.Errorf("expected UUID format with dashes, got: %s", guid)
	}
	// Version should be 5 (char at position 14)
	if guid[14] != '5' {
		t.Errorf("expected version 5 at position 14, got: %c in %s", guid[14], guid)
	}
}

func Test_OrchestrationContext_SignalEntity_SetsParentInstanceID(t *testing.T) {
	ctx := &OrchestrationContext{
		ID:             api.InstanceID("orchestrator-instance"),
		pendingActions: make(map[int32]*protos.OrchestratorAction),
	}

	err := ctx.SignalEntity(api.NewEntityID("counter", "key1"), "increment")
	require.NoError(t, err)
	require.Len(t, ctx.pendingActions, 1)

	var action *protos.OrchestratorAction
	for _, candidate := range ctx.pendingActions {
		action = candidate
	}
	require.NotNil(t, action)

	send := action.GetSendEvent()
	require.NotNil(t, send)

	var msg helpers.EntityRequestMessage
	require.NoError(t, json.Unmarshal([]byte(send.Data.GetValue()), &msg))
	assert.Equal(t, "orchestrator-instance", msg.ParentInstanceID)
	assert.True(t, msg.IsSignal)
	assert.Equal(t, "increment", msg.Operation)
}
