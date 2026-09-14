package durabletaskscheduler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"math"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/failure"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/internal/tagcodec"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// minTimestamp and maxTimestamp mirror DateTimeOffset.MinValue/MaxValue. The
// exact minimum is Go's zero time, which the schedule wire format encodes as an
// absent value, so the boundary cases use the smallest representable non-zero
// instant instead.
var (
	minTimestamp     = time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC)
	nearMinTimestamp = minTimestamp.Add(100 * time.Nanosecond)
	maxTimestamp     = time.Date(9999, time.December, 31, 23, 59, 59, 999999900, time.UTC)
)

// ---------------------------------------------------------------------------
// Next-run calculation
// ---------------------------------------------------------------------------

func TestDetermineNextRunMatrix(t *testing.T) {
	now := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	minute := time.Minute

	tests := []struct {
		name        string
		startAt     time.Time
		createdAt   time.Time
		lastRunAt   time.Time
		nextRunAt   time.Time
		interval    time.Duration
		immediate   bool
		want        time.Time
		wantErrorIs error
	}{
		{
			name:     "start in future is honored",
			startAt:  now.Add(5 * minute),
			interval: 5 * minute,
			want:     now.Add(5 * minute),
		},
		{
			name:      "start in future ignores start immediately if late",
			startAt:   now.Add(5 * minute),
			interval:  5 * minute,
			immediate: true,
			want:      now.Add(5 * minute),
		},
		{
			name:      "start in future with prior run still waits for start",
			startAt:   now.Add(90 * time.Second),
			lastRunAt: now.Add(-time.Hour),
			interval:  5 * minute,
			want:      now.Add(90 * time.Second),
		},
		{
			name:      "late start runs immediately on first run",
			startAt:   now.Add(-5 * minute),
			interval:  5 * minute,
			immediate: true,
			want:      now,
		},
		{
			name:     "late start without immediate waits for next boundary",
			startAt:  now.Add(-5 * minute),
			interval: 3 * minute,
			want:     now.Add(-5 * minute).Add(6 * minute),
		},
		{
			name:      "immediate is ignored once a run happened",
			startAt:   now.Add(-10 * minute),
			lastRunAt: now.Add(-minute),
			interval:  3 * minute,
			immediate: true,
			want:      now.Add(-10 * minute).Add(12 * minute),
		},
		{
			name:     "exact interval boundary advances one full interval",
			startAt:  now.Add(-9 * minute),
			interval: 3 * minute,
			want:     now.Add(3 * minute),
		},
		{
			name:     "start equal to now advances one full interval",
			startAt:  now,
			interval: 5 * minute,
			want:     now.Add(5 * minute),
		},
		{
			name:     "fractional interval keeps sub-second precision",
			startAt:  now.Add(-4 * time.Second),
			interval: 1500 * time.Millisecond,
			want:     now.Add(-4 * time.Second).Add(4500 * time.Millisecond),
		},
		{
			name:      "creation time anchors the schedule when start is unset",
			createdAt: now.Add(-10 * minute),
			interval:  3 * minute,
			want:      now.Add(-10 * minute).Add(12 * minute),
		},
		{
			name:     "now anchors the schedule when start and creation are unset",
			interval: 5 * minute,
			want:     now.Add(5 * minute),
		},
		{
			name:      "existing next run is returned unchanged",
			startAt:   now.Add(-time.Hour),
			nextRunAt: now.Add(-time.Hour),
			interval:  5 * minute,
			immediate: true,
			want:      now.Add(-time.Hour),
		},
		{
			name:     "near-minimum start stays on the interval grid",
			startAt:  nearMinTimestamp,
			interval: time.Second,
			want:     now.Add(100 * time.Nanosecond),
		},
		{
			name:     "maximum end time does not affect the next run",
			startAt:  now.Add(-time.Hour),
			interval: 45 * minute,
			want:     now.Add(-time.Hour).Add(90 * minute),
		},
		{
			name:        "interval below one second is rejected",
			interval:    500 * time.Millisecond,
			wantErrorIs: ErrScheduleValidation,
		},
		{
			name:        "zero interval is rejected",
			wantErrorIs: ErrScheduleValidation,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := &scheduleState{
				LastRunAt:         test.lastRunAt,
				NextRunAt:         test.nextRunAt,
				ScheduleCreatedAt: test.createdAt,
			}
			config := &scheduleConfiguration{
				StartAt:                test.startAt,
				Interval:               dotNetSpan(test.interval),
				StartImmediatelyIfLate: test.immediate,
			}
			next, err := determineNextRun(state, config, now)
			if test.wantErrorIs != nil {
				require.ErrorIs(t, err, test.wantErrorIs)
				return
			}
			require.NoError(t, err)
			require.True(t, test.want.Equal(next), "want %s, got %s", test.want, next)
		})
	}
}

// TestDetermineNextRunLandsOnIntervalGrid verifies the .NET formula
// start + interval*(elapsed/interval + 1) using independent big.Int math, which
// time.Duration cannot express for extreme start times.
func TestDetermineNextRunLandsOnIntervalGrid(t *testing.T) {
	now := time.Date(2026, time.August, 27, 12, 34, 56, 789012300, time.UTC)

	tests := []struct {
		name     string
		startAt  time.Time
		interval time.Duration
	}{
		{name: "near minimum start with one second interval", startAt: nearMinTimestamp, interval: time.Second},
		{name: "near minimum start with fractional interval", startAt: nearMinTimestamp, interval: 1500 * time.Millisecond},
		{name: "year 1200 start", startAt: time.Date(1200, time.June, 1, 3, 4, 5, 600, time.UTC), interval: 7 * time.Hour},
		{name: "recent start", startAt: now.Add(-987654321 * time.Nanosecond), interval: 90 * time.Second},
		{name: "century old start", startAt: now.AddDate(-100, 0, 0), interval: 13*time.Minute + 7*time.Second},
		{name: "very large interval", startAt: nearMinTimestamp, interval: 200000 * time.Hour},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			next, err := determineNextRun(
				&scheduleState{},
				&scheduleConfiguration{StartAt: test.startAt, Interval: dotNetSpan(test.interval)},
				now,
			)
			require.NoError(t, err)
			require.True(t, next.After(now), "next run %s must be after %s", next, now)
			require.False(t, next.After(now.Add(test.interval)), "next run must be within one interval")
			offset := new(big.Int).Sub(nanosecondsSinceEpoch(next), nanosecondsSinceEpoch(test.startAt))
			remainder := new(big.Int).Mod(offset, big.NewInt(int64(test.interval)))
			require.Equal(t, "0", remainder.String(), "next run must sit on the interval grid")
		})
	}
}

// TestDetermineNextRunRejectsTimesBeyondTheSupportedRange pins the upper
// boundary of the .NET schedule wire format: a next run one tick past
// DateTimeOffset.MaxValue must be rejected up front rather than serialized as a
// year-10000 timestamp that only fails later.
func TestDetermineNextRunRejectsTimesBeyondTheSupportedRange(t *testing.T) {
	tests := []struct {
		name     string
		now      time.Time
		interval time.Duration
		want     time.Time
	}{
		{
			name:     "next run exactly at the maximum is allowed",
			now:      maxTimestamp.Add(-time.Hour),
			interval: time.Hour,
			want:     maxTimestamp,
		},
		{
			name:     "next run one tick past the maximum is rejected",
			now:      maxTimestamp.Add(-time.Hour).Add(100 * time.Nanosecond),
			interval: time.Hour,
		},
		{
			name:     "next run far past the maximum is rejected",
			now:      maxTimestamp.Add(-time.Second),
			interval: 24 * time.Hour,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			next, err := determineNextRun(
				&scheduleState{},
				// Anchoring the start at now makes the next run land exactly
				// one interval later.
				&scheduleConfiguration{StartAt: test.now, Interval: dotNetSpan(test.interval)},
				test.now,
			)
			if test.want.IsZero() {
				require.ErrorIs(t, err, ErrScheduleValidation)
				return
			}
			require.NoError(t, err)
			require.True(t, test.want.Equal(next), "want %s, got %s", test.want, next)
		})
	}
}

func TestIntervalRemainderIsExact(t *testing.T) {
	now := time.Date(2026, time.August, 27, 12, 0, 0, 500, time.UTC)
	tests := []struct {
		name     string
		start    time.Time
		interval time.Duration
	}{
		{name: "zero elapsed", start: now, interval: time.Minute},
		{name: "sub interval", start: now.Add(-999 * time.Millisecond), interval: time.Second},
		{name: "exact multiple", start: now.Add(-3 * time.Hour), interval: time.Hour},
		{name: "near minimum start", start: nearMinTimestamp, interval: time.Second},
		{name: "near minimum start fractional", start: nearMinTimestamp, interval: 1234567891 * time.Nanosecond},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := intervalRemainder(test.start, now, test.interval)
			elapsed := new(big.Int).Sub(nanosecondsSinceEpoch(now), nanosecondsSinceEpoch(test.start))
			want := new(big.Int).Mod(elapsed, big.NewInt(int64(test.interval)))
			require.Equal(t, want.String(), big.NewInt(int64(got)).String())
		})
	}
}

// TestIntervalRemainderClampsInputsOutsideTheDivisionDomain pins the two guards
// that keep the 128-bit division well defined: a non-positive interval would
// divide by zero, and a start after now would make the elapsed time negative.
func TestIntervalRemainderClampsInputsOutsideTheDivisionDomain(t *testing.T) {
	now := time.Date(2026, time.August, 27, 12, 0, 0, 500, time.UTC)
	require.Zero(t, intervalRemainder(now.Add(-time.Hour), now, 0))
	require.Zero(t, intervalRemainder(now.Add(-time.Hour), now, -time.Minute))
	require.Zero(t, intervalRemainder(now.Add(time.Hour), now, time.Minute))
}

func nanosecondsSinceEpoch(value time.Time) *big.Int {
	total := new(big.Int).Mul(big.NewInt(value.Unix()), big.NewInt(int64(time.Second)))
	return total.Add(total, big.NewInt(int64(value.Nanosecond())))
}

// ---------------------------------------------------------------------------
// .NET-compatible serialization boundaries
// ---------------------------------------------------------------------------

func TestDotNetSpanRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		json     string
	}{
		{name: "zero", duration: 0, json: `"00:00:00"`},
		{name: "one second", duration: time.Second, json: `"00:00:01"`},
		{name: "fractional", duration: 1500 * time.Millisecond, json: `"00:00:01.5000000"`},
		{name: "one tick", duration: 100 * time.Nanosecond, json: `"00:00:00.0000001"`},
		{name: "one day", duration: 24 * time.Hour, json: `"1.00:00:00"`},
		{name: "days hours minutes", duration: 26*time.Hour + 3*time.Minute + 4*time.Second + 500*time.Millisecond, json: `"1.02:03:04.5000000"`},
		{name: "maximum tick aligned duration", duration: (math.MaxInt64 / 100) * 100, json: `"106751.23:47:16.8547758"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload, err := json.Marshal(dotNetSpan(test.duration))
			require.NoError(t, err)
			require.Equal(t, test.json, string(payload))

			var decoded dotNetSpan
			require.NoError(t, json.Unmarshal(payload, &decoded))
			require.Equal(t, test.duration, time.Duration(decoded))
		})
	}
}

func TestDotNetSpanRejectsInvalidValues(t *testing.T) {
	for _, value := range []string{
		`"-00:00:01"`,
		`"00:00"`,
		`"00:60:00"`,
		`"00:00:60"`,
		`"00:00:00.00000001"`,
		`"1.24:00:00"`,
		`"abc"`,
		`"1.aa:00:00"`,
		`5`,
	} {
		t.Run(value, func(t *testing.T) {
			var decoded dotNetSpan
			require.Error(t, json.Unmarshal([]byte(value), &decoded))
		})
	}
	_, err := json.Marshal(dotNetSpan(-time.Second))
	require.Error(t, err)
}

func TestScheduleCreationOptionsJSONBoundaries(t *testing.T) {
	tests := []struct {
		name    string
		options ScheduleCreationOptions
		want    string
	}{
		{
			name: "minimum start is encoded as absent",
			options: ScheduleCreationOptions{
				ScheduleID: "min", OrchestrationName: "Run", Interval: time.Second, StartAt: minTimestamp,
			},
			want: `{"ScheduleId":"min","OrchestrationName":"Run","Interval":"00:00:01","StartImmediatelyIfLate":false}`,
		},
		{
			name: "near minimum start round-trips",
			options: ScheduleCreationOptions{
				ScheduleID: "near-min", OrchestrationName: "Run", Interval: time.Second, StartAt: nearMinTimestamp,
			},
			want: `{"ScheduleId":"near-min","OrchestrationName":"Run","Interval":"00:00:01","StartAt":"0001-01-01T00:00:00.0000001Z","StartImmediatelyIfLate":false}`,
		},
		{
			name: "maximum end round-trips",
			options: ScheduleCreationOptions{
				ScheduleID: "max", OrchestrationName: "Run", Interval: time.Second, EndAt: maxTimestamp,
			},
			want: `{"ScheduleId":"max","OrchestrationName":"Run","Interval":"00:00:01","EndAt":"9999-12-31T23:59:59.9999999Z","StartImmediatelyIfLate":false}`,
		},
		{
			name: "unicode schedule id and json input",
			options: ScheduleCreationOptions{
				ScheduleID: "日程-🌟", OrchestrationName: "Run", Interval: time.Second,
				OrchestrationInput: `{"key":"value with \"quotes\""}`,
			},
			want: `{"ScheduleId":"日程-🌟","OrchestrationName":"Run","Interval":"00:00:01","OrchestrationInput":"{\"key\":\"value with \\\"quotes\\\"\"}","StartImmediatelyIfLate":false}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload, err := json.Marshal(test.options)
			require.NoError(t, err)
			require.JSONEq(t, test.want, string(payload))

			var decoded ScheduleCreationOptions
			require.NoError(t, json.Unmarshal(payload, &decoded))
			require.Equal(t, test.options.ScheduleID, decoded.ScheduleID)
			require.Equal(t, test.options.Interval, decoded.Interval)
			require.Equal(t, test.options.OrchestrationInput, decoded.OrchestrationInput)
			if test.options.StartAt.Equal(minTimestamp) {
				require.True(t, decoded.StartAt.IsZero())
			} else {
				require.True(t, test.options.StartAt.Equal(decoded.StartAt))
			}
			require.True(t, test.options.EndAt.Equal(decoded.EndAt))
		})
	}
}

func TestScheduleUpdateOptionsOmitsUnsetFields(t *testing.T) {
	payload, err := json.Marshal(ScheduleUpdateOptions{})
	require.NoError(t, err)
	require.JSONEq(t, `{}`, string(payload))

	name := ""
	payload, err = json.Marshal(ScheduleUpdateOptions{OrchestrationName: &name})
	require.NoError(t, err)
	require.JSONEq(t, `{"OrchestrationName":""}`, string(payload))

	var decoded ScheduleUpdateOptions
	require.NoError(t, json.Unmarshal(payload, &decoded))
	require.NotNil(t, decoded.OrchestrationName)
	require.Empty(t, *decoded.OrchestrationName)
}

func TestScheduleUpdateMapPresenceAcrossClientOrchestratorAndEntity(t *testing.T) {
	for _, clearMaps := range []bool{false, true} {
		name := "nil maps preserve values"
		if clearMaps {
			name = "empty maps clear values"
		}
		t.Run(name, func(t *testing.T) {
			backend := &recordingScheduleBackend{}
			handle, err := (&ScheduledTaskClient{client: backend}).GetScheduleClient("daily")
			require.NoError(t, err)
			options := ScheduleUpdateOptions{}
			if clearMaps {
				options.Tags = map[string]string{}
				options.ContextFields = api.ContextFields{}
			}
			require.NoError(t, handle.Update(context.Background(), options))
			payload, err := api.DefaultDataConverter().Serialize(backend.request)
			require.NoError(t, err)
			registry := newScheduleRegistry(t)
			response := executeScheduleOperationTurn(t, registry, nil, []*protos.HistoryEvent{
				helpers.NewOrchestratorStartedEvent(),
				helpers.NewExecutionStartedEvent(ExecuteScheduleOperationOrchestratorName, "op",
					wrapperspb.String(payload), nil, nil, nil),
			})
			require.Len(t, response.Actions, 1)
			called := response.Actions[0].GetSendEntityMessage().GetEntityOperationCalled()
			require.NotNil(t, called)

			state := seededScheduleState(ScheduleStatusActive)
			state.ScheduleConfiguration.Tags = map[string]string{"tag": "old"}
			state.ScheduleConfiguration.ContextFields = api.ContextFields{"field": "old"}
			state.ScheduleConfiguration.StartImmediatelyIfLate = true
			executor := newScheduleExecutor(t)
			updated, err := executor.ExecuteEntity(context.Background(), &protos.EntityBatchRequest{
				InstanceId:  called.TargetInstanceId.GetValue(),
				EntityState: marshalScheduleState(state),
				Operations: []*protos.OperationRequest{{
					RequestId: called.RequestId,
					Operation: called.Operation,
					Input:     called.Input,
				}},
			})
			require.NoError(t, err)
			requireOperationSucceeded(t, updated)
			current := decodeScheduleState(t, updated.EntityState)
			if clearMaps {
				require.Empty(t, current.ScheduleConfiguration.Tags)
				require.Empty(t, current.ScheduleConfiguration.ContextFields)
				require.NotEqual(t, state.ExecutionToken, current.ExecutionToken)
			} else {
				require.Equal(t, state.ScheduleConfiguration.Tags, current.ScheduleConfiguration.Tags)
				require.Equal(t, state.ScheduleConfiguration.ContextFields, current.ScheduleConfiguration.ContextFields)
				require.Equal(t, state.ExecutionToken, current.ExecutionToken)
			}
			run := runScheduleOperationBatch(t, executor, "daily", updated.EntityState, runScheduleOperation, current.ExecutionToken)
			requireOperationSucceeded(t, run)
			var started *protos.StartNewOrchestrationAction
			for _, action := range run.Actions {
				if start := action.GetStartNewOrchestration(); start != nil {
					started = start
				}
			}
			require.NotNil(t, started)
			if clearMaps {
				require.Equal(t, "Backup", started.Name)
			} else {
				require.Equal(t, ExecuteScheduledTaskOrchestratorName, started.Name)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Entity state transitions
// ---------------------------------------------------------------------------

func TestScheduleEntityTransitionMatrix(t *testing.T) {
	tests := []struct {
		name                  string
		status                ScheduleStatus
		operation             string
		input                 any
		useToken              bool
		wantInvalidTransition bool
		wantStateGone         bool
	}{
		{name: "create from uninitialized", status: ScheduleStatusUninitialized, operation: createScheduleOperation, input: validCreationOptions()},
		{name: "create from active", status: ScheduleStatusActive, operation: createScheduleOperation, input: validCreationOptions()},
		{name: "create from paused", status: ScheduleStatusPaused, operation: createScheduleOperation, input: validCreationOptions()},

		{name: "update from uninitialized", status: ScheduleStatusUninitialized, operation: updateScheduleOperation, input: intervalUpdate(10 * time.Minute), wantInvalidTransition: true},
		{name: "update from active", status: ScheduleStatusActive, operation: updateScheduleOperation, input: intervalUpdate(10 * time.Minute)},
		{name: "update from paused", status: ScheduleStatusPaused, operation: updateScheduleOperation, input: intervalUpdate(10 * time.Minute)},

		{name: "pause from uninitialized", status: ScheduleStatusUninitialized, operation: pauseScheduleOperation, wantInvalidTransition: true},
		{name: "pause from active", status: ScheduleStatusActive, operation: pauseScheduleOperation},
		{name: "pause from paused", status: ScheduleStatusPaused, operation: pauseScheduleOperation, wantInvalidTransition: true},

		{name: "resume from uninitialized", status: ScheduleStatusUninitialized, operation: resumeScheduleOperation, wantInvalidTransition: true},
		{name: "resume from active", status: ScheduleStatusActive, operation: resumeScheduleOperation, wantInvalidTransition: true},
		{name: "resume from paused", status: ScheduleStatusPaused, operation: resumeScheduleOperation},

		{name: "run from uninitialized deletes state", status: ScheduleStatusUninitialized, operation: runScheduleOperation, useToken: true, wantStateGone: true},
		{name: "run from active", status: ScheduleStatusActive, operation: runScheduleOperation, useToken: true},
		{name: "run from paused", status: ScheduleStatusPaused, operation: runScheduleOperation, useToken: true, wantInvalidTransition: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := newScheduleExecutor(t)
			state := seededScheduleState(test.status)
			input := test.input
			if test.useToken {
				input = state.ExecutionToken
			}
			result := runScheduleOperationBatch(t, executor, "daily", marshalScheduleState(state), test.operation, input)
			if test.wantInvalidTransition {
				requireOperationErrorType(t, result, scheduleInvalidTransitionType)
				return
			}
			requireOperationSucceeded(t, result)
			if test.wantStateGone {
				require.Nil(t, result.EntityState)
				return
			}
			require.NotNil(t, result.EntityState)
		})
	}
}

func TestScheduleEntityUpdateNoOpInvariants(t *testing.T) {
	sameValues := ScheduleUpdateOptions{
		OrchestrationName: pointerTo("Backup"),
		Interval:          pointerTo(5 * time.Minute),
	}
	emptyStrings := ScheduleUpdateOptions{
		OrchestrationName:       pointerTo(""),
		OrchestrationInput:      pointerTo(""),
		OrchestrationInstanceID: pointerTo(""),
		OrchestrationVersion:    pointerTo(""),
	}
	unchangedFlags := ScheduleUpdateOptions{
		StartImmediatelyIfLate: pointerTo(false),
		EndAt:                  pointerTo(time.Time{}),
	}

	tests := []struct {
		name    string
		status  ScheduleStatus
		options ScheduleUpdateOptions
	}{
		{name: "all fields nil on active", status: ScheduleStatusActive, options: ScheduleUpdateOptions{}},
		{name: "all fields nil on paused", status: ScheduleStatusPaused, options: ScheduleUpdateOptions{}},
		{name: "same values on active", status: ScheduleStatusActive, options: sameValues},
		{name: "same values on paused", status: ScheduleStatusPaused, options: sameValues},
		{name: "empty strings are ignored", status: ScheduleStatusActive, options: emptyStrings},
		{name: "unchanged flags are ignored", status: ScheduleStatusActive, options: unchangedFlags},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := newScheduleExecutor(t)
			before := seededScheduleState(test.status)
			before.LastRunAt = time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
			before.NextRunAt = time.Date(2026, time.August, 1, 0, 5, 0, 0, time.UTC)

			result := runScheduleOperationBatch(t, executor, "daily", marshalScheduleState(before),
				updateScheduleOperation, test.options)
			requireOperationSucceeded(t, result)
			after := decodeScheduleState(t, result.EntityState)

			require.Empty(t, result.Actions, "a no-op update must not resignal the schedule")
			require.Equal(t, before.ExecutionToken, after.ExecutionToken, "a no-op update must not rotate the token")
			require.True(t, before.ScheduleLastModifiedAt.Equal(after.ScheduleLastModifiedAt))
			require.True(t, before.NextRunAt.Equal(after.NextRunAt))
			require.True(t, before.LastRunAt.Equal(after.LastRunAt))
			require.Equal(t, before.Status, after.Status)
			require.Equal(t, before.ScheduleConfiguration, after.ScheduleConfiguration)
		})
	}
}

func TestScheduleEntityUpdateRotatesTokenAndResetsNextRun(t *testing.T) {
	tests := []struct {
		name          string
		status        ScheduleStatus
		options       ScheduleUpdateOptions
		wantResetNext bool
		wantSignal    bool
	}{
		{
			name:          "interval change resets next run and resignals",
			status:        ScheduleStatusActive,
			options:       intervalUpdate(6 * time.Minute),
			wantResetNext: true,
			wantSignal:    true,
		},
		{
			name:          "start change resets next run",
			status:        ScheduleStatusActive,
			options:       ScheduleUpdateOptions{StartAt: pointerTo(time.Date(2030, time.January, 1, 0, 0, 0, 0, time.UTC))},
			wantResetNext: true,
			wantSignal:    true,
		},
		{
			name:          "start immediately change resets next run",
			status:        ScheduleStatusActive,
			options:       ScheduleUpdateOptions{StartImmediatelyIfLate: pointerTo(true)},
			wantResetNext: true,
			wantSignal:    true,
		},
		{
			name:       "end change keeps next run",
			status:     ScheduleStatusActive,
			options:    ScheduleUpdateOptions{EndAt: pointerTo(time.Date(2030, time.January, 1, 0, 0, 0, 0, time.UTC))},
			wantSignal: true,
		},
		{
			name:       "orchestration name change keeps next run",
			status:     ScheduleStatusActive,
			options:    ScheduleUpdateOptions{OrchestrationName: pointerTo("Restore")},
			wantSignal: true,
		},
		{
			name:          "paused schedules rotate the token without resignaling",
			status:        ScheduleStatusPaused,
			options:       intervalUpdate(6 * time.Minute),
			wantResetNext: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := newScheduleExecutor(t)
			before := seededScheduleState(test.status)
			before.NextRunAt = time.Date(2026, time.August, 1, 0, 5, 0, 0, time.UTC)

			result := runScheduleOperationBatch(t, executor, "daily", marshalScheduleState(before),
				updateScheduleOperation, test.options)
			requireOperationSucceeded(t, result)
			after := decodeScheduleState(t, result.EntityState)

			require.NotEqual(t, before.ExecutionToken, after.ExecutionToken)
			require.False(t, after.ScheduleLastModifiedAt.Before(before.ScheduleLastModifiedAt))
			if test.wantResetNext {
				require.True(t, after.NextRunAt.IsZero())
			} else {
				require.True(t, before.NextRunAt.Equal(after.NextRunAt))
			}
			if test.wantSignal {
				require.Len(t, result.Actions, 1)
				signal := result.Actions[0].GetSendSignal()
				require.NotNil(t, signal)
				require.Equal(t, runScheduleOperation, signal.GetName())
				require.Equal(t, after.ExecutionToken, decodeSignalToken(t, signal))
			} else {
				require.Empty(t, result.Actions)
			}
		})
	}
}

func TestScheduleEntityRunTokenMatrix(t *testing.T) {
	tests := []struct {
		name  string
		token func(current string) string
	}{
		{name: "empty token", token: func(string) string { return "" }},
		{name: "whitespace token", token: func(string) string { return "   " }},
		{name: "superseded token", token: func(string) string { return "0123456789abcdef0123456789abcdef" }},
		{name: "token with different case", token: strings.ToUpper},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := newScheduleExecutor(t)
			before := seededScheduleState(ScheduleStatusActive)
			before.ScheduleConfiguration.StartImmediatelyIfLate = true

			result := runScheduleOperationBatch(t, executor, "daily", marshalScheduleState(before),
				runScheduleOperation, test.token(before.ExecutionToken))
			requireOperationSucceeded(t, result)
			after := decodeScheduleState(t, result.EntityState)

			require.Empty(t, result.Actions, "a stale run must not start work or resignal")
			require.True(t, after.LastRunAt.IsZero())
			require.Equal(t, before.ExecutionToken, after.ExecutionToken)
			require.Equal(t, ScheduleStatusActive, after.Status)
		})
	}
}

func TestScheduleEntityRunSupersededAfterUpdate(t *testing.T) {
	executor := newScheduleExecutor(t)
	created := runScheduleOperationBatch(t, executor, "daily", nil, createScheduleOperation, ScheduleCreationOptions{
		ScheduleID:             "daily",
		OrchestrationName:      "Backup",
		Interval:               5 * time.Minute,
		StartImmediatelyIfLate: true,
	})
	requireOperationSucceeded(t, created)
	initialToken := decodeScheduleState(t, created.EntityState).ExecutionToken

	updated := runScheduleOperationBatch(t, executor, "daily", created.EntityState,
		updateScheduleOperation, intervalUpdate(6*time.Minute))
	requireOperationSucceeded(t, updated)
	currentToken := decodeScheduleState(t, updated.EntityState).ExecutionToken
	require.NotEqual(t, initialToken, currentToken)

	stale := runScheduleOperationBatch(t, executor, "daily", updated.EntityState, runScheduleOperation, initialToken)
	requireOperationSucceeded(t, stale)
	require.Empty(t, stale.Actions)
	require.True(t, decodeScheduleState(t, stale.EntityState).LastRunAt.IsZero())

	current := runScheduleOperationBatch(t, executor, "daily", stale.EntityState, runScheduleOperation, currentToken)
	requireOperationSucceeded(t, current)
	require.NotNil(t, current.Actions[0].GetStartNewOrchestration())
	require.False(t, decodeScheduleState(t, current.EntityState).LastRunAt.IsZero())
}

func TestScheduleEntityRunTimingMatrix(t *testing.T) {
	tests := []struct {
		name        string
		startOffset time.Duration
		endOffset   time.Duration
		hasStart    bool
		hasEnd      bool
		// maxEnd sets the end to DateTimeOffset.MaxValue instead of an offset
		// from now.
		maxEnd      bool
		immediate   bool
		wantStarted bool
		wantRetired bool
	}{
		{name: "future start does not run", startOffset: 5 * time.Minute, hasStart: true},
		{name: "future start ignores immediate", startOffset: 5 * time.Minute, hasStart: true, immediate: true},
		{name: "late start with immediate runs", startOffset: -5 * time.Minute, hasStart: true, immediate: true, wantStarted: true},
		{name: "late start without immediate waits", startOffset: -5 * time.Minute, hasStart: true},
		{name: "no start with immediate runs", immediate: true, wantStarted: true},
		{name: "no start without immediate waits"},
		{name: "expired end retires the schedule", endOffset: -time.Second, hasEnd: true, immediate: true, wantRetired: true},
		{name: "expired end retires even without immediate", endOffset: -5 * time.Minute, hasEnd: true, wantRetired: true},
		{name: "future end still runs", endOffset: time.Hour, hasEnd: true, immediate: true, wantStarted: true},
		{name: "maximum end still runs", maxEnd: true, immediate: true, wantStarted: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := newScheduleExecutor(t)
			now := time.Now().UTC()
			options := ScheduleCreationOptions{
				ScheduleID:             "daily",
				OrchestrationName:      "Backup",
				Interval:               5 * time.Minute,
				StartImmediatelyIfLate: test.immediate,
			}
			if test.hasStart {
				options.StartAt = now.Add(test.startOffset)
			}
			switch {
			case test.maxEnd:
				options.EndAt = maxTimestamp
			case test.hasEnd:
				options.EndAt = now.Add(test.endOffset)
			}

			created := runScheduleOperationBatch(t, executor, "daily", nil, createScheduleOperation, options)
			requireOperationSucceeded(t, created)
			token := decodeScheduleState(t, created.EntityState).ExecutionToken

			ran := runScheduleOperationBatch(t, executor, "daily", created.EntityState, runScheduleOperation, token)
			requireOperationSucceeded(t, ran)
			after := decodeScheduleState(t, ran.EntityState)

			switch {
			case test.wantRetired:
				require.Len(t, ran.Actions, 1)
				require.Equal(t, deleteScheduleOperation, ran.Actions[0].GetSendSignal().GetName())
				require.True(t, after.LastRunAt.IsZero())
				require.True(t, after.NextRunAt.IsZero())
			case test.wantStarted:
				require.Len(t, ran.Actions, 2)
				require.NotNil(t, ran.Actions[0].GetStartNewOrchestration())
				require.Equal(t, runScheduleOperation, ran.Actions[1].GetSendSignal().GetName())
				require.False(t, after.LastRunAt.IsZero())
				require.True(t, after.NextRunAt.After(after.LastRunAt))
			default:
				// The schedule is not due yet, so it only reschedules itself.
				require.Len(t, ran.Actions, 1)
				signal := ran.Actions[0].GetSendSignal()
				require.Equal(t, runScheduleOperation, signal.GetName())
				require.True(t, after.LastRunAt.IsZero())
				require.True(t, after.NextRunAt.After(now))
				require.True(t, signal.GetScheduledTime().AsTime().Equal(after.NextRunAt))
				if test.hasStart && test.startOffset > 0 {
					require.True(t, after.NextRunAt.Equal(options.StartAt.UTC()))
				}
			}
		})
	}
}

func TestScheduleEntityRunInstanceIDMatrix(t *testing.T) {
	tests := []struct {
		name       string
		configured string
		want       func(after scheduleState) string
	}{
		{
			name: "derived from the run time when unconfigured",
			want: func(after scheduleState) string {
				// The .NET round-trip format keeps 100ns precision and an
				// explicit UTC offset.
				return "daily-" + after.LastRunAt.UTC().Format("2006-01-02T15:04:05.0000000-07:00")
			},
		},
		{
			name:       "configured instance id is used verbatim",
			configured: "fixed-instance",
			want:       func(scheduleState) string { return "fixed-instance" },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := newScheduleExecutor(t)
			state := seededScheduleState(ScheduleStatusActive)
			state.ScheduleConfiguration.StartImmediatelyIfLate = true
			state.ScheduleConfiguration.OrchestrationInstanceID = test.configured

			ran := runScheduleOperationBatch(t, executor, "daily", marshalScheduleState(state),
				runScheduleOperation, state.ExecutionToken)
			requireOperationSucceeded(t, ran)

			start := ran.Actions[0].GetStartNewOrchestration()
			require.NotNil(t, start)
			require.Equal(t, test.want(decodeScheduleState(t, ran.EntityState)), start.InstanceId)
		})
	}
}

func TestScheduleEntityPauseAndResumeLifecycle(t *testing.T) {
	executor := newScheduleExecutor(t)
	created := runScheduleOperationBatch(t, executor, "daily", nil, createScheduleOperation, validCreationOptions())
	requireOperationSucceeded(t, created)
	createdState := decodeScheduleState(t, created.EntityState)
	require.Equal(t, ScheduleStatusActive, createdState.Status)
	require.Len(t, created.Actions, 1)

	paused := runScheduleOperationBatch(t, executor, "daily", created.EntityState, pauseScheduleOperation, nil)
	requireOperationSucceeded(t, paused)
	pausedState := decodeScheduleState(t, paused.EntityState)
	require.Equal(t, ScheduleStatusPaused, pausedState.Status)
	require.True(t, pausedState.NextRunAt.IsZero())
	require.NotEqual(t, createdState.ExecutionToken, pausedState.ExecutionToken)
	require.Empty(t, paused.Actions, "pausing must not signal a run")

	resumed := runScheduleOperationBatch(t, executor, "daily", paused.EntityState, resumeScheduleOperation, nil)
	requireOperationSucceeded(t, resumed)
	resumedState := decodeScheduleState(t, resumed.EntityState)
	require.Equal(t, ScheduleStatusActive, resumedState.Status)
	require.Equal(t, pausedState.ExecutionToken, resumedState.ExecutionToken, "resume keeps the paused token")
	require.Len(t, resumed.Actions, 1)
	require.Equal(t, resumedState.ExecutionToken, decodeSignalToken(t, resumed.Actions[0].GetSendSignal()))
}

func TestScheduleEntityCreateValidationMatrix(t *testing.T) {
	now := time.Now().UTC()
	tests := []struct {
		name    string
		options ScheduleCreationOptions
	}{
		{name: "missing schedule id", options: ScheduleCreationOptions{OrchestrationName: "Backup", Interval: time.Minute}},
		{name: "missing orchestration name", options: ScheduleCreationOptions{ScheduleID: "daily", Interval: time.Minute}},
		{name: "zero interval", options: ScheduleCreationOptions{ScheduleID: "daily", OrchestrationName: "Backup"}},
		{name: "sub second interval", options: ScheduleCreationOptions{ScheduleID: "daily", OrchestrationName: "Backup", Interval: 500 * time.Millisecond}},
		{
			name: "end before start",
			options: ScheduleCreationOptions{
				ScheduleID: "daily", OrchestrationName: "Backup", Interval: time.Minute,
				StartAt: now.Add(2 * time.Hour), EndAt: now.Add(time.Hour),
			},
		},
		{
			name: "reserved tag prefix",
			options: ScheduleCreationOptions{
				ScheduleID: "daily", OrchestrationName: "Backup", Interval: time.Minute,
				Tags: map[string]string{tagcodec.UserTagPrefix + "schedule": "daily"},
			},
		},
		{
			name: "empty tag key",
			options: ScheduleCreationOptions{
				ScheduleID: "daily", OrchestrationName: "Backup", Interval: time.Minute,
				Tags: map[string]string{"": "daily"},
			},
		},
		{
			name: "reserved context field prefix",
			options: ScheduleCreationOptions{
				ScheduleID: "daily", OrchestrationName: "Backup", Interval: time.Minute,
				ContextFields: api.ContextFields{api.ReservedContextFieldPrefix + "tenant": "north"},
			},
		},
		{
			name: "fixed instance id with retry policy",
			options: ScheduleCreationOptions{
				ScheduleID: "daily", OrchestrationName: "Backup", Interval: time.Minute,
				OrchestrationInstanceID: "fixed",
				RetryPolicy:             &ScheduleRetryPolicy{MaxAttempts: 2, InitialRetryInterval: time.Second},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := newScheduleExecutor(t)
			result := runScheduleOperationBatch(t, executor, "daily", nil, createScheduleOperation, test.options)
			requireOperationErrorType(t, result, scheduleValidationErrorType)
			require.Nil(t, result.EntityState, "a rejected creation must not persist state")
		})
	}
}

func TestScheduleRejectsReservedTargetsBeforeMutation(t *testing.T) {
	for _, path := range []string{"direct", "tags", "context"} {
		for _, target := range []string{"explicit", "derived"} {
			t.Run(path+"/"+target, func(t *testing.T) {
				options := validCreationOptions()
				if target == "explicit" {
					options.OrchestrationInstanceID = "@counter@one"
				} else {
					options.ScheduleID = "@counter@one"
				}
				switch path {
				case "tags":
					options.Tags = map[string]string{"schedule": "daily"}
				case "context":
					options.ContextFields = api.ContextFields{"tenant": "one"}
				}
				executor := newScheduleExecutor(t)
				for _, status := range []ScheduleStatus{ScheduleStatusUninitialized, ScheduleStatusActive, ScheduleStatusPaused} {
					t.Run("create/"+status.String(), func(t *testing.T) {
						var before *wrapperspb.StringValue
						if status != ScheduleStatusUninitialized {
							state := seededScheduleState(status)
							state.ScheduleConfiguration.ScheduleID = options.ScheduleID
							before = marshalScheduleState(state)
						}
						result := runScheduleOperationBatch(t, executor, options.ScheduleID, before, createScheduleOperation, options)
						requireOperationErrorType(t, result, scheduleValidationErrorType)
						require.Equal(t, before == nil, result.EntityState == nil)
						require.Equal(t, before.GetValue(), result.EntityState.GetValue())
						require.Empty(t, result.Actions)

						backend := &recordingScheduleBackend{completion: &api.OrchestrationMetadata{
							RuntimeStatus:  api.RUNTIME_STATUS_FAILED,
							FailureDetails: failure.FromProto(result.Results[0].GetFailure().GetFailureDetails()),
						}}
						client := &ScheduledTaskClient{client: backend, converter: api.DefaultDataConverter()}
						_, err := client.Create(t.Context(), options)
						require.ErrorIs(t, err, ErrScheduleValidation)
						var typed *ScheduleValidationError
						require.ErrorAs(t, err, &typed)
						require.Contains(t, typed.Message, "reserved entity format")
					})
				}
				for _, status := range []ScheduleStatus{ScheduleStatusActive, ScheduleStatusPaused} {
					t.Run("update/"+status.String(), func(t *testing.T) {
						state := seededScheduleState(status)
						state.ScheduleConfiguration.ScheduleID = options.ScheduleID
						state.ScheduleConfiguration.Tags = options.Tags
						state.ScheduleConfiguration.ContextFields = options.ContextFields
						before := marshalScheduleState(state)
						update := ScheduleUpdateOptions{
							OrchestrationName: pointerTo("Changed"),
							Interval:          pointerTo(6 * time.Minute),
						}
						if target == "explicit" {
							update.OrchestrationInstanceID = &options.OrchestrationInstanceID
						}
						result := runScheduleOperationBatch(t, executor, options.ScheduleID, before, updateScheduleOperation, update)
						requireOperationErrorType(t, result, scheduleValidationErrorType)
						require.Equal(t, before.GetValue(), result.EntityState.GetValue())
						require.Empty(t, result.Actions)
					})
				}
				err := validateCreation(options)
				require.ErrorIs(t, err, ErrScheduleValidation)
				var typed *ScheduleValidationError
				require.ErrorAs(t, err, &typed)
			})
		}
	}
}

func TestScheduleValidatesOnlyEffectiveTargets(t *testing.T) {
	for _, test := range []struct {
		name, scheduleID, instanceID string
		wrapper, retry               bool
	}{
		{name: "safe override", scheduleID: "@counter@one", instanceID: "safe"},
		{name: "wrapped safe override", scheduleID: "@counter@one", instanceID: "safe", wrapper: true},
		{name: "unused retry derived target", scheduleID: "@counter@one", retry: true},
		{name: "single at sign", scheduleID: "@counter"},
		{name: "empty entity name", scheduleID: "@@one"},
		{name: "wrapped derived target", scheduleID: "daily", wrapper: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := validCreationOptions()
			options.ScheduleID = test.scheduleID
			options.OrchestrationInstanceID = test.instanceID
			options.StartImmediatelyIfLate = true
			if test.wrapper {
				options.Tags = map[string]string{"schedule": "daily"}
				options.ContextFields = api.ContextFields{"tenant": "one"}
			}
			if test.retry {
				options.RetryPolicy = &ScheduleRetryPolicy{MaxAttempts: 2, InitialRetryInterval: time.Second}
			}
			require.NoError(t, validateCreation(options))
			registry := newScheduleRegistry(t)
			executor := task.NewTaskExecutor(registry).(entityExecutor)
			created := runScheduleOperationBatch(t, executor, options.ScheduleID, nil, createScheduleOperation, options)
			requireOperationSucceeded(t, created)
			updated := runScheduleOperationBatch(t, executor, options.ScheduleID, created.EntityState,
				updateScheduleOperation, intervalUpdate(6*time.Minute))
			requireOperationSucceeded(t, updated)
			state := decodeScheduleState(t, updated.EntityState)
			ran := runScheduleOperationBatch(t, executor, options.ScheduleID, updated.EntityState, runScheduleOperation, state.ExecutionToken)
			requireOperationSucceeded(t, ran)
			require.Len(t, ran.Actions, 2)
			started := ran.Actions[0].GetStartNewOrchestration()
			require.NotNil(t, started)
			require.NoError(t, helpers.ValidateOrchestrationInstanceID(started.InstanceId))
			targetID := started.InstanceId
			if test.wrapper || test.retry {
				require.Equal(t, ExecuteScheduledTaskOrchestratorName, started.Name)
				response := executeScheduleOperationTurn(t, registry, nil, []*protos.HistoryEvent{
					helpers.NewOrchestratorStartedEvent(),
					helpers.NewExecutionStartedEvent(started.Name, "op", started.Input, nil, nil, nil),
				})
				require.Len(t, response.Actions, 1)
				target := response.Actions[0].GetCreateSubOrchestration()
				require.NotNil(t, target)
				require.Equal(t, "Backup", target.Name)
				targetID = target.InstanceId
			}
			require.NoError(t, helpers.ValidateOrchestrationInstanceID(targetID))
			switch {
			case test.retry:
				require.Equal(t, "op:0000", targetID)
			case test.instanceID != "":
				require.Equal(t, test.instanceID, targetID)
			default:
				require.True(t, strings.HasPrefix(targetID, test.scheduleID+"-"))
			}
			require.Equal(t, runScheduleOperation, ran.Actions[1].GetSendSignal().GetName())
		})
	}
}

func TestScheduleRejectsEmptyContextKeysBeforeMutation(t *testing.T) {
	for _, status := range []ScheduleStatus{ScheduleStatusUninitialized, ScheduleStatusActive, ScheduleStatusPaused} {
		for _, operation := range []string{createScheduleOperation, updateScheduleOperation} {
			if status == ScheduleStatusUninitialized && operation == updateScheduleOperation {
				continue
			}
			t.Run(operation+"/"+status.String(), func(t *testing.T) {
				var before *wrapperspb.StringValue
				if status != ScheduleStatusUninitialized {
					before = marshalScheduleState(seededScheduleState(status))
				}
				options := validCreationOptions()
				options.ContextFields = api.ContextFields{"": "value"}
				var input any = options
				if operation == updateScheduleOperation {
					input = ScheduleUpdateOptions{
						OrchestrationName: pointerTo("Changed"),
						ContextFields:     options.ContextFields,
					}
				}
				result := runScheduleOperationBatch(t, newScheduleExecutor(t), options.ScheduleID, before, operation, input)
				requireOperationErrorType(t, result, scheduleValidationErrorType)
				require.Equal(t, before == nil, result.EntityState == nil)
				require.Equal(t, before.GetValue(), result.EntityState.GetValue())
				require.Empty(t, result.Actions)
				err := scheduleOperationFailure(options.ScheduleID, operation, &api.OrchestrationMetadata{
					RuntimeStatus:  api.RUNTIME_STATUS_FAILED,
					FailureDetails: failure.FromProto(result.Results[0].GetFailure().GetFailureDetails()),
				})
				require.ErrorIs(t, err, ErrScheduleValidation)
				var typed *ScheduleValidationError
				require.ErrorAs(t, err, &typed)
				require.Equal(t, "context field key cannot be empty", typed.Message)
			})
		}
	}
}

func TestScheduleNegativeIntervalIsRejectedBeforeTheWire(t *testing.T) {
	require.ErrorIs(t, validateCreation(ScheduleCreationOptions{
		ScheduleID: "daily", OrchestrationName: "Backup", Interval: -time.Minute,
	}), ErrScheduleValidation)

	// The .NET TimeSpan wire format cannot express a negative interval, so the
	// options never reach the entity.
	_, err := json.Marshal(ScheduleCreationOptions{
		ScheduleID: "daily", OrchestrationName: "Backup", Interval: -time.Minute,
	})
	require.Error(t, err)
}

func TestScheduleEntityInvalidTransitionCarriesScheduleIdentity(t *testing.T) {
	tests := []struct {
		name      string
		status    ScheduleStatus
		operation string
		input     any
		useToken  bool
		to        ScheduleStatus
	}{
		{name: "update", status: ScheduleStatusUninitialized, operation: updateScheduleOperation,
			input: intervalUpdate(time.Minute), to: ScheduleStatusUninitialized},
		{name: "pause", status: ScheduleStatusPaused, operation: pauseScheduleOperation,
			to: ScheduleStatusPaused},
		{name: "resume", status: ScheduleStatusActive, operation: resumeScheduleOperation,
			to: ScheduleStatusActive},
		{name: "run", status: ScheduleStatusPaused, operation: runScheduleOperation, useToken: true,
			to: ScheduleStatusActive},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := newScheduleExecutor(t)
			state := seededScheduleState(test.status)
			input := test.input
			if test.useToken {
				input = state.ExecutionToken
			}
			result := runScheduleOperationBatch(t, executor, "daily", marshalScheduleState(state),
				test.operation, input)
			details := result.Results[0].GetFailure().GetFailureDetails()
			require.Equal(t, string(scheduleInvalidTransitionType), details.GetErrorType())
			require.Contains(t, details.GetErrorMessage(), `"daily"`)

			properties := decodeErrorProperties(t, details)
			require.Equal(t, "daily", properties["scheduleId"])
			require.EqualValues(t, int(test.status), properties["from"])
			require.EqualValues(t, int(test.to), properties["to"])
			require.Equal(t, test.operation, properties["operation"])
		})
	}
}

func TestScheduleEntityCreateAcceptsEqualStartAndEnd(t *testing.T) {
	executor := newScheduleExecutor(t)
	at := time.Now().UTC().Add(time.Hour)
	result := runScheduleOperationBatch(t, executor, "daily", nil, createScheduleOperation, ScheduleCreationOptions{
		ScheduleID: "daily", OrchestrationName: "Backup", Interval: time.Minute, StartAt: at, EndAt: at,
	})
	requireOperationSucceeded(t, result)
	state := decodeScheduleState(t, result.EntityState)
	require.True(t, state.ScheduleConfiguration.StartAt.Equal(at))
	require.True(t, state.ScheduleConfiguration.EndAt.Equal(at))
}

func TestScheduleEntityRecreatePreservesCreationTimeAndClearsEnd(t *testing.T) {
	executor := newScheduleExecutor(t)
	created := runScheduleOperationBatch(t, executor, "daily", nil, createScheduleOperation, ScheduleCreationOptions{
		ScheduleID: "daily", OrchestrationName: "Backup", Interval: 5 * time.Minute,
		EndAt: time.Now().UTC().Add(time.Hour),
	})
	requireOperationSucceeded(t, created)
	first := decodeScheduleState(t, created.EntityState)
	require.False(t, first.ScheduleConfiguration.EndAt.IsZero())

	recreated := runScheduleOperationBatch(t, executor, "daily", created.EntityState,
		createScheduleOperation, validCreationOptions())
	requireOperationSucceeded(t, recreated)
	second := decodeScheduleState(t, recreated.EntityState)
	require.True(t, second.ScheduleConfiguration.EndAt.IsZero(), "recreating must clear the end time")
	require.True(t, first.ScheduleCreatedAt.Equal(second.ScheduleCreatedAt))
	require.NotEqual(t, first.ExecutionToken, second.ExecutionToken)
	require.True(t, second.NextRunAt.IsZero())
}

func TestScheduleEntityRejectsUnknownOperation(t *testing.T) {
	executor := newScheduleExecutor(t)
	result := runScheduleOperationBatch(t, executor, "daily",
		marshalScheduleState(seededScheduleState(ScheduleStatusActive)), "NotAnOperation", nil)
	require.NotNil(t, result.Results[0].GetFailure())
	require.Contains(t, result.Results[0].GetFailure().GetFailureDetails().GetErrorMessage(), "NotAnOperation")
}

// ---------------------------------------------------------------------------
// Text handling
// ---------------------------------------------------------------------------

func TestScheduleEntityPreservesTextPayloads(t *testing.T) {
	tests := []struct {
		name       string
		scheduleID string
		input      string
	}{
		{name: "special characters", scheduleID: "test@schedule#123$%^", input: `plain`},
		{name: "unicode", scheduleID: "スケジュール-日程-🌟", input: `{"名前":"値","emoji":"🌟"}`},
		{name: "maximum length id", scheduleID: strings.Repeat("a", 1000), input: `"payload"`},
		{name: "json special characters", scheduleID: "json", input: `{"key":"value with \"quotes\" and \\backslash\\ and /slash/"}`},
		{name: "html special characters", scheduleID: "html", input: `<script>alert("xss")</script>&amp;<>"'`},
		{name: "multiline", scheduleID: "multiline", input: "line one\nline two\r\nline three\ttabbed"},
		{name: "base64", scheduleID: "base64", input: base64.StdEncoding.EncodeToString([]byte("scheduled task payload"))},
		{name: "large input", scheduleID: "large", input: strings.Repeat("x", 100000)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := newScheduleExecutor(t)
			created := runScheduleOperationBatch(t, executor, test.scheduleID, nil, createScheduleOperation,
				ScheduleCreationOptions{
					ScheduleID:         test.scheduleID,
					OrchestrationName:  "Backup",
					Interval:           5 * time.Minute,
					OrchestrationInput: test.input,
				})
			requireOperationSucceeded(t, created)
			createdState := decodeScheduleState(t, created.EntityState)
			require.Equal(t, test.scheduleID, createdState.ScheduleConfiguration.ScheduleID)
			require.Equal(t, test.input, createdState.ScheduleConfiguration.OrchestrationInput)

			updatedInput := test.input + "-updated"
			updated := runScheduleOperationBatch(t, executor, test.scheduleID, created.EntityState,
				updateScheduleOperation, ScheduleUpdateOptions{OrchestrationInput: pointerTo(updatedInput)})
			requireOperationSucceeded(t, updated)
			updatedState := decodeScheduleState(t, updated.EntityState)
			require.Equal(t, updatedInput, updatedState.ScheduleConfiguration.OrchestrationInput)
			require.NotEqual(t, createdState.ExecutionToken, updatedState.ExecutionToken)

			description, err := scheduleDescription(&api.EntityMetadata{
				InstanceID:      api.NewEntityID(ScheduleEntityName, test.scheduleID),
				StateIncluded:   true,
				HasState:        true,
				SerializedState: updated.EntityState.GetValue(),
			}, nil)
			require.NoError(t, err)
			require.Equal(t, test.scheduleID, description.ScheduleID)
			require.Equal(t, updatedInput, description.OrchestrationInput)
		})
	}
}

func TestScheduleEntityUpdatesLongInstanceID(t *testing.T) {
	executor := newScheduleExecutor(t)
	created := runScheduleOperationBatch(t, executor, "daily", nil, createScheduleOperation, validCreationOptions())
	requireOperationSucceeded(t, created)

	longInstanceID := strings.Repeat("i", 1000)
	updated := runScheduleOperationBatch(t, executor, "daily", created.EntityState, updateScheduleOperation,
		ScheduleUpdateOptions{OrchestrationInstanceID: pointerTo(longInstanceID)})
	requireOperationSucceeded(t, updated)
	require.Equal(t, longInstanceID,
		decodeScheduleState(t, updated.EntityState).ScheduleConfiguration.OrchestrationInstanceID)
}

// ---------------------------------------------------------------------------
// Operation orchestrator
// ---------------------------------------------------------------------------

func TestExecuteScheduleOperationOrchestratorCallsEntity(t *testing.T) {
	tests := []struct {
		name  string
		input any
	}{
		{name: "with input", input: ScheduleUpdateOptions{Interval: pointerTo(10 * time.Minute)}},
		{name: "without input", input: nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry := newScheduleRegistry(t)
			request := ScheduleOperationRequest{
				EntityID:      scheduleEntityID("test-schedule"),
				OperationName: updateScheduleOperation,
				Input:         test.input,
			}
			payload, err := api.DefaultDataConverter().Serialize(request)
			require.NoError(t, err)

			response := executeScheduleOperationTurn(t, registry, nil, []*protos.HistoryEvent{
				helpers.NewOrchestratorStartedEvent(),
				helpers.NewExecutionStartedEvent(ExecuteScheduleOperationOrchestratorName, "op", wrapperspb.String(payload), nil, nil, nil),
			})
			require.Len(t, response.Actions, 1)
			called := response.Actions[0].GetSendEntityMessage().GetEntityOperationCalled()
			require.NotNil(t, called)
			require.Equal(t, scheduleEntityID("test-schedule").String(), called.TargetInstanceId.GetValue())
			require.Equal(t, updateScheduleOperation, called.Operation)
			if test.input == nil {
				require.Empty(t, called.Input.GetValue())
			} else {
				require.Contains(t, called.Input.GetValue(), "00:10:00")
			}
		})
	}
}

func TestExecuteScheduleOperationOrchestratorPropagatesResultAndFailure(t *testing.T) {
	registry := newScheduleRegistry(t)
	payload, err := api.DefaultDataConverter().Serialize(ScheduleOperationRequest{
		EntityID:      scheduleEntityID("test-schedule"),
		OperationName: pauseScheduleOperation,
	})
	require.NoError(t, err)

	started := helpers.NewOrchestratorStartedEvent()
	executionStarted := helpers.NewExecutionStartedEvent(
		ExecuteScheduleOperationOrchestratorName, "op", wrapperspb.String(payload), nil, nil, nil)
	first := executeScheduleOperationTurn(t, registry, nil, []*protos.HistoryEvent{started, executionStarted})
	called := first.Actions[0].GetSendEntityMessage().GetEntityOperationCalled()
	require.NotNil(t, called)

	history := []*protos.HistoryEvent{started, executionStarted, {
		EventId:   first.Actions[0].Id,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_EntityOperationCalled{EntityOperationCalled: called},
	}}

	t.Run("result is propagated", func(t *testing.T) {
		completed := &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: timestamppb.Now(),
			EventType: &protos.HistoryEvent_EntityOperationCompleted{
				EntityOperationCompleted: &protos.EntityOperationCompletedEvent{
					RequestId: called.RequestId,
					Output:    wrapperspb.String(`{"Status":2}`),
				},
			},
		}
		response := executeScheduleOperationTurn(t, registry, history,
			[]*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent(), completed})
		completion := scheduleCompletionAction(t, response)
		require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, completion.OrchestrationStatus)
		require.JSONEq(t, `{"Status":2}`, completion.Result.GetValue())
	})

	t.Run("failure is propagated", func(t *testing.T) {
		failed := &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: timestamppb.Now(),
			EventType: &protos.HistoryEvent_EntityOperationFailed{
				EntityOperationFailed: &protos.EntityOperationFailedEvent{
					RequestId: called.RequestId,
					FailureDetails: &protos.TaskFailureDetails{
						ErrorType:    string(scheduleInvalidTransitionType),
						ErrorMessage: "cannot pause a paused schedule",
					},
				},
			},
		}
		response := executeScheduleOperationTurn(t, registry, history,
			[]*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent(), failed})
		completion := scheduleCompletionAction(t, response)
		require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, completion.OrchestrationStatus)
		require.Contains(t, completion.FailureDetails.GetErrorMessage(), "cannot pause a paused schedule")
	})
}

func TestExecuteScheduleOperationOrchestratorRequiresOperationName(t *testing.T) {
	registry := newScheduleRegistry(t)
	payload, err := api.DefaultDataConverter().Serialize(ScheduleOperationRequest{
		EntityID: scheduleEntityID("test-schedule"),
	})
	require.NoError(t, err)
	response := executeScheduleOperationTurn(t, registry, nil, []*protos.HistoryEvent{
		helpers.NewOrchestratorStartedEvent(),
		helpers.NewExecutionStartedEvent(ExecuteScheduleOperationOrchestratorName, "op", wrapperspb.String(payload), nil, nil, nil),
	})
	completion := scheduleCompletionAction(t, response)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, completion.OrchestrationStatus)
	require.Equal(t, string(scheduleValidationErrorType), completion.FailureDetails.GetErrorType())
}

// ---------------------------------------------------------------------------
// Client behavior
// ---------------------------------------------------------------------------

func TestScheduleClientOperationsTargetTheEntity(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		invoke    func(*ScheduleClient) error
	}{
		{name: "create", operation: createScheduleOperation, invoke: func(c *ScheduleClient) error {
			return c.Create(context.Background(), ScheduleCreationOptions{
				ScheduleID: "daily", OrchestrationName: "Backup", Interval: time.Minute,
			})
		}},
		{name: "update", operation: updateScheduleOperation, invoke: func(c *ScheduleClient) error {
			return c.Update(context.Background(), ScheduleUpdateOptions{Interval: pointerTo(time.Minute)})
		}},
		{name: "pause", operation: pauseScheduleOperation, invoke: func(c *ScheduleClient) error {
			return c.Pause(context.Background())
		}},
		{name: "resume", operation: resumeScheduleOperation, invoke: func(c *ScheduleClient) error {
			return c.Resume(context.Background())
		}},
		{name: "delete", operation: deleteScheduleOperation, invoke: func(c *ScheduleClient) error {
			return c.Delete(context.Background())
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := &recordingScheduleBackend{}
			handle, err := (&ScheduledTaskClient{client: backend}).GetScheduleClient("daily")
			require.NoError(t, err)
			require.NoError(t, test.invoke(handle))

			require.Equal(t, ExecuteScheduleOperationOrchestratorName, backend.orchestrator)
			require.NotNil(t, backend.version)
			require.Empty(t, backend.version.GetValue())
			require.Equal(t, test.operation, backend.request.OperationName)
			require.Equal(t, scheduleEntityID("daily"), backend.request.EntityID)
			require.Equal(t, 1, backend.waitCount)
		})
	}
}

func TestScheduleClientFailureMatrix(t *testing.T) {
	tests := []struct {
		name      string
		metadata  *api.OrchestrationMetadata
		wantIs    error
		assertion func(*testing.T, error)
	}{
		{
			name: "invalid transition",
			metadata: &api.OrchestrationMetadata{
				RuntimeStatus: api.RUNTIME_STATUS_FAILED,
				FailureDetails: &api.FailureDetails{
					ErrorType: scheduleInvalidTransitionType,
					Properties: map[string]any{
						"scheduleId": "daily",
						"from":       float64(ScheduleStatusPaused),
						"to":         float64(ScheduleStatusPaused),
						"operation":  pauseScheduleOperation,
					},
				},
			},
			wantIs: ErrScheduleInvalidTransition,
			assertion: func(t *testing.T, err error) {
				var transition *ScheduleInvalidTransitionError
				require.ErrorAs(t, err, &transition)
				require.Equal(t, "daily", transition.ScheduleID)
				require.Equal(t, ScheduleStatusPaused, transition.From)
			},
		},
		{
			name: "namespaced invalid transition",
			metadata: &api.OrchestrationMetadata{
				RuntimeStatus: api.RUNTIME_STATUS_FAILED,
				FailureDetails: &api.FailureDetails{
					ErrorType: api.ErrorType("Microsoft.DurableTask.ScheduledTasks." + string(scheduleInvalidTransitionType)),
				},
			},
			wantIs: ErrScheduleInvalidTransition,
		},
		{
			name: "nested validation failure",
			metadata: &api.OrchestrationMetadata{
				RuntimeStatus: api.RUNTIME_STATUS_FAILED,
				FailureDetails: &api.FailureDetails{
					ErrorType: api.ErrorTypeEntityOperationFailed,
					InnerFailure: &api.FailureDetails{
						ErrorType:  scheduleValidationErrorType,
						Properties: map[string]any{"message": "interval must be positive"},
					},
				},
			},
			wantIs: ErrScheduleValidation,
			assertion: func(t *testing.T, err error) {
				var validation *ScheduleValidationError
				require.ErrorAs(t, err, &validation)
				require.Equal(t, "interval must be positive", validation.Message)
			},
		},
		{
			name: "generic failure",
			metadata: &api.OrchestrationMetadata{
				RuntimeStatus:  api.RUNTIME_STATUS_FAILED,
				FailureDetails: &api.FailureDetails{ErrorMessage: "boom"},
			},
			wantIs: ErrScheduleOperationFailed,
		},
		{
			name:     "terminated without failure details",
			metadata: &api.OrchestrationMetadata{RuntimeStatus: api.RUNTIME_STATUS_TERMINATED},
			wantIs:   ErrScheduleOperationFailed,
			assertion: func(t *testing.T, err error) {
				var operation *ScheduleOperationError
				require.ErrorAs(t, err, &operation)
				require.Equal(t, api.RUNTIME_STATUS_TERMINATED, operation.RuntimeStatus)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := &recordingScheduleBackend{completion: test.metadata}
			handle, err := (&ScheduledTaskClient{client: backend}).GetScheduleClient("daily")
			require.NoError(t, err)
			for _, operate := range []func() error{
				func() error { return handle.Pause(context.Background()) },
				func() error { return handle.Resume(context.Background()) },
				func() error { return handle.Delete(context.Background()) },
				func() error { return handle.Update(context.Background(), ScheduleUpdateOptions{}) },
			} {
				err := operate()
				require.ErrorIs(t, err, test.wantIs)
				if test.assertion != nil {
					test.assertion(t, err)
				}
			}
		})
	}
}

func TestScheduleClientNotFoundMatrix(t *testing.T) {
	tests := []struct {
		name     string
		backend  *recordingScheduleBackend
		wantErr  bool
		wantNil  bool
		wantDesc bool
	}{
		{name: "nil metadata", backend: &recordingScheduleBackend{}, wantErr: true, wantNil: true},
		{
			name:    "empty serialized state",
			backend: &recordingScheduleBackend{entity: &api.EntityMetadata{InstanceID: scheduleEntityID("daily")}},
			wantErr: true, wantNil: true,
		},
		{
			name: "present schedule",
			backend: &recordingScheduleBackend{entity: &api.EntityMetadata{
				InstanceID:      scheduleEntityID("daily"),
				StateIncluded:   true,
				HasState:        true,
				SerializedState: marshalScheduleState(seededScheduleState(ScheduleStatusActive)).GetValue(),
			}},
			wantDesc: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &ScheduledTaskClient{client: test.backend}
			handle, err := client.GetScheduleClient("daily")
			require.NoError(t, err)

			description, err := handle.Describe(context.Background())
			if test.wantErr {
				require.ErrorIs(t, err, ErrScheduleNotFound)
				var notFound *ScheduleNotFoundError
				require.ErrorAs(t, err, &notFound)
				require.Equal(t, "daily", notFound.ScheduleID)
				require.Nil(t, description)
			} else {
				require.NoError(t, err)
				require.NotNil(t, description)
			}

			got, err := client.Get(context.Background(), "daily")
			require.NoError(t, err)
			if test.wantNil {
				require.Nil(t, got)
			}
			if test.wantDesc {
				require.NotNil(t, got)
				require.Equal(t, "daily", got.ScheduleID)
				require.Equal(t, ScheduleStatusActive, got.Status)
				require.Equal(t, "Backup", got.OrchestrationName)
				require.Equal(t, 5*time.Minute, got.Interval)
			}
		})
	}
}

func TestScheduleClientValidationMatrix(t *testing.T) {
	client := &ScheduledTaskClient{client: &recordingScheduleBackend{}}

	_, err := client.GetScheduleClient("")
	require.ErrorIs(t, err, ErrScheduleValidation)

	_, err = client.Create(context.Background(), ScheduleCreationOptions{OrchestrationName: "Backup"})
	require.ErrorIs(t, err, ErrScheduleValidation)

	handle, err := client.GetScheduleClient("daily")
	require.NoError(t, err)
	require.ErrorIs(t,
		handle.Create(context.Background(), ScheduleCreationOptions{ScheduleID: "other", OrchestrationName: "Backup"}),
		ErrScheduleValidation)

	var nilTasks *ScheduledTaskClient
	_, err = nilTasks.GetScheduleClient("daily")
	require.Error(t, err)
	_, err = nilTasks.List(context.Background(), ScheduleQuery{})
	require.Error(t, err)

	var nilHandle *ScheduleClient
	require.Empty(t, nilHandle.ID())
	require.Error(t, nilHandle.Pause(context.Background()))
	_, err = nilHandle.Describe(context.Background())
	require.Error(t, err)

	require.Nil(t, NewScheduledTaskClient(nil))
	var nilClient *Client
	require.Nil(t, nilClient.ScheduledTasks())
}

func TestScheduleClientCreateReturnsHandle(t *testing.T) {
	backend := &recordingScheduleBackend{}
	client := &ScheduledTaskClient{client: backend, converter: api.DefaultDataConverter()}
	handle, err := client.Create(context.Background(), ScheduleCreationOptions{
		ScheduleID: "daily", OrchestrationName: "Backup", Interval: 5 * time.Minute,
	})
	require.NoError(t, err)
	require.Equal(t, "daily", handle.ID())
	require.Equal(t, createScheduleOperation, backend.request.OperationName)
}

func TestScheduleClientSerializesTypedInputWithConverter(t *testing.T) {
	backend := &recordingScheduleBackend{}
	client := &ScheduledTaskClient{client: backend, converter: prefixedConverter{}}
	handle, err := client.GetScheduleClient("daily")
	require.NoError(t, err)

	require.NoError(t, handle.Create(context.Background(), ScheduleCreationOptions{
		ScheduleID: "daily", OrchestrationName: "Backup", Interval: time.Minute,
		TypedOrchestrationInput: map[string]string{"database": "main"},
	}))
	creation, ok := backend.request.Input.(ScheduleCreationOptions)
	require.True(t, ok)
	require.Equal(t, `converter:{"database":"main"}`, creation.OrchestrationInput)
	require.Nil(t, creation.TypedOrchestrationInput)

	require.NoError(t, handle.Update(context.Background(), ScheduleUpdateOptions{
		TypedOrchestrationInput: map[string]string{"database": "replica"},
	}))
	update, ok := backend.request.Input.(ScheduleUpdateOptions)
	require.True(t, ok)
	require.NotNil(t, update.OrchestrationInput)
	require.Equal(t, `converter:{"database":"replica"}`, *update.OrchestrationInput)
	require.Nil(t, update.TypedOrchestrationInput)
}

func TestScheduleClientListFiltersAndValidates(t *testing.T) {
	created := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	entities := []*api.EntityMetadata{
		listedSchedule(t, "alpha", ScheduleStatusActive, created),
		listedSchedule(t, "beta", ScheduleStatusPaused, created.Add(time.Hour)),
		{InstanceID: scheduleEntityID("gamma")},
	}
	active := ScheduleStatusActive

	tests := []struct {
		name    string
		query   ScheduleQuery
		want    []string
		wantErr bool
	}{
		{name: "no filter skips stateless entities", query: ScheduleQuery{}, want: []string{"alpha", "beta"}},
		{name: "status filter", query: ScheduleQuery{Status: &active}, want: []string{"alpha"}},
		{name: "created from is exclusive", query: ScheduleQuery{CreatedFrom: created}, want: []string{"beta"}},
		{name: "created to is exclusive", query: ScheduleQuery{CreatedTo: created.Add(time.Hour)}, want: []string{"alpha"}},
		{name: "negative page size", query: ScheduleQuery{PageSize: -1}, wantErr: true},
		{name: "oversized page size", query: ScheduleQuery{PageSize: api.MaxInstanceQueryPageSize + 1}, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := &recordingScheduleBackend{entities: entities}
			client := &ScheduledTaskClient{client: backend}
			result, err := client.List(context.Background(), test.query)
			if test.wantErr {
				require.ErrorIs(t, err, ErrScheduleValidation)
				return
			}
			require.NoError(t, err)
			ids := make([]string, 0, len(result.Schedules))
			for _, schedule := range result.Schedules {
				ids = append(ids, schedule.ScheduleID)
			}
			require.Equal(t, test.want, ids)
			require.Equal(t, "@schedule@"+test.query.ScheduleIDPrefix, backend.query.InstanceIDStartsWith)
			require.False(t, backend.query.ExcludeState)
		})
	}
}

func TestScheduleClientListRequiresResult(t *testing.T) {
	client := &ScheduledTaskClient{client: &recordingScheduleBackend{nilQueryResult: true}}
	_, err := client.List(context.Background(), ScheduleQuery{})
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newScheduleRegistry(t *testing.T) *task.TaskRegistry {
	t.Helper()
	registry := task.NewTaskRegistry()
	require.NoError(t, RegisterScheduledTasks(registry))
	return registry
}

func newScheduleExecutor(t *testing.T) entityExecutor {
	t.Helper()
	return task.NewTaskExecutor(newScheduleRegistry(t)).(entityExecutor)
}

func runScheduleOperationBatch(
	t *testing.T,
	executor entityExecutor,
	scheduleID string,
	state *wrapperspb.StringValue,
	operation string,
	input any,
) *protos.EntityBatchResult {
	t.Helper()
	request := &protos.EntityBatchRequest{
		InstanceId:  scheduleEntityID(scheduleID).String(),
		EntityState: state,
		Operations:  []*protos.OperationRequest{{Operation: operation}},
	}
	if input != nil {
		payload, err := api.DefaultDataConverter().Serialize(input)
		require.NoError(t, err)
		request.Operations[0].Input = wrapperspb.String(payload)
	}
	result, err := executor.ExecuteEntity(context.Background(), request)
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	return result
}

func requireOperationSucceeded(t *testing.T, result *protos.EntityBatchResult) {
	t.Helper()
	if failure := result.Results[0].GetFailure(); failure != nil {
		t.Fatalf("expected success, got %s: %s",
			failure.GetFailureDetails().GetErrorType(), failure.GetFailureDetails().GetErrorMessage())
	}
}

func requireOperationErrorType(t *testing.T, result *protos.EntityBatchResult, errorType api.ErrorType) {
	t.Helper()
	failure := result.Results[0].GetFailure()
	require.NotNil(t, failure, "expected the operation to fail")
	require.Equal(t, string(errorType), failure.GetFailureDetails().GetErrorType())
}

func decodeErrorProperties(t *testing.T, details *protos.TaskFailureDetails) map[string]any {
	t.Helper()
	require.NotNil(t, details)
	properties := make(map[string]any, len(details.GetProperties()))
	for key, value := range details.GetProperties() {
		properties[key] = value.AsInterface()
	}
	return properties
}

func decodeScheduleState(t *testing.T, state *wrapperspb.StringValue) scheduleState {
	t.Helper()
	require.NotNil(t, state)
	var decoded scheduleState
	require.NoError(t, json.Unmarshal([]byte(state.GetValue()), &decoded))
	return decoded
}

// marshalScheduleState encodes state the way the service persists it. Encoding
// cannot fail for a well-formed scheduleState, so it panics rather than taking
// a *testing.T that most call sites would only forward.
func marshalScheduleState(state scheduleState) *wrapperspb.StringValue {
	payload, err := json.Marshal(state)
	if err != nil {
		panic(err)
	}
	return wrapperspb.String(string(payload))
}

func decodeSignalToken(t *testing.T, signal *protos.SendSignalAction) string {
	t.Helper()
	require.NotNil(t, signal)
	var token string
	require.NoError(t, api.DefaultDataConverter().Deserialize(signal.GetInput().GetValue(), &token))
	return token
}

func seededScheduleState(status ScheduleStatus) scheduleState {
	created := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	return scheduleState{
		Status:                 status,
		ExecutionToken:         "0123456789abcdef0123456789abcdee",
		ScheduleCreatedAt:      created,
		ScheduleLastModifiedAt: created,
		ScheduleConfiguration: &scheduleConfiguration{
			ScheduleID:        "daily",
			OrchestrationName: "Backup",
			Interval:          dotNetSpan(5 * time.Minute),
		},
	}
}

func validCreationOptions() ScheduleCreationOptions {
	return ScheduleCreationOptions{
		ScheduleID:        "daily",
		OrchestrationName: "Backup",
		Interval:          5 * time.Minute,
	}
}

func intervalUpdate(interval time.Duration) ScheduleUpdateOptions {
	return ScheduleUpdateOptions{Interval: pointerTo(interval)}
}

func listedSchedule(t *testing.T, id string, status ScheduleStatus, created time.Time) *api.EntityMetadata {
	t.Helper()
	state := seededScheduleState(status)
	state.ScheduleCreatedAt = created
	state.ScheduleConfiguration.ScheduleID = id
	return &api.EntityMetadata{
		InstanceID:      scheduleEntityID(id),
		StateIncluded:   true,
		HasState:        true,
		SerializedState: marshalScheduleState(state).GetValue(),
	}
}

func executeScheduleOperationTurn(
	t *testing.T,
	registry *task.TaskRegistry,
	oldEvents []*protos.HistoryEvent,
	newEvents []*protos.HistoryEvent,
) *protos.OrchestratorResponse {
	t.Helper()
	result, err := task.NewTaskExecutor(registry).ExecuteOrchestrator(
		context.Background(),
		api.InstanceID("op"),
		oldEvents,
		newEvents,
		&protos.OrchestratorEntityParameters{EntityMessageReorderWindow: durationpb.New(0)},
	)

	require.NoError(t, err)
	return result.Response
}

func scheduleCompletionAction(t *testing.T, response *protos.OrchestratorResponse) *protos.CompleteOrchestrationAction {
	t.Helper()
	for _, action := range response.Actions {
		if completion := action.GetCompleteOrchestration(); completion != nil {
			return completion
		}
	}
	t.Fatal("expected the orchestration to complete")
	return nil
}

func pointerTo[T any](value T) *T { return &value }

// recordingScheduleBackend captures the requests issued by the schedule clients.
type recordingScheduleBackend struct {
	orchestrator   string
	version        *wrapperspb.StringValue
	request        ScheduleOperationRequest
	waitCount      int
	completion     *api.OrchestrationMetadata
	entity         *api.EntityMetadata
	fetchErr       error
	entities       []*api.EntityMetadata
	query          api.EntityQuery
	nilQueryResult bool
}

func (b *recordingScheduleBackend) ScheduleNewOrchestration(
	_ context.Context,
	orchestrator string,
	options ...api.NewOrchestrationOptions,
) (api.InstanceID, error) {
	request := &protos.CreateInstanceRequest{}
	for _, option := range options {
		if err := option(request, api.DefaultDataConverter()); err != nil {
			return "", err
		}
	}
	b.orchestrator = orchestrator
	b.version = request.Version
	if err := json.Unmarshal([]byte(request.Input.GetValue()), &b.request); err != nil {
		return "", err
	}
	// Re-decode the operation input into its concrete type so tests can assert
	// on converter-produced payloads.
	var envelope struct{ Input json.RawMessage }
	if err := json.Unmarshal([]byte(request.Input.GetValue()), &envelope); err == nil && len(envelope.Input) > 0 {
		switch b.request.OperationName {
		case createScheduleOperation:
			var options ScheduleCreationOptions
			if err := json.Unmarshal(envelope.Input, &options); err == nil {
				b.request.Input = options
			}
		case updateScheduleOperation:
			var options ScheduleUpdateOptions
			if err := json.Unmarshal(envelope.Input, &options); err == nil {
				b.request.Input = options
			}
		}
	}
	return "operation", nil
}

func (b *recordingScheduleBackend) WaitForOrchestrationCompletion(
	context.Context,
	api.InstanceID,
	...api.FetchOrchestrationMetadataOptions,
) (*api.OrchestrationMetadata, error) {
	b.waitCount++
	if b.completion != nil {
		return b.completion, nil
	}
	return &api.OrchestrationMetadata{RuntimeStatus: api.RUNTIME_STATUS_COMPLETED}, nil
}

func (b *recordingScheduleBackend) GetEntity(
	context.Context,
	api.EntityID,
	...api.GetEntityOptions,
) (*api.EntityMetadata, error) {
	return b.entity, b.fetchErr
}

func (b *recordingScheduleBackend) QueryEntities(
	_ context.Context,
	query api.EntityQuery,
) (*api.EntityQueryResults, error) {
	b.query = query
	if b.nilQueryResult {
		return nil, nil
	}
	return &api.EntityQueryResults{Entities: b.entities}, nil
}

var _ scheduledTaskBackend = (*recordingScheduleBackend)(nil)
