package task

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestRetryOptionsCloneAndNormalizeCallerPolicy(t *testing.T) {
	caller := &RetryPolicy{InitialRetryInterval: time.Second}
	activityOption := WithActivityRetryPolicy(caller)
	subOrchestrationOption := WithSubOrchestrationRetryPolicy(caller)

	// Mutation after option construction must not affect either captured policy.
	caller.InitialRetryInterval = time.Hour
	caller.MaxAttempts = 99
	activity := new(callActivityOptions)
	if err := activityOption(activity, api.DefaultDataConverter()); err != nil {
		t.Fatal(err)
	}
	subOrchestration := new(callSubOrchestratorOptions)
	if err := subOrchestrationOption(subOrchestration, api.DefaultDataConverter()); err != nil {
		t.Fatal(err)
	}
	for name, policy := range map[string]*RetryPolicy{
		"activity":          activity.retryPolicy,
		"sub-orchestration": subOrchestration.retryPolicy,
	} {
		if policy == caller {
			t.Fatalf("%s option retained caller-owned policy", name)
		}
		if policy.InitialRetryInterval != time.Second || policy.MaxAttempts != 1 ||
			policy.BackoffCoefficient != 1 || policy.MaxRetryInterval != math.MaxInt64 ||
			policy.RetryTimeout != math.MaxInt64 || policy.Handle == nil {
			t.Fatalf("%s policy was not independently normalized: %+v", name, policy)
		}
	}
	if activity.retryPolicy == subOrchestration.retryPolicy {
		t.Fatal("activity and sub-orchestration options share a mutable policy copy")
	}
}

func TestRetryPolicyValidateDoesNotMutateReceiver(t *testing.T) {
	policy := &RetryPolicy{InitialRetryInterval: time.Second}
	if err := policy.Validate(); err != nil {
		t.Fatal(err)
	}
	if policy.MaxAttempts != 0 || policy.BackoffCoefficient != 0 ||
		policy.MaxRetryInterval != 0 || policy.RetryTimeout != 0 || policy.Handle != nil {
		t.Fatalf("Validate mutated its receiver: %+v", policy)
	}
}

func TestComputeNextDelayDoesNotCrossRetryDeadline(t *testing.T) {
	firstAttempt := time.Unix(0, 0).UTC()
	failure := &TaskFailedError{
		TaskName:       "activity",
		FailureDetails: &api.FailureDetails{ErrorType: "TestError", ErrorMessage: "failed"},
	}
	policy := RetryPolicy{
		MaxAttempts:          3,
		InitialRetryInterval: 20 * time.Second,
		BackoffCoefficient:   1,
		MaxRetryInterval:     time.Minute,
		RetryTimeout:         time.Minute,
		Handle:               func(RetryContext) bool { return true },
	}

	if delay := computeNextDelay(firstAttempt.Add(50*time.Second), policy, 0, firstAttempt, failure); delay != 0 {
		t.Fatalf("delay %v crosses retry deadline", delay)
	}
	policy.InitialRetryInterval = 10 * time.Second
	if delay := computeNextDelay(firstAttempt.Add(50*time.Second), policy, 0, firstAttempt, failure); delay != 10*time.Second {
		t.Fatalf("delay at retry deadline = %v, want 10s", delay)
	}
	if delay := computeNextDelay(firstAttempt.Add(time.Minute), policy, 0, firstAttempt, failure); delay != 0 {
		t.Fatalf("delay at expired retry deadline = %v, want 0", delay)
	}
}

func TestRetryOptionDoesNotReadCallerPolicyAfterConstruction(t *testing.T) {
	caller := &RetryPolicy{InitialRetryInterval: time.Second, MaxAttempts: 3}
	option := WithActivityRetryPolicy(caller)

	var writers sync.WaitGroup
	writers.Add(1)
	go func() {
		defer writers.Done()
		for i := 1; i <= 10_000; i++ {
			caller.InitialRetryInterval = time.Duration(i) * time.Millisecond
			caller.MaxAttempts = i
		}
	}()
	for range 1_000 {
		configured := new(callActivityOptions)
		if err := option(configured, api.DefaultDataConverter()); err != nil {
			t.Fatal(err)
		}
		if configured.retryPolicy.InitialRetryInterval != time.Second || configured.retryPolicy.MaxAttempts != 3 {
			t.Fatalf("captured policy drifted: %+v", configured.retryPolicy)
		}
	}
	writers.Wait()
}

// TestRetryDoesNotScheduleTimerPastRetryTimeout proves the retry loop stops
// instead of creating a durable timer whose delay would carry the next attempt
// past RetryTimeout. Bounding the timer at creation keeps the decision on the
// failure event, where every input is replayed from history.
func TestRetryDoesNotScheduleTimerPastRetryTimeout(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("bounded-retry", func(ctx *OrchestrationContext) (any, error) {
		return nil, ctx.CallActivity("flaky", WithActivityRetryPolicy(&RetryPolicy{
			MaxAttempts:          3,
			InitialRetryInterval: 10 * time.Second,
			RetryTimeout:         time.Minute,
		})).Await(nil)
	}); err != nil {
		t.Fatal(err)
	}

	firstAttempt := time.Unix(1_700_000_000, 0).UTC()
	startedTurn := helpers.NewOrchestratorStartedEvent()
	startedTurn.Timestamp = timestamppb.New(firstAttempt)
	// Only five seconds of the retry budget remain, so the ten-second backoff
	// would fire after the deadline.
	failureTurn := helpers.NewOrchestratorStartedEvent()
	failureTurn.Timestamp = timestamppb.New(firstAttempt.Add(55 * time.Second))
	instanceID := api.InstanceID("bounded-retry-instance")
	result, err := NewTaskExecutor(registry).ExecuteOrchestrator(
		context.Background(),
		instanceID,
		[]*protos.HistoryEvent{
			startedTurn,
			helpers.NewExecutionStartedEvent("bounded-retry", string(instanceID), nil, nil, nil, nil),
			helpers.NewTaskScheduledEvent(0, "flaky", nil, nil, nil),
		},
		[]*protos.HistoryEvent{
			failureTurn,
			helpers.NewTaskFailedEvent(0, &protos.TaskFailureDetails{
				ErrorType:    "TransientFailure",
				ErrorMessage: "retry me",
			}),
		}, nil)

	if err != nil {
		t.Fatal(err)
	}
	for _, action := range result.Response.Actions {
		if action.GetCreateTimer() != nil {
			t.Fatal("retry timer scheduled past RetryTimeout")
		}
		if scheduled := action.GetScheduleTask(); scheduled != nil {
			t.Fatalf("retry scheduled activity %q past RetryTimeout", scheduled.GetName())
		}
	}
	if completed := completionAction(t, result.Response); completed.GetOrchestrationStatus() !=
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED {
		t.Fatalf("orchestration status = %v, want FAILED", completed.GetOrchestrationStatus())
	}
}

// TestRetryDecisionIsStableWhenCompletionIsRedelivered pins the replay contract
// the retry loop depends on: when a completion response is lost and DTS
// redelivers the work item, the same events must produce the same retry
// actions even though they arrive as replayed history.
func TestRetryDecisionIsStableWhenCompletionIsRedelivered(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("stable-retry", func(ctx *OrchestrationContext) (any, error) {
		return nil, ctx.CallActivity("flaky", WithActivityRetryPolicy(&RetryPolicy{
			MaxAttempts:          3,
			InitialRetryInterval: 10 * time.Second,
			RetryTimeout:         time.Minute,
		})).Await(nil)
	}); err != nil {
		t.Fatal(err)
	}

	firstAttempt := time.Unix(1_700_000_000, 0).UTC()
	failureTurn := helpers.NewOrchestratorStartedEvent()
	failureTurn.Timestamp = timestamppb.New(firstAttempt.Add(5 * time.Second))
	startedTurn := helpers.NewOrchestratorStartedEvent()
	startedTurn.Timestamp = timestamppb.New(firstAttempt)
	committed := []*protos.HistoryEvent{
		startedTurn,
		helpers.NewExecutionStartedEvent("stable-retry", string(instanceIDStableRetry), nil, nil, nil, nil),
		helpers.NewTaskScheduledEvent(0, "flaky", nil, nil, nil),
	}
	delivered := []*protos.HistoryEvent{
		failureTurn,
		helpers.NewTaskFailedEvent(0, &protos.TaskFailureDetails{
			ErrorType:    "TransientFailure",
			ErrorMessage: "retry me",
		}),
	}
	executor := NewTaskExecutor(registry)

	// First delivery: the failure arrives as a new event.
	first, err := executor.ExecuteOrchestrator(
		context.Background(), instanceIDStableRetry, committed, delivered, nil)

	if err != nil {
		t.Fatal(err)
	}
	// Redelivery after a lost response: the identical failure is now replayed
	// history, so IsReplaying flips while the decision must not.
	second, err := executor.ExecuteOrchestrator(
		context.Background(), instanceIDStableRetry, append(committed, delivered...), nil, nil)

	if err != nil {
		t.Fatal(err)
	}

	firstTimer := singleRetryTimer(t, first.Response)
	secondTimer := singleRetryTimer(t, second.Response)
	if !firstTimer.GetFireAt().AsTime().Equal(secondTimer.GetFireAt().AsTime()) {
		t.Fatalf("retry timer moved across redelivery: %v then %v",
			firstTimer.GetFireAt().AsTime(), secondTimer.GetFireAt().AsTime())
	}
}

const instanceIDStableRetry = api.InstanceID("stable-retry-instance")

func singleRetryTimer(t *testing.T, response *protos.OrchestratorResponse) *protos.CreateTimerAction {
	t.Helper()
	var timer *protos.CreateTimerAction
	for _, action := range response.GetActions() {
		if created := action.GetCreateTimer(); created != nil {
			if timer != nil {
				t.Fatal("more than one retry timer action")
			}
			timer = created
		}
	}
	if timer == nil {
		t.Fatalf("no retry timer action in %v", response.GetActions())
	}
	return timer
}

func TestComputeNextDelayPreservesPositiveDurations(t *testing.T) {
	for _, test := range []struct {
		name        string
		initial     time.Duration
		coefficient float64
		attempt     int
		cap         time.Duration
		timeout     time.Duration
		elapsed     time.Duration
		want        time.Duration
	}{
		{name: "submillisecond initial", initial: 500 * time.Microsecond, want: 500 * time.Microsecond},
		{name: "fractional backoff", initial: 1500 * time.Microsecond, coefficient: .5, attempt: 1, want: 750 * time.Microsecond},
		{name: "subnanosecond backoff", initial: time.Nanosecond, coefficient: .5, attempt: 1, want: time.Nanosecond},
		{name: "floating point underflow", initial: time.Second, coefficient: .01, attempt: 1000, want: time.Nanosecond},
		{name: "unscaled precision", initial: math.MaxInt64 - 31, coefficient: 1, attempt: 8, want: math.MaxInt64 - 31},
		{name: "duration overflow", initial: time.Second, coefficient: math.MaxFloat64, attempt: 2, want: math.MaxInt64},
		{name: "overflow capped", initial: time.Second, coefficient: math.MaxFloat64, attempt: 2, cap: 1234 * time.Microsecond, want: 1234 * time.Microsecond},
		{name: "submillisecond cap", initial: time.Millisecond, coefficient: 2, attempt: 3, cap: 500 * time.Microsecond, want: 500 * time.Microsecond},
		{name: "exact deadline", initial: 500 * time.Microsecond, timeout: time.Millisecond, elapsed: 500 * time.Microsecond, want: 500 * time.Microsecond},
		{name: "past deadline", initial: 500 * time.Microsecond, timeout: time.Millisecond, elapsed: 500*time.Microsecond + time.Nanosecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			policy, err := (&RetryPolicy{
				MaxAttempts:          3,
				InitialRetryInterval: test.initial,
				BackoffCoefficient:   test.coefficient,
				MaxRetryInterval:     test.cap,
				RetryTimeout:         test.timeout,
			}).Normalized()
			require.NoError(t, err)
			first := time.Unix(1_700_000_000, 0).UTC()
			failure := &TaskFailedError{FailureDetails: &api.FailureDetails{ErrorType: "Transient", ErrorMessage: "retry"}}
			require.Equal(t, test.want, computeNextDelay(first.Add(test.elapsed), policy, test.attempt, first, failure))
		})
	}
}

func TestRetryPreservesSubmillisecondTimersAcrossReplay(t *testing.T) {
	for _, kind := range []string{"activity", "sub-orchestration"} {
		for _, test := range []struct {
			name        string
			initial     time.Duration
			coefficient float64
			cap         time.Duration
			delays      []time.Duration
		}{
			{"decreasing", 500 * time.Microsecond, .5, 0, []time.Duration{500 * time.Microsecond, 250 * time.Microsecond}},
			{"increasing", 500 * time.Microsecond, 1.5, 0, []time.Duration{500 * time.Microsecond, 750 * time.Microsecond}},
			{"capped", 1500 * time.Microsecond, .5, 750 * time.Microsecond, []time.Duration{750 * time.Microsecond, 750 * time.Microsecond}},
			{"minimum", 500 * time.Microsecond, 1e-200, 0, []time.Duration{500 * time.Microsecond, time.Nanosecond}},
		} {
			t.Run(kind+"/"+test.name, func(t *testing.T) {
				registry := NewTaskRegistry()
				require.NoError(t, registry.AddOrchestratorN("precise-retry", func(ctx *OrchestrationContext) (any, error) {
					policy := &RetryPolicy{
						MaxAttempts: 3, InitialRetryInterval: test.initial,
						BackoffCoefficient: test.coefficient, MaxRetryInterval: test.cap,
					}
					var pending Task
					if kind == "activity" {
						pending = ctx.CallActivity("flaky", WithActivityRetryPolicy(policy))
					} else {
						pending = ctx.CallSubOrchestrator("flaky", WithSubOrchestrationRetryPolicy(policy))
					}
					var value string
					err := pending.Await(&value)
					return value, err
				}))
				turnAt := func(at time.Time) *protos.HistoryEvent {
					event := helpers.NewOrchestratorStartedEvent()
					event.Timestamp = timestamppb.New(at)
					return event
				}
				scheduled := func(id int32) *protos.HistoryEvent {
					if kind == "activity" {
						return helpers.NewTaskScheduledEvent(id, "flaky", nil, nil, nil)
					}
					return helpers.NewSubOrchestrationCreatedEvent(id, "flaky", nil, nil, fmt.Sprintf("instance:%04x", id), nil)
				}
				failed := func(id int32) *protos.HistoryEvent {
					details := &protos.TaskFailureDetails{ErrorType: "Transient", ErrorMessage: "retry"}
					if kind == "activity" {
						return helpers.NewTaskFailedEvent(id, details)
					}
					return &protos.HistoryEvent{EventType: &protos.HistoryEvent_SubOrchestrationInstanceFailed{
						SubOrchestrationInstanceFailed: &protos.SubOrchestrationInstanceFailedEvent{
							TaskScheduledId: id, FailureDetails: details,
						},
					}}
				}
				now := time.Unix(1_700_000_000, 0).UTC()
				history := []*protos.HistoryEvent{
					turnAt(now),
					helpers.NewExecutionStartedEvent("precise-retry", "instance", nil, nil, nil, nil),
					scheduled(0),
				}
				for attempt, delay := range test.delays {
					id := int32(2 * attempt)
					delivered := []*protos.HistoryEvent{turnAt(now), failed(id)}
					response := executeOrchestrationTurn(t, registry, "instance", history, delivered)
					timer := singleRetryTimer(t, response)
					history = append(history, delivered...)
					replay := executeOrchestrationTurn(t, registry, "instance", history, nil)
					require.True(t, proto.Equal(timer, singleRetryTimer(t, replay)))
					require.Equal(t, now.Add(delay), timer.FireAt.AsTime())
					history = append(history, helpers.NewTimerCreatedEvent(id+1, timer.FireAt))
					now = timer.FireAt.AsTime()
					fired := []*protos.HistoryEvent{turnAt(now), helpers.NewTimerFiredEvent(id+1, timer.FireAt, nil)}
					retried := executeOrchestrationTurn(t, registry, "instance", history, fired)
					require.Len(t, retried.Actions, 1)
					require.Equal(t, id+2, retried.Actions[0].Id)
					if kind == "activity" {
						require.Equal(t, "flaky", retried.Actions[0].GetScheduleTask().GetName())
					} else {
						require.Equal(t, "flaky", retried.Actions[0].GetCreateSubOrchestration().GetName())
					}
					history = append(history, fired...)
					history = append(history, scheduled(id+2))
				}
				id := int32(2 * len(test.delays))
				completed := helpers.NewTaskCompletedEvent(id, wrapperspb.String(`"done"`))
				if kind == "sub-orchestration" {
					completed = &protos.HistoryEvent{EventType: &protos.HistoryEvent_SubOrchestrationInstanceCompleted{
						SubOrchestrationInstanceCompleted: &protos.SubOrchestrationInstanceCompletedEvent{
							TaskScheduledId: id, Result: wrapperspb.String(`"done"`),
						},
					}}
				}
				delivered := []*protos.HistoryEvent{turnAt(now), completed}
				require.Equal(t, `"done"`, completionResult(t, executeOrchestrationTurn(t, registry, "instance", history, delivered)))
				history = append(history, delivered...)
				require.Equal(t, `"done"`, completionResult(t, executeOrchestrationTurn(t, registry, "instance", history, nil)))
			})
		}
	}
}
