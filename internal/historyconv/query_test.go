package historyconv

import (
	"errors"
	"strings"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/stretchr/testify/require"
)

func TestStreamValidatedDoesNotLoseHandlerErrors(t *testing.T) {
	failure := errors.New("destination failed")
	calls := 0
	_, _, err := StreamValidated(api.HistoryQuery{ExecutionID: "execution"}, func(handler api.HistoryEventHandler) error {
		_ = handler(executionStarted("execution"))
		_ = handler(executionStarted("execution"))
		return nil // Even a misbehaving source cannot hide a handler failure.
	}, func(*api.HistoryEvent) error {
		calls++
		return failure
	})
	require.ErrorIs(t, err, failure)
	require.Equal(t, 1, calls)
}

func TestStreamValidatedEndOfStreamIdentity(t *testing.T) {
	called := false
	_, _, err := StreamValidated(api.HistoryQuery{ExecutionID: "execution"}, func(handler api.HistoryEventHandler) error {
		return handler(&api.HistoryEvent{Type: api.HistoryEventOrchestratorStarted})
	}, func(*api.HistoryEvent) error {
		called = true
		return nil
	})
	require.True(t, called, "events are delivered without waiting for the whole history")
	require.ErrorContains(t, err, "missing an ExecutionStarted")
}

func collectEvents(query api.HistoryQuery, events ...*api.HistoryEvent) (*api.OrchestrationHistory, error) {
	return Collect("instance", query, func(handler api.HistoryEventHandler) error {
		for _, event := range events {
			if err := handler(event); err != nil {
				return err
			}
		}
		return nil
	})
}

func executionStarted(executionID string) *api.HistoryEvent {
	return &api.HistoryEvent{
		Type: api.HistoryEventExecutionStarted,
		ExecutionStarted: &api.HistoryExecutionStartedEvent{
			InstanceID:  "instance",
			ExecutionID: executionID,
		},
	}
}

func TestCollectFailureContentByteLimit(t *testing.T) {
	large := strings.Repeat("x", 4096)
	failures := []struct {
		name    string
		details *api.FailureDetails
	}{
		{"error type", &api.FailureDetails{ErrorType: api.ErrorType(large)}},
		{"message", &api.FailureDetails{ErrorMessage: large}},
		{"stack", &api.FailureDetails{StackTrace: large}},
		{"inner failure", &api.FailureDetails{InnerFailure: &api.FailureDetails{StackTrace: large}}},
		{"property key", &api.FailureDetails{Properties: map[string]any{large: nil}}},
		{"nested properties", &api.FailureDetails{Properties: map[string]any{
			"nested": []any{nil, true, float64(1), map[string]any{"value": large}},
		}}},
		{"typed properties", &api.FailureDetails{Properties: map[string]any{
			"nested": []map[string]string{{"value": large}},
		}}},
		{"byte property", &api.FailureDetails{Properties: map[string]any{"value": []byte(large)}}},
		{"struct property", &api.FailureDetails{Properties: map[string]any{
			"value": &struct{ Message string }{Message: large},
		}}},
		{"array property", &api.FailureDetails{Properties: map[string]any{"value": [1]string{large}}}},
		{"retained byte capacity", &api.FailureDetails{Properties: map[string]any{"value": make([]byte, 0, 4096)}}},
		{"retained list capacity", &api.FailureDetails{Properties: map[string]any{"value": []string{large}[:0]}}},
	}
	variants := []struct {
		name  string
		event func(*api.FailureDetails) *api.HistoryEvent
	}{
		{"execution", func(details *api.FailureDetails) *api.HistoryEvent {
			return &api.HistoryEvent{ExecutionCompleted: &api.HistoryExecutionCompletedEvent{FailureDetails: details}}
		}},
		{"task", func(details *api.FailureDetails) *api.HistoryEvent {
			return &api.HistoryEvent{TaskFailed: &api.HistoryTaskFailureEvent{FailureDetails: details}}
		}},
		{"sub-orchestration", func(details *api.FailureDetails) *api.HistoryEvent {
			return &api.HistoryEvent{SubOrchestrationInstanceFailed: &api.HistoryTaskFailureEvent{FailureDetails: details}}
		}},
		{"entity", func(details *api.FailureDetails) *api.HistoryEvent {
			return &api.HistoryEvent{Entity: &api.HistoryEntityEvent{FailureDetails: details}}
		}},
		{"history state", func(details *api.FailureDetails) *api.HistoryEvent {
			return &api.HistoryEvent{HistoryState: &api.HistoryStateEvent{
				State: &api.OrchestrationMetadata{FailureDetails: details},
			}}
		}},
	}
	for _, variant := range variants {
		for _, failure := range failures {
			t.Run(variant.name+"/"+failure.name, func(t *testing.T) {
				event := variant.event(failure.details)
				result, err := collectEvents(api.HistoryQuery{MaxBytes: 1024}, event)
				require.ErrorIs(t, err, api.ErrHistoryLimitExceeded)
				require.Nil(t, result)
				require.Greater(t, ApproximateEventSize(event), 1024)
			})
		}
	}
}

func TestCollectHistoryStateContentByteLimit(t *testing.T) {
	large := strings.Repeat("x", 4096)
	states := []struct {
		name  string
		state *api.OrchestrationMetadata
	}{
		{"input", &api.OrchestrationMetadata{SerializedInput: large}},
		{"output", &api.OrchestrationMetadata{SerializedOutput: large}},
		{"custom status", &api.OrchestrationMetadata{SerializedCustomStatus: large}},
		{"name", &api.OrchestrationMetadata{Name: large}},
		{"version", &api.OrchestrationMetadata{Version: large}},
		{"instance", &api.OrchestrationMetadata{InstanceID: api.InstanceID(large)}},
		{"execution", &api.OrchestrationMetadata{ExecutionID: large}},
		{"parent instance", &api.OrchestrationMetadata{ParentInstanceID: api.InstanceID(large)}},
		{"tag key", &api.OrchestrationMetadata{Tags: map[string]string{large: "value"}}},
		{"tag value", &api.OrchestrationMetadata{Tags: map[string]string{"key": large}}},
	}
	for _, test := range states {
		t.Run(test.name, func(t *testing.T) {
			event := &api.HistoryEvent{HistoryState: &api.HistoryStateEvent{State: test.state}}
			result, err := collectEvents(api.HistoryQuery{MaxBytes: 1024}, event)
			require.ErrorIs(t, err, api.ErrHistoryLimitExceeded)
			require.Nil(t, result)
		})
	}
}

func TestCollectParentAndTraceContentByteLimit(t *testing.T) {
	large := strings.Repeat("x", 4096)
	parents := []struct {
		name   string
		parent *api.HistoryParentInstanceInfo
	}{
		{"name", &api.HistoryParentInstanceInfo{Name: large}},
		{"version", &api.HistoryParentInstanceInfo{Version: large}},
		{"instance", &api.HistoryParentInstanceInfo{InstanceID: api.InstanceID(large)}},
		{"execution", &api.HistoryParentInstanceInfo{ExecutionID: large}},
	}
	for _, test := range parents {
		started := executionStarted("execution")
		started.ExecutionStarted.Parent = test.parent
		for name, event := range map[string]*api.HistoryEvent{
			"started": started,
			"rewound": {ExecutionRewound: &api.HistoryExecutionRewoundEvent{Parent: test.parent}},
		} {
			t.Run(name+"/"+test.name, func(t *testing.T) {
				result, err := collectEvents(api.HistoryQuery{MaxBytes: 1024}, event)
				require.ErrorIs(t, err, api.ErrHistoryLimitExceeded)
				require.Nil(t, result)
			})
		}
	}
	traces := []struct {
		name  string
		trace *api.HistoryTraceContext
	}{
		{"parent", &api.HistoryTraceContext{TraceParent: large}},
		{"state", &api.HistoryTraceContext{TraceState: large}},
		{"span", &api.HistoryTraceContext{SpanID: large}},
	}
	for _, test := range traces {
		started := executionStarted("execution")
		started.ExecutionStarted.ParentTraceContext = test.trace
		for name, event := range map[string]*api.HistoryEvent{
			"started":           started,
			"task":              {TaskScheduled: &api.HistoryTaskScheduledEvent{ParentTraceContext: test.trace}},
			"sub-orchestration": {SubOrchestrationInstanceCreated: &api.HistorySubOrchestrationInstanceCreatedEvent{ParentTraceContext: test.trace}},
			"rewound":           {ExecutionRewound: &api.HistoryExecutionRewoundEvent{ParentTraceContext: test.trace}},
		} {
			t.Run(name+"/"+test.name, func(t *testing.T) {
				result, err := collectEvents(api.HistoryQuery{MaxBytes: 1024}, event)
				require.ErrorIs(t, err, api.ErrHistoryLimitExceeded)
				require.Nil(t, result)
			})
		}
	}
}

func TestCollectRetainedContentFailsClosed(t *testing.T) {
	cyclicFailure := &api.FailureDetails{}
	cyclicFailure.InnerFailure = cyclicFailure
	cyclicMap := map[string]any{}
	cyclicMap["self"] = cyclicMap
	cyclicList := make([]any, 1)
	cyclicList[0] = cyclicList
	var cyclicPointer any
	cyclicPointer = &cyclicPointer
	deepFailure := &api.FailureDetails{}
	for range api.MaxFailureDetailsDepth {
		deepFailure = &api.FailureDetails{InnerFailure: deepFailure}
	}
	var deepProperty any = "value"
	for range 128 {
		deepProperty = map[string]any{"nested": []any{deepProperty}}
	}
	var repeatedProperty any = strings.Repeat("x", 1024*1024)
	for range 12 {
		repeatedProperty = []any{repeatedProperty, repeatedProperty}
	}
	for name, details := range map[string]*api.FailureDetails{
		"cyclic failure": cyclicFailure,
		"cyclic map":     {Properties: cyclicMap},
		"cyclic list":    {Properties: map[string]any{"value": cyclicList}},
		"cyclic pointer": {Properties: map[string]any{"value": cyclicPointer}},
		"deep failure":   deepFailure,
		"deep property":  {Properties: map[string]any{"value": deepProperty}},
		"saturated size": {Properties: map[string]any{"value": repeatedProperty}},
	} {
		t.Run(name, func(t *testing.T) {
			event := &api.HistoryEvent{TaskFailed: &api.HistoryTaskFailureEvent{FailureDetails: details}}
			require.Equal(t, api.MaxHistoryMaxBytes+1, ApproximateEventSize(event))
			result, err := collectEvents(api.HistoryQuery{MaxBytes: api.MaxHistoryMaxBytes},
				&api.HistoryEvent{Generic: &api.HistoryPayloadEvent{SerializedInput: "small"}}, event)
			require.ErrorIs(t, err, api.ErrHistoryLimitExceeded)
			require.Nil(t, result)
		})
	}
}

func TestRetainedSizeArithmeticSaturates(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	require.Equal(t, maxEstimatedSize, addSize(maxEstimatedSize, maxInt))
	require.Equal(t, maxEstimatedSize, addSize(maxInt, maxInt))
	require.Equal(t, maxEstimatedSize, addSize(512, maxInt))
	require.Equal(t, maxEstimatedSize, addSize(api.MaxHistoryMaxBytes, 1))
	require.Equal(t, api.MaxHistoryMaxBytes, addSize(api.MaxHistoryMaxBytes-1, 1))
	require.Equal(t, maxEstimatedSize, multiplySize(maxInt, maxInt))
	require.Equal(t, maxEstimatedSize, multiplySize(maxInt, 16))
	require.Equal(t, 0, multiplySize(maxInt, 0))
	require.Equal(t, 0, multiplySize(0, maxInt))
	require.Equal(t, 64, multiplySize(4, 16))
}

func TestCollectAllowsMaximumFailureDepth(t *testing.T) {
	details := &api.FailureDetails{}
	for range api.MaxFailureDetailsDepth - 1 {
		details = &api.FailureDetails{InnerFailure: details}
	}
	event := &api.HistoryEvent{TaskFailed: &api.HistoryTaskFailureEvent{FailureDetails: details}}
	result, err := collectEvents(api.HistoryQuery{MaxBytes: 4096}, event)
	require.NoError(t, err)
	require.Equal(t, []*api.HistoryEvent{event}, result.Events)
}

func TestApproximateEventSizeDoesNotAllocateForPayloads(t *testing.T) {
	event := &api.HistoryEvent{Generic: &api.HistoryPayloadEvent{SerializedInput: strings.Repeat("x", 4096)}}
	var size int
	require.Zero(t, testing.AllocsPerRun(100, func() {
		size = ApproximateEventSize(event)
	}))
	require.Greater(t, size, len(event.Generic.SerializedInput))
}

func TestCollectRetainsSmallHistoriesWithinTheBudget(t *testing.T) {
	details := &api.FailureDetails{
		ErrorType:    "failure",
		ErrorMessage: "message",
		InnerFailure: &api.FailureDetails{StackTrace: "stack"},
		Properties:   map[string]any{"nested": []any{nil, true, float64(1), map[string]any{"key": "value"}}},
	}
	events := []*api.HistoryEvent{
		executionStarted("execution"),
		{ExecutionCompleted: &api.HistoryExecutionCompletedEvent{FailureDetails: details}},
		{HistoryState: &api.HistoryStateEvent{State: &api.OrchestrationMetadata{
			SerializedInput: "input", SerializedOutput: "output", SerializedCustomStatus: "status",
		}}},
	}
	size := 0
	for _, event := range events {
		size += ApproximateEventSize(event)
	}
	require.Less(t, size, 4096)
	result, err := collectEvents(api.HistoryQuery{MaxBytes: size}, events...)
	require.NoError(t, err)
	require.Equal(t, events, result.Events)
	require.Equal(t, "execution", result.ExecutionID)

	result, err = collectEvents(api.HistoryQuery{MaxBytes: size - 1}, events...)
	require.ErrorIs(t, err, api.ErrHistoryLimitExceeded)
	require.Nil(t, result)
	result, err = collectEvents(api.HistoryQuery{MaxEvents: len(events) - 1}, events...)
	require.ErrorIs(t, err, api.ErrHistoryLimitExceeded)
	require.Nil(t, result)
}

func TestCollectCountsAllRetainedDetails(t *testing.T) {
	event := executionStarted("execution")
	event.TaskFailed = &api.HistoryTaskFailureEvent{FailureDetails: &api.FailureDetails{
		ErrorMessage: strings.Repeat("x", 4096),
	}}
	result, err := collectEvents(api.HistoryQuery{MaxBytes: 1024}, event)
	require.ErrorIs(t, err, api.ErrHistoryLimitExceeded)
	require.Nil(t, result)
}

func TestCollectExecutionIdentity(t *testing.T) {
	generic := &api.HistoryEvent{Type: api.HistoryEventGeneric, Generic: &api.HistoryPayloadEvent{SerializedInput: "value"}}
	for _, test := range []struct {
		name      string
		requested string
		events    []*api.HistoryEvent
		observed  string
		wantError bool
	}{
		{"pinned match", "A", []*api.HistoryEvent{executionStarted("A")}, "A", false},
		{"pinned mismatch", "A", []*api.HistoryEvent{executionStarted("B")}, "", true},
		{"pinned mixed", "A", []*api.HistoryEvent{executionStarted("A"), executionStarted("B")}, "", true},
		{"unpinned mixed", "", []*api.HistoryEvent{executionStarted("A"), executionStarted("B")}, "", true},
		{"pinned repeated match", "A", []*api.HistoryEvent{executionStarted("A"), executionStarted("A")}, "A", false},
		{"pinned empty", "A", nil, "", true},
		{"pinned generic", "A", []*api.HistoryEvent{generic}, "", true},
		{"pinned missing ID", "A", []*api.HistoryEvent{executionStarted("")}, "", true},
		{"unpinned missing ID", "", []*api.HistoryEvent{executionStarted("")}, "", true},
		{"pinned missing details", "A", []*api.HistoryEvent{{Type: api.HistoryEventExecutionStarted}}, "", true},
		{"missing repeated ID", "A", []*api.HistoryEvent{executionStarted("A"), executionStarted("")}, "", true},
		{"unpinned empty", "", nil, "", false},
		{"unpinned generic", "", []*api.HistoryEvent{generic}, "", false},
		{"unpinned observed", "", []*api.HistoryEvent{generic, executionStarted("A")}, "A", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := collectEvents(api.HistoryQuery{ExecutionID: test.requested}, test.events...)
			if test.wantError {
				require.ErrorContains(t, err, "execution")
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.observed, result.ExecutionID)
				require.Equal(t, test.events, result.Events)
			}
		})
	}
}

func TestCollectRejectsNilEvents(t *testing.T) {
	result, err := collectEvents(api.HistoryQuery{}, nil)
	require.ErrorContains(t, err, "nil")
	require.Nil(t, result)
}
