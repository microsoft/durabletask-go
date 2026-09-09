package task

import (
	"context"
	"errors"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestCompareVersionsMatchesDurableTaskRules(t *testing.T) {
	tests := []struct {
		left  string
		right string
		want  int
	}{
		{"", "", 0},
		{"", "1.0", -1},
		{"1.0", "", 1},
		{"1.2", "1.10", -1},
		{"2.0.0", "2.0", 1},
		{"preview-A", "preview-a", 0},
		{"preview-b", "preview-a", 1},
	}
	for _, test := range tests {
		got := compareVersions(test.left, test.right)
		if got < 0 {
			got = -1
		} else if got > 0 {
			got = 1
		}
		if got != test.want {
			t.Fatalf("compareVersions(%q, %q) = %d, want %d", test.left, test.right, got, test.want)
		}
	}
}

func TestVersionMismatchRejectsOrchestration(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("versioned", func(*OrchestrationContext) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	instanceID := api.InstanceID("version-reject")
	executor := NewTaskExecutor(registry, WithVersioning(VersioningOptions{
		Version:         "1.0",
		MatchStrategy:   VersionMatchCurrentOrOlder,
		FailureStrategy: VersionFailureReject,
	}))
	_, err := executor.ExecuteOrchestrator(
		context.Background(),
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewExecutionStartedEvent(
				"versioned",
				string(instanceID),
				nil,
				nil,
				nil,
				nil,
				wrapperspb.String("2.0"),
			),
		}, nil)

	var mismatch *VersionMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("error = %v, want VersionMismatchError", err)
	}
}

func TestVersionMismatchFailsOrchestration(t *testing.T) {
	registry := NewTaskRegistry()
	instanceID := api.InstanceID("version-fail")
	executor := NewTaskExecutor(registry, WithVersioning(VersioningOptions{
		Version:         "1.0",
		MatchStrategy:   VersionMatchStrict,
		FailureStrategy: VersionFailureFail,
	}))
	result, err := executor.ExecuteOrchestrator(
		context.Background(),
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewExecutionStartedEvent(
				"versioned",
				string(instanceID),
				nil,
				nil,
				nil,
				nil,
				wrapperspb.String("2.0"),
			),
		}, nil)

	if err != nil {
		t.Fatal(err)
	}
	completed := result.Response.Actions[0].GetCompleteOrchestration()
	if completed.GetOrchestrationStatus() != protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED {
		t.Fatalf("status = %v", completed.GetOrchestrationStatus())
	}
	if !completed.GetFailureDetails().GetIsNonRetriable() {
		t.Fatal("version mismatch failure must be non-retriable")
	}
}

func TestVersionMismatchFailsActivity(t *testing.T) {
	registry := NewTaskRegistry()
	executor := NewTaskExecutor(registry, WithVersioning(VersioningOptions{
		Version:         "1.0",
		MatchStrategy:   VersionMatchStrict,
		FailureStrategy: VersionFailureFail,
	}))
	event := helpers.NewTaskScheduledEvent(
		1,
		"activity",
		wrapperspb.String("2.0"),
		nil,
		nil,
	)
	result, err := executor.ExecuteActivity(context.Background(), "instance", event)
	if err != nil {
		t.Fatal(err)
	}
	failed := result.GetTaskFailed()
	if failed == nil || !failed.GetFailureDetails().GetIsNonRetriable() {
		t.Fatalf("activity result = %v", result)
	}
}

func TestVersionedRegistryDispatchAndSchedulingDefaults(t *testing.T) {
	registry := NewTaskRegistry()
	requireNoError := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	requireNoError(registry.AddOrchestratorNVersion("parent", "v1", func(ctx *OrchestrationContext) (any, error) {
		ctx.CallActivity("activity")
		ctx.CallSubOrchestrator("child")
		ctx.ContinueAsNew("next", WithContinueAsNewVersion("v3"))
		return nil, nil
	}))

	instanceID := api.InstanceID("versioned-defaults")
	executor := NewTaskExecutor(registry, WithVersioning(VersioningOptions{
		DefaultVersion: "v2",
		MatchStrategy:  VersionMatchNone,
	}))
	result, err := executor.ExecuteOrchestrator(
		context.Background(),
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewExecutionStartedEvent(
				"parent",
				string(instanceID),
				nil,
				nil,
				nil,
				nil,
				wrapperspb.String("v1"),
			),
		}, nil)

	if err != nil {
		t.Fatal(err)
	}

	var activityVersion, subVersion, newVersion string
	for _, action := range result.Response.Actions {
		switch {
		case action.GetScheduleTask() != nil:
			activityVersion = action.GetScheduleTask().GetVersion().GetValue()
		case action.GetCreateSubOrchestration() != nil:
			subVersion = action.GetCreateSubOrchestration().GetVersion().GetValue()
		case action.GetCompleteOrchestration() != nil:
			newVersion = action.GetCompleteOrchestration().GetNewVersion().GetValue()
		}
	}
	if activityVersion != "v1" {
		t.Fatalf("activity version = %q, want inherited v1", activityVersion)
	}
	if subVersion != "v2" {
		t.Fatalf("sub-orchestration version = %q, want default v2", subVersion)
	}
	if newVersion != "v3" {
		t.Fatalf("continue-as-new version = %q, want v3", newVersion)
	}
}

func TestAllowedUnversionedSystemOrchestratorBypassesStrictVersionMatch(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("system", func(*OrchestrationContext) (any, error) {
		return "ok", nil
	}); err != nil {
		t.Fatal(err)
	}
	executor := NewTaskExecutor(
		registry,
		WithVersioning(VersioningOptions{
			Version:       "1.0",
			MatchStrategy: VersionMatchStrict,
		}),
		WithUnversionedOrchestratorNames("system"),
	)
	result, err := executor.ExecuteOrchestrator(
		context.Background(),
		"system-instance",
		nil,
		[]*protos.HistoryEvent{
			helpers.NewExecutionStartedEvent("system", "system-instance", nil, nil, nil, nil, wrapperspb.String("")),
		}, nil)

	if err != nil {
		t.Fatal(err)
	}
	if result.Response.Actions[len(result.Response.Actions)-1].GetCompleteOrchestration() == nil {
		t.Fatalf("system orchestrator did not complete: %#v", result.Response.Actions)
	}
}

// TestAllowedUnversionedSystemActivityBypassesStrictVersionMatch covers the
// activity half of the same contract: a system orchestration runs unversioned,
// and an activity inherits its caller's version, so its work item arrives
// unversioned and would otherwise be rejected by a strict worker.
func TestAllowedUnversionedSystemActivityBypassesStrictVersionMatch(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddActivityN("SystemActivity", func(ActivityContext) (any, error) {
		return "ok", nil
	}); err != nil {
		t.Fatal(err)
	}
	scheduled := helpers.NewTaskScheduledEvent(1, "SystemActivity", wrapperspb.String(""), nil, nil)
	versioning := VersioningOptions{Version: "1.0", MatchStrategy: VersionMatchStrict}

	rejected := NewTaskExecutor(registry, WithVersioning(versioning))
	if _, err := rejected.ExecuteActivity(context.Background(), "system-instance", scheduled); err == nil {
		t.Fatal("expected a strict version mismatch without the allow-list")
	}

	allowed := NewTaskExecutor(
		registry,
		WithVersioning(versioning),
		WithUnversionedActivityNames("systemactivity"),
	)
	response, err := allowed.ExecuteActivity(context.Background(), "system-instance", scheduled)
	if err != nil {
		t.Fatal(err)
	}
	if response.GetTaskCompleted() == nil {
		t.Fatalf("system activity did not complete: %#v", response)
	}

	// A versioned work item for the same name is still version-checked.
	versionedScheduled := helpers.NewTaskScheduledEvent(2, "SystemActivity", wrapperspb.String("9.9"), nil, nil)
	if _, err := allowed.ExecuteActivity(context.Background(), "system-instance", versionedScheduled); err == nil {
		t.Fatal("expected a strict version mismatch for a versioned work item")
	}
}

func TestSubOrchestrationOptionsAddTagsAndContextFields(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("parent", func(ctx *OrchestrationContext) (any, error) {
		ctx.CallSubOrchestrator(
			"child",
			WithSubOrchestrationTags(map[string]string{"schedule": "daily"}),
			WithSubOrchestrationContextFields(api.ContextFields{"tenant": "north"}),
		)
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	executor := NewTaskExecutor(registry)
	result, err := executor.ExecuteOrchestrator(
		context.Background(),
		"parent-instance",
		nil,
		[]*protos.HistoryEvent{
			helpers.NewExecutionStartedEvent("parent", "parent-instance", nil, nil, nil, nil),
		}, nil)

	if err != nil {
		t.Fatal(err)
	}
	for _, action := range result.Response.Actions {
		if created := action.GetCreateSubOrchestration(); created != nil {
			if created.Tags["schedule"] != "daily" {
				t.Fatalf("tags = %#v", created.Tags)
			}
			if created.Tags["__durabletask.context.field.tenant"] != "north" {
				t.Fatalf("context tags = %#v", created.Tags)
			}
			return
		}
	}
	t.Fatal("missing sub-orchestration action")
}

func TestActivityTagsOverrideOrchestrationTagsAndAreCloned(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("parent", func(ctx *OrchestrationContext) (any, error) {
		tags := map[string]string{"scope": "activity", "child": "yes"}
		ctx.CallActivity("activity", WithActivityTags(tags))
		tags["child"] = "mutated"
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	started := helpers.NewExecutionStartedEvent("parent", "parent-instance", nil, nil, nil, nil)
	started.GetExecutionStarted().Tags = map[string]string{"scope": "orchestration", "parent": "yes"}
	result, err := NewTaskExecutor(registry).ExecuteOrchestrator(
		context.Background(),
		"parent-instance",
		nil,
		[]*protos.HistoryEvent{started}, nil)

	if err != nil {
		t.Fatal(err)
	}
	var foundActivity, foundCompletion bool
	for _, action := range result.Response.Actions {
		if scheduled := action.GetScheduleTask(); scheduled != nil {
			if scheduled.Tags["scope"] != "activity" ||
				scheduled.Tags["parent"] != "yes" ||
				scheduled.Tags["child"] != "yes" {
				t.Fatalf("activity tags = %#v", scheduled.Tags)
			}
			foundActivity = true
		}
		if completed := action.GetCompleteOrchestration(); completed != nil {
			if completed.Tags["scope"] != "orchestration" || completed.Tags["parent"] != "yes" {
				t.Fatalf("completion tags = %#v", completed.Tags)
			}
			foundCompletion = true
		}
	}
	if !foundActivity || !foundCompletion {
		t.Fatalf("found activity=%t completion=%t", foundActivity, foundCompletion)
	}
}

func TestActivityTagsRejectReservedAndEmptyKeys(t *testing.T) {
	for _, tags := range []map[string]string{
		{"": "value"},
		{api.ReservedContextFieldPrefix + "tenant": "value"},
	} {
		if err := WithActivityTags(tags)(new(callActivityOptions), api.DefaultDataConverter()); err == nil {
			t.Fatalf("WithActivityTags(%#v) succeeded", tags)
		}
	}
}

func TestActivityTagValidationIsDeterministic(t *testing.T) {
	tags := map[string]string{
		api.ReservedContextFieldPrefix + "z": "value",
		api.ReservedContextFieldPrefix + "a": "value",
	}
	for range 20 {
		err := WithActivityTags(tags)(new(callActivityOptions), api.DefaultDataConverter())
		if err == nil || err.Error() != `activity tag "__durabletask.context.a" uses a reserved prefix` {
			t.Fatalf("unexpected validation error: %v", err)
		}
	}
}

func TestContinueAsNewCarriesTagsAndNextVersion(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorNVersion("eternal", "v1", func(ctx *OrchestrationContext) (any, error) {
		ctx.ContinueAsNew("next", WithContinueAsNewVersion("v2"))
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	started := helpers.NewExecutionStartedEvent(
		"eternal",
		"eternal-instance",
		nil,
		nil,
		nil,
		nil,
		wrapperspb.String("v1"),
	)
	started.GetExecutionStarted().Tags = map[string]string{
		"tenant": "north",
		"__durabletask.context.field.correlation_id": "42",
	}
	result, err := NewTaskExecutor(registry).ExecuteOrchestrator(
		context.Background(),
		"eternal-instance",
		nil,
		[]*protos.HistoryEvent{started}, nil)

	if err != nil {
		t.Fatal(err)
	}
	completed := completionAction(t, result.Response)
	if completed.GetTags()["tenant"] != "north" {
		t.Fatalf("continue-as-new tags = %#v", completed.GetTags())
	}
	if completed.GetTags()["__durabletask.context.field.correlation_id"] != "42" {
		t.Fatalf("continue-as-new context tags = %#v", completed.GetTags())
	}
	if completed.GetTags()["__durabletask.context.orchestration_version"] != "v2" {
		t.Fatalf("continue-as-new version tag = %#v", completed.GetTags())
	}
}

func TestExplicitUnversionedSchedulingOverridesDefaults(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorNVersion("parent", "v1", func(ctx *OrchestrationContext) (any, error) {
		ctx.CallActivity("activity", WithActivityVersion(""))
		ctx.CallSubOrchestrator("child", WithSubOrchestrationVersion(""))
		ctx.ContinueAsNew(nil, WithContinueAsNewVersion(""))
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	instanceID := api.InstanceID("explicit-unversioned")
	executor := NewTaskExecutor(registry, WithVersioning(VersioningOptions{
		DefaultVersion: "v2",
		MatchStrategy:  VersionMatchNone,
	}))
	result, err := executor.ExecuteOrchestrator(
		context.Background(),
		instanceID,
		nil,
		[]*protos.HistoryEvent{helpers.NewExecutionStartedEvent(
			"parent",
			string(instanceID),
			nil,
			nil,
			nil,
			nil,
			wrapperspb.String("v1"),
		)}, nil)

	if err != nil {
		t.Fatal(err)
	}
	for _, action := range result.Response.Actions {
		if scheduled := action.GetScheduleTask(); scheduled != nil && scheduled.Version == nil {
			t.Fatal("explicit unversioned activity must retain wrapper presence")
		}
		if scheduled := action.GetCreateSubOrchestration(); scheduled != nil && scheduled.Version == nil {
			t.Fatal("explicit unversioned sub-orchestration must retain wrapper presence")
		}
		if completed := action.GetCompleteOrchestration(); completed != nil {
			if completed.NewVersion == nil || completed.NewVersion.GetValue() != "" {
				t.Fatal("explicit unversioned ContinueAsNew must retain wrapper presence")
			}
		}
	}
}

func TestVersionedReplayAcceptsLegacyUnversionedChildHistory(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorNVersion("parent", "v1", func(ctx *OrchestrationContext) (any, error) {
		ctx.CallActivity("activity")
		ctx.CallSubOrchestrator("child")
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}

	instanceID := api.InstanceID("legacy-unversioned-children")
	executor := NewTaskExecutor(registry, WithVersioning(VersioningOptions{
		DefaultVersion: "v2",
		MatchStrategy:  VersionMatchNone,
	}))
	oldEvents := []*protos.HistoryEvent{
		helpers.NewExecutionStartedEvent(
			"parent",
			string(instanceID),
			nil,
			nil,
			nil,
			nil,
			wrapperspb.String("v1"),
		),
		helpers.NewTaskScheduledEvent(0, "activity", nil, nil, nil),
		helpers.NewSubOrchestrationCreatedEvent(1, "child", nil, nil, "child-instance", nil),
	}

	if _, err := executor.ExecuteOrchestrator(context.Background(), instanceID, oldEvents, nil, nil); err != nil {
		t.Fatalf("legacy unversioned child history failed replay: %v", err)
	}
}
