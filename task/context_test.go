package task

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/contextprop"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestReplaySafeLoggerSuppressesReplayOutput(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	ctx := newTestOrchestrationContext(NewTaskRegistry(), "logging", nil, nil)
	ctx.Name = "logger-test"
	ctx.Version = "v1"
	ctx.logger = logger

	ctx.IsReplaying = true
	ctx.Logger().Info("replayed")
	ctx.IsReplaying = false
	ctx.Logger().Info("live")

	if strings.Contains(output.String(), "replayed") {
		t.Fatalf("replay log was emitted: %s", output.String())
	}
	if !strings.Contains(output.String(), "live") {
		t.Fatalf("live log was not emitted: %s", output.String())
	}
}

func TestOrchestrationContextPropagatesOnlyPersistedIdentityAndFields(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("context-test", func(ctx *OrchestrationContext) (any, error) {
		info, ok := api.OrchestrationContextInfoFromContext(ctx.Context())
		if !ok {
			return nil, nil
		}
		return struct {
			Info   api.OrchestrationContextInfo
			Fields api.ContextFields
		}{
			Info:   info,
			Fields: api.ContextFieldsFromContext(ctx.Context()),
		}, nil
	}); err != nil {
		t.Fatal(err)
	}

	fields := api.ContextFields{"tenant": "alpha"}
	instanceID := api.InstanceID("context-instance")
	parent := helpers.NewParentInfo(4, "parent", "parent-instance")
	started := helpers.NewExecutionStartedEvent(
		"context-test",
		string(instanceID),
		nil,
		parent,
		nil,
		nil,
		wrapperspb.String("v2"),
	)
	started.GetExecutionStarted().Tags = contextprop.Encode(api.OrchestrationContextInfo{}, fields)
	fields["tenant"] = "mutated"
	events := []*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent(), started}

	run := func(workerField string) string {
		executor := NewTaskExecutor(registry, WithContextFields(api.ContextFields{"worker": workerField}))
		result, err := executor.ExecuteOrchestrator(context.Background(), instanceID, nil, events, nil)
		if err != nil {
			t.Fatal(err)
		}
		return completionAction(t, result.Response).GetResult().GetValue()
	}
	first, second := run("first-worker"), run("second-worker")
	if first != second {
		t.Fatalf("orchestration output changed across worker context: first=%s second=%s", first, second)
	}

	var output struct {
		Info   api.OrchestrationContextInfo
		Fields api.ContextFields
	}
	if err := json.Unmarshal([]byte(first), &output); err != nil {
		t.Fatal(err)
	}
	if output.Info.InstanceID != instanceID ||
		output.Info.Name != "context-test" ||
		output.Info.Version != "v2" ||
		output.Info.ParentInstanceID != "parent-instance" {
		t.Fatalf("unexpected orchestration info: %+v", output.Info)
	}
	if output.Fields["tenant"] != "alpha" || output.Fields["worker"] != "" {
		t.Fatalf("orchestration fields are not purely persisted: %#v", output.Fields)
	}
}

func TestActivityContextPropagatesIdentityFieldsAndLogger(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddActivityN("inspect", func(ctx ActivityContext) (any, error) {
		orchestration, _ := api.OrchestrationContextInfoFromContext(ctx.Context())
		activity, _ := api.ActivityContextInfoFromContext(ctx.Context())
		LoggerFromContext(ctx.Context()).Info("activity log")
		return struct {
			Orchestration api.OrchestrationContextInfo
			Activity      api.ActivityContextInfo
			Fields        api.ContextFields
		}{
			Orchestration: orchestration,
			Activity:      activity,
			Fields:        api.ContextFieldsFromContext(ctx.Context()),
		}, nil
	}); err != nil {
		t.Fatal(err)
	}

	var logs bytes.Buffer
	executor := NewTaskExecutor(
		registry,
		WithLogger(slog.New(slog.NewTextHandler(&logs, nil))),
		WithContextFields(api.ContextFields{"feature": "enabled"}),
	)
	instanceID := api.InstanceID("activity-context")
	base := api.WithOrchestrationContextInfo(context.Background(), api.OrchestrationContextInfo{
		InstanceID: instanceID,
		Name:       "parent",
		Version:    "v3",
	})
	response, err := executor.ExecuteActivity(
		base,
		instanceID,
		helpers.NewTaskScheduledEvent(9, "inspect", wrapperspb.String("a1"), nil, nil),
	)
	if err != nil {
		t.Fatal(err)
	}

	var output struct {
		Orchestration api.OrchestrationContextInfo
		Activity      api.ActivityContextInfo
		Fields        api.ContextFields
	}
	if err := json.Unmarshal([]byte(response.GetTaskCompleted().GetResult().GetValue()), &output); err != nil {
		t.Fatal(err)
	}
	if output.Orchestration.Name != "parent" || output.Orchestration.Version != "v3" {
		t.Fatalf("unexpected orchestration context: %+v", output.Orchestration)
	}
	if output.Activity.Name != "inspect" || output.Activity.Version != "a1" || output.Activity.TaskID != 9 {
		t.Fatalf("unexpected activity context: %+v", output.Activity)
	}
	if output.Fields["feature"] != "enabled" {
		t.Fatalf("context field = %q, want enabled", output.Fields["feature"])
	}
	if !strings.Contains(logs.String(), "activity log") {
		t.Fatalf("activity logger output missing: %s", logs.String())
	}
}

func TestActivityContextDecodesDurableContextTags(t *testing.T) {
	registry := NewTaskRegistry()
	if err := registry.AddActivityN("inspect-tags", func(ctx ActivityContext) (any, error) {
		orchestration, _ := api.OrchestrationContextInfoFromContext(ctx.Context())
		return struct {
			Orchestration api.OrchestrationContextInfo
			Fields        api.ContextFields
		}{
			Orchestration: orchestration,
			Fields:        api.ContextFieldsFromContext(ctx.Context()),
		}, nil
	}); err != nil {
		t.Fatal(err)
	}

	event := helpers.NewTaskScheduledEvent(4, "inspect-tags", nil, nil, nil)
	event.GetTaskScheduled().Tags = contextprop.Encode(api.OrchestrationContextInfo{
		InstanceID:       "tagged-instance",
		Name:             "tagged-parent",
		Version:          "v4",
		ParentInstanceID: "root",
	}, api.ContextFields{"tenant": "tagged"})
	response, err := NewTaskExecutor(registry).ExecuteActivity(
		context.Background(),
		"tagged-instance",
		event,
	)
	if err != nil {
		t.Fatal(err)
	}

	var output struct {
		Orchestration api.OrchestrationContextInfo
		Fields        api.ContextFields
	}
	if err := json.Unmarshal([]byte(response.GetTaskCompleted().GetResult().GetValue()), &output); err != nil {
		t.Fatal(err)
	}
	if output.Orchestration.Name != "tagged-parent" ||
		output.Orchestration.Version != "v4" ||
		output.Orchestration.ParentInstanceID != "root" {
		t.Fatalf("unexpected orchestration identity: %+v", output.Orchestration)
	}
	if output.Fields["tenant"] != "tagged" {
		t.Fatalf("tenant = %q, want tagged", output.Fields["tenant"])
	}
}

type hostContextKey struct{}

func TestOrchestrationContextExcludesHostContextState(t *testing.T) {
	type contextState struct {
		HasHostValue bool
		HasDeadline  bool
		HasDone      bool
		HasInfo      bool
		InstanceID   api.InstanceID
		HasLogger    bool
	}
	registry := NewTaskRegistry()
	if err := registry.AddOrchestratorN("isolated-context", func(ctx *OrchestrationContext) (any, error) {
		deterministic := ctx.Context()
		info, hasInfo := api.OrchestrationContextInfoFromContext(deterministic)
		_, hasDeadline := deterministic.Deadline()
		return contextState{
			HasHostValue: deterministic.Value(hostContextKey{}) != nil,
			HasDeadline:  hasDeadline,
			HasDone:      deterministic.Done() != nil,
			HasInfo:      hasInfo,
			InstanceID:   info.InstanceID,
			HasLogger:    LoggerFromContext(deterministic) != slog.Default(),
		}, nil
	}); err != nil {
		t.Fatal(err)
	}

	host, cancel := context.WithTimeout(
		context.WithValue(context.Background(), hostContextKey{}, "host-only"),
		time.Hour,
	)
	defer cancel()
	instanceID := api.InstanceID("isolated-context-instance")
	result, err := NewTaskExecutor(registry).ExecuteOrchestrator(
		host,
		instanceID,
		nil,
		[]*protos.HistoryEvent{
			helpers.NewOrchestratorStartedEvent(),
			helpers.NewExecutionStartedEvent("isolated-context", string(instanceID), nil, nil, nil, nil),
		}, nil)

	if err != nil {
		t.Fatal(err)
	}

	var output contextState
	if err := json.Unmarshal([]byte(completionAction(t, result.Response).GetResult().GetValue()), &output); err != nil {
		t.Fatal(err)
	}
	if output.HasHostValue || output.HasDeadline || output.HasDone || output.HasLogger {
		t.Fatalf("host context leaked into orchestrator context: %+v", output)
	}
	if !output.HasInfo || output.InstanceID != instanceID {
		t.Fatalf("persisted orchestration identity missing: %+v", output)
	}
}
