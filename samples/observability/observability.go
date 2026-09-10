// Command observability demonstrates tags, immutable context fields, logs, and metric hooks.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/microsoft/durabletask-go/task"
)

var observabilityAttempts = &attemptCounter{counts: make(map[string]int)}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("SAMPLE_OK observability")
}

func run() (err error) {
	logs := newLogCollector()
	metrics := newMetricCollector()

	registry := task.NewTaskRegistry()
	if err := registry.AddOrchestratorN("SampleObservability", observabilityWorkflow); err != nil {
		return err
	}
	if err := registry.AddActivityN("SampleObservabilityActivity", observabilityActivity); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	app, err := dtssample.Start(ctx, registry,
		durabletaskclient.WithTaskExecutorOptions(
			task.WithLogger(slog.New(logs)),
			task.WithContextFields(api.ContextFields{"worker": "sample-worker"}),
			task.WithMetricsHooks(task.MetricsHooks{
				Retry:   metrics.AddRetry,
				History: metrics.AddHistory,
			}),
		),
	)
	if err != nil {
		return err
	}
	id := dtssample.NewInstanceID("observability")
	defer func() { err = errors.Join(err, dtssample.Cleanup(app.Client, id), app.Shutdown()) }()

	input := observabilityInput{Key: string(id)}
	tags := map[string]string{"sample": "observability", "scenario": "propagation"}
	fields := api.ContextFields{"tenant": "alpha", "immutable": "root"}
	if _, err := app.Client.ScheduleNewOrchestration(ctx, "SampleObservability",
		api.WithInstanceID(id), api.WithInput(input), api.WithTags(tags), api.WithContextFields(fields)); err != nil {
		return err
	}
	metadata, err := app.Client.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	var output observabilityOutput
	if err := metadata.ReadOutput(&output); err != nil {
		return err
	}
	if output.InstanceID != string(id) || output.OrchestrationName != "SampleObservability" ||
		output.OrchestrationFields["tenant"] != "alpha" || output.OrchestrationFields["immutable"] != "root" ||
		output.ActivityName != "SampleObservabilityActivity" ||
		output.ActivityFields["worker"] != "sample-worker" || output.Attempts != 2 {
		return fmt.Errorf("unexpected observability output: %+v", output)
	}
	if _, leaked := output.OrchestrationFields["worker"]; leaked {
		return errors.New("worker-local fields leaked into durable orchestration context")
	}

	refetched, err := app.Client.FetchOrchestrationMetadata(ctx, id, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if refetched.Tags["sample"] != "observability" || refetched.Tags["scenario"] != "propagation" {
		return fmt.Errorf("metadata tags = %+v", refetched.Tags)
	}
	if refetched.ExecutionID == "" {
		return errors.New("completed metadata did not include an execution ID")
	}
	if err := verifyHistoryFields(ctx, app.Client, id, refetched.ExecutionID, tags, fields); err != nil {
		return err
	}
	if err := metrics.Verify(id); err != nil {
		return err
	}
	if err := logs.Verify(id); err != nil {
		return err
	}
	fmt.Println("verified tags, context fields, replay-safe logs, retry metrics, and history metrics")
	return nil
}

func observabilityWorkflow(ctx *task.OrchestrationContext) (any, error) {
	var input observabilityInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	ctx.Logger().Info("sample observability orchestrator log", "checkpoint", "before-activity")
	info, _ := api.OrchestrationContextInfoFromContext(ctx.Context())
	var result observabilityOutput
	if err := ctx.CallActivity("SampleObservabilityActivity",
		task.WithActivityInput(input),
		task.WithActivityTags(map[string]string{"activity-tag": "echo"}),
		task.WithActivityRetryPolicy(&task.RetryPolicy{
			MaxAttempts:          3,
			InitialRetryInterval: 100 * time.Millisecond,
			BackoffCoefficient:   1,
			MaxRetryInterval:     100 * time.Millisecond,
			Handle: func(ctx task.RetryContext) bool {
				return ctx.LastFailure.IsCausedBy("SampleObservabilityTransient")
			},
		}),
	).Await(&result); err != nil {
		return nil, err
	}
	result.InstanceID = string(info.InstanceID)
	result.OrchestrationName = info.Name
	result.OrchestrationFields = api.ContextFieldsFromContext(ctx.Context())
	return result, nil
}

func observabilityActivity(ctx task.ActivityContext) (any, error) {
	var input observabilityInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	attempt := observabilityAttempts.Increment(input.Key)
	if attempt == 1 {
		return nil, observabilityTransient{key: input.Key}
	}
	task.LoggerFromContext(ctx.Context()).Info("sample observability activity log", "attempt", attempt)
	activity, _ := api.ActivityContextInfoFromContext(ctx.Context())
	return observabilityOutput{
		ActivityName:   activity.Name,
		ActivityFields: api.ContextFieldsFromContext(ctx.Context()),
		Attempts:       attempt,
	}, nil
}

type observabilityInput struct {
	Key string `json:"key"`
}

type observabilityOutput struct {
	InstanceID          string            `json:"instanceId"`
	OrchestrationName   string            `json:"orchestrationName"`
	OrchestrationFields api.ContextFields `json:"orchestrationFields"`
	ActivityName        string            `json:"activityName"`
	ActivityFields      api.ContextFields `json:"activityFields"`
	Attempts            int               `json:"attempts"`
}

type observabilityTransient struct {
	key string
}

func (e observabilityTransient) Error() string {
	return "observability transient failure for " + e.key
}

func (e observabilityTransient) DurableTaskErrorType() api.ErrorType {
	return "SampleObservabilityTransient"
}

type attemptCounter struct {
	mu     sync.Mutex
	counts map[string]int
}

func (c *attemptCounter) Increment(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[key]++
	return c.counts[key]
}

type metricCollector struct {
	mu      sync.Mutex
	retries []task.RetryMetric
	history []task.HistoryMetric
}

func newMetricCollector() *metricCollector {
	return &metricCollector{}
}

func (c *metricCollector) AddRetry(metric task.RetryMetric) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.retries = append(c.retries, metric)
}

func (c *metricCollector) AddHistory(metric task.HistoryMetric) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.history = append(c.history, metric)
}

func (c *metricCollector) Verify(id api.InstanceID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !slices.ContainsFunc(c.retries, func(metric task.RetryMetric) bool {
		return metric.InstanceID == id &&
			metric.TaskKind == task.WorkItemKindActivity &&
			metric.TaskName == "SampleObservabilityActivity" &&
			metric.FailedAttempt == 1 &&
			metric.NextAttempt == 2 &&
			metric.MaxAttempts == 3
	}) {
		return fmt.Errorf("retry metrics missing expected retry: %+v", c.retries)
	}
	if !slices.ContainsFunc(c.history, func(metric task.HistoryMetric) bool {
		return metric.InstanceID == id &&
			metric.OrchestrationName == "SampleObservability" &&
			metric.HistoryLength > 0 &&
			!metric.HistoryLimitExceeded
	}) {
		return fmt.Errorf("history metrics missing expected turn: %+v", c.history)
	}
	return nil
}

type logCollector struct {
	mu      sync.Mutex
	records []logRecord
}

type logRecord struct {
	message string
	attrs   map[string]any
}

func newLogCollector() *logCollector {
	return &logCollector{}
}

func (h *logCollector) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *logCollector) Handle(_ context.Context, record slog.Record) error {
	attrs := make(map[string]any)
	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.Any()
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, logRecord{message: record.Message, attrs: attrs})
	return nil
}

func (h *logCollector) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &logCollectorWith{parent: h, attrs: attrs}
}

func (h *logCollector) WithGroup(string) slog.Handler {
	return h
}

func (h *logCollector) Verify(id api.InstanceID) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	orchestratorLogs := 0
	activityLogs := 0
	for _, record := range h.records {
		if record.message == "sample observability orchestrator log" && record.attrs["durabletask.instance_id"] == string(id) {
			orchestratorLogs++
		}
		if record.message == "sample observability activity log" && record.attrs["durabletask.activity.name"] == "SampleObservabilityActivity" {
			activityLogs++
		}
	}
	if orchestratorLogs != 1 {
		return fmt.Errorf("orchestrator replay-safe log count = %d, want 1; records=%+v", orchestratorLogs, h.records)
	}
	if activityLogs != 1 {
		return fmt.Errorf("activity log count = %d, want 1; records=%+v", activityLogs, h.records)
	}
	return nil
}

type logCollectorWith struct {
	parent *logCollector
	attrs  []slog.Attr
}

func (h *logCollectorWith) Enabled(ctx context.Context, level slog.Level) bool {
	return h.parent.Enabled(ctx, level)
}

func (h *logCollectorWith) Handle(ctx context.Context, record slog.Record) error {
	for _, attr := range h.attrs {
		record.AddAttrs(attr)
	}
	return h.parent.Handle(ctx, record)
}

func (h *logCollectorWith) WithAttrs(attrs []slog.Attr) slog.Handler {
	merged := make([]slog.Attr, 0, len(h.attrs)+len(attrs))
	merged = append(merged, h.attrs...)
	merged = append(merged, attrs...)
	return &logCollectorWith{parent: h.parent, attrs: merged}
}

func (h *logCollectorWith) WithGroup(string) slog.Handler {
	return h
}

func verifyHistoryFields(
	ctx context.Context,
	client interface {
		GetOrchestrationHistory(context.Context, api.InstanceID, api.HistoryQuery) (*api.OrchestrationHistory, error)
	},
	id api.InstanceID,
	executionID string,
	tags map[string]string,
	fields api.ContextFields,
) error {
	history, err := client.GetOrchestrationHistory(ctx, id, api.HistoryQuery{ExecutionID: executionID, MaxEvents: 100, MaxBytes: 1 << 20})
	if err != nil {
		return err
	}
	startIndex := slices.IndexFunc(history.Events, func(event *api.HistoryEvent) bool {
		return event.Type == api.HistoryEventExecutionStarted
	})
	if startIndex < 0 || history.Events[startIndex].ExecutionStarted == nil {
		return errors.New("observability history missing ExecutionStarted")
	}
	started := history.Events[startIndex].ExecutionStarted
	for key, value := range tags {
		if started.Tags[key] != value {
			return fmt.Errorf("ExecutionStarted tag %s = %q, want %q", key, started.Tags[key], value)
		}
	}
	for key, value := range fields {
		if started.ContextFields[key] != value {
			return fmt.Errorf("ExecutionStarted context field %s = %q, want %q", key, started.ContextFields[key], value)
		}
	}
	return nil
}
