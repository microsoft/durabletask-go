package task

import (
	"context"
	"log/slog"
	"maps"

	"github.com/microsoft/durabletask-go/api"
)

type taskLoggerKey struct{}

// mergeStringMaps returns base overlaid with overrides, or nil when both are
// empty. The result never aliases either input.
func mergeStringMaps[M ~map[string]string](base, overrides M) M {
	if len(base) == 0 && len(overrides) == 0 {
		return nil
	}
	merged := make(M, len(base)+len(overrides))
	maps.Copy(merged, base)
	maps.Copy(merged, overrides)
	return merged
}

// Context returns a replay-stable Go context containing only persisted
// orchestration identity and caller fields. Host values, deadlines, and
// cancellation are intentionally excluded from deterministic orchestrator code.
func (ctx *OrchestrationContext) Context() context.Context {
	engine := ctx.engineContext()
	base := api.ContextWithFields(context.Background(), engine.contextFields)
	return api.WithOrchestrationContextInfo(base, api.OrchestrationContextInfo{
		InstanceID:       engine.ID,
		Name:             engine.Name,
		Version:          engine.Version,
		ParentInstanceID: engine.parentInstanceID,
	})
}

// Logger returns a slog logger that suppresses output while replaying history.
func (ctx *OrchestrationContext) Logger() *slog.Logger {
	engine := ctx.engineContext()
	logger := engine.logger
	if logger == nil {
		logger = slog.Default()
	}
	handler := &replaySafeHandler{
		handler: logger.Handler(),
		replaying: func() bool {
			return engine.IsReplaying
		},
	}
	return slog.New(handler).With(
		slog.String("durabletask.instance_id", string(engine.ID)),
		slog.String("durabletask.orchestration.name", engine.Name),
		slog.String("durabletask.orchestration.version", engine.Version),
	)
}

// LoggerFromContext returns the task logger associated with an activity or
// entity context, or slog.Default. Orchestrators must use
// [OrchestrationContext.Logger]; their replay-stable Context excludes loggers.
func LoggerFromContext(ctx context.Context) *slog.Logger {
	if logger, ok := ctx.Value(taskLoggerKey{}).(*slog.Logger); ok && logger != nil {
		return logger
	}
	return slog.Default()
}

func withActivityLogger(ctx context.Context, logger *slog.Logger) context.Context {
	if logger == nil {
		logger = slog.Default()
	}
	attrs := make([]any, 0, 10)
	if orchestration, ok := api.OrchestrationContextInfoFromContext(ctx); ok {
		attrs = append(attrs,
			slog.String("durabletask.instance_id", string(orchestration.InstanceID)),
			slog.String("durabletask.orchestration.name", orchestration.Name),
			slog.String("durabletask.orchestration.version", orchestration.Version),
		)
	}
	if activity, ok := api.ActivityContextInfoFromContext(ctx); ok {
		attrs = append(attrs,
			slog.String("durabletask.activity.name", activity.Name),
			slog.String("durabletask.activity.version", activity.Version),
			slog.Int("durabletask.activity.task_id", int(activity.TaskID)),
		)
	}
	return context.WithValue(ctx, taskLoggerKey{}, logger.With(attrs...))
}

type replaySafeHandler struct {
	handler   slog.Handler
	replaying func() bool
}

func (h *replaySafeHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return !h.replaying() && h.handler.Enabled(ctx, level)
}

func (h *replaySafeHandler) Handle(ctx context.Context, record slog.Record) error {
	if h.replaying() {
		return nil
	}
	return h.handler.Handle(ctx, record)
}

func (h *replaySafeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &replaySafeHandler{handler: h.handler.WithAttrs(attrs), replaying: h.replaying}
}

func (h *replaySafeHandler) WithGroup(name string) slog.Handler {
	return &replaySafeHandler{handler: h.handler.WithGroup(name), replaying: h.replaying}
}
