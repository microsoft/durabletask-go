// Package logging exercises the replay-unsafe logging checks and the
// replay-safe orchestration logger that replaces them.
package logging

import (
	"fmt"
	"log"
	"log/slog"
	"os"

	"github.com/microsoft/durabletask-go/task"
)

func standardLog(ctx *task.OrchestrationContext) (any, error) {
	log.Print("starting")    // want `log\.Print writes on every replay`
	log.Printf("step %d", 1) // want `log\.Printf writes on every replay`
	log.Println("done")      // want `log\.Println writes on every replay`
	custom := log.New(os.Stderr, "orchestration ", 0)
	custom.Printf("step %d", 2) // want `\(\*log\.Logger\)\.Printf writes on every replay`
	return nil, nil
}

func globalSlog(ctx *task.OrchestrationContext) (any, error) {
	slog.Info("starting")              // want `slog\.Info writes on every replay`
	slog.Warn("slow")                  // want `slog\.Warn writes on every replay`
	slog.Error("failed", "attempt", 1) // want `slog\.Error writes on every replay`
	slog.Default().Info("also global") // want `slog\.Default reaches the process-wide logger, which writes on every replay`
	replacement := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(replacement) // want `slog\.SetDefault reaches the process-wide logger, which writes on every replay`
	return nil, nil
}

func standardStreams(ctx *task.OrchestrationContext) (any, error) {
	fmt.Println("starting")                // want `fmt\.Println writes on every replay`
	fmt.Printf("step %d\n", 1)             // want `fmt\.Printf writes on every replay`
	fmt.Fprintf(os.Stdout, "step %d\n", 2) // want `fmt\.Fprintf writes on every replay`
	fmt.Fprintln(os.Stderr, "warning")     // want `fmt\.Fprintln writes on every replay`
	return nil, nil
}

// replaySafeLogging uses the orchestration logger and pure formatting.
func replaySafeLogging(ctx *task.OrchestrationContext) (any, error) {
	logger := ctx.Logger()
	logger.Info("starting", "instance", ctx.ID)
	logger.With("stage", "two").Debug("progress")
	ctx.Logger().Error("failed", "attempt", 1)
	message := fmt.Sprintf("instance %s", ctx.ID)
	ctx.SetCustomStatus(message)
	return message, nil
}

func register() {
	registry := task.NewTaskRegistry()
	_ = registry.AddOrchestrator(standardLog)
	_ = registry.AddOrchestrator(globalSlog)
	_ = registry.AddOrchestrator(standardStreams)
	_ = registry.AddOrchestrator(replaySafeLogging)
}
