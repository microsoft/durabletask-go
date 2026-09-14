package bad

import (
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/task"
)

func Register(registry *task.TaskRegistry) error {
	return registry.AddOrchestratorN("ReplayAnalysisUnsafe", replayAnalysisUnsafe)
}

func replayAnalysisUnsafe(ctx *task.OrchestrationContext) (any, error) {
	fmt.Println("printing from an orchestrator is replay-unsafe")
	_, _ = http.Get("http://example.invalid")
	ch := make(chan string, 1)
	go func() {
		ch <- uuid.NewString()
	}()
	select {
	case value := <-ch:
		return time.Now().String() + value, nil
	default:
		return time.Now().String(), nil
	}
}
