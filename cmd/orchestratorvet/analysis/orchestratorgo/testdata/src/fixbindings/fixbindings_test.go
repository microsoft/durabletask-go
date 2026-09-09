package fixbindings

import (
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/task"
)

func TestContextBindingsRemainSafe(t *testing.T) {
	for name, orchestrator := range map[string]task.Orchestrator{
		"same type shadow": sameTypeShadow,
		"reassigned":       reassignedContext,
	} {
		t.Run(name, func(t *testing.T) {
			value, err := orchestrator(&task.OrchestrationContext{})
			if err != nil {
				t.Fatal(err)
			}
			if clock, ok := value.(time.Time); !ok || clock.IsZero() {
				t.Fatalf("unfixable clock read changed: %v", value)
			}
		})
	}
}
