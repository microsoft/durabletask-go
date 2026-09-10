package task

import (
	"testing"

	"github.com/microsoft/durabletask-go/internal/protos"
)

func TestCompletedTaskIgnoresLaterCancellationFailureAndCompletion(t *testing.T) {
	ctx := newTestOrchestrationContext(NewTaskRegistry(), "completed-task", nil, nil)
	scope := newCancellationScope(nil)
	task := newTaskInScope(ctx, scope)
	task.complete([]byte(`"first"`))

	scope.cancel(nil)
	task.fail(&protos.TaskFailureDetails{ErrorMessage: "late failure"})
	task.complete([]byte(`"second"`))

	var result string
	if err := task.Await(&result); err != nil {
		t.Fatal(err)
	}
	if result != "first" {
		t.Fatalf("result = %q, want first", result)
	}
}
