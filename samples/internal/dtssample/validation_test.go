package dtssample

import (
	"strings"
	"testing"

	"github.com/microsoft/durabletask-go/api"
)

func TestRequireCompleted(t *testing.T) {
	if RequireCompleted(nil) == nil {
		t.Fatal("missing metadata must fail")
	}
	for _, status := range []api.OrchestrationStatus{
		api.RUNTIME_STATUS_PENDING, api.RUNTIME_STATUS_RUNNING,
		api.RUNTIME_STATUS_SUSPENDED, api.RUNTIME_STATUS_FAILED,
		api.RUNTIME_STATUS_CANCELED, api.RUNTIME_STATUS_TERMINATED,
		api.RUNTIME_STATUS_CONTINUED_AS_NEW, api.RUNTIME_STATUS_COMPLETED,
	} {
		err := RequireCompleted(&api.OrchestrationMetadata{InstanceID: "sample", RuntimeStatus: status})
		if (err == nil) != (status == api.RUNTIME_STATUS_COMPLETED) {
			t.Errorf("status %s: unexpected error %v", status, err)
		}
	}
}

func TestNewInstanceID(t *testing.T) {
	first := NewInstanceID("example")
	second := NewInstanceID("example")
	if !strings.HasPrefix(string(first), "sample-example-") || first == second {
		t.Fatalf("invalid sample IDs: %q, %q", first, second)
	}
}
