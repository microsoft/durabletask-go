package api

import (
	"testing"

	"github.com/microsoft/durabletask-go/internal/protos"
)

func TestWithContextFieldsRejectsReservedKeys(t *testing.T) {
	option := WithContextFields(ContextFields{
		ReservedContextFieldPrefix + "orchestration_version": "spoofed",
	})
	if err := option(new(protos.CreateInstanceRequest), DefaultDataConverter()); err == nil {
		t.Fatal("expected reserved context field to be rejected")
	}
}
