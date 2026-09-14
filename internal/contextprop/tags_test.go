package contextprop

import (
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/tagcodec"
)

func TestEncodeDecode(t *testing.T) {
	tags := Encode(api.OrchestrationContextInfo{
		InstanceID:       "instance",
		Name:             "orchestration",
		Version:          "v1",
		ParentInstanceID: "parent",
	}, api.ContextFields{"tenant": "alpha"})

	info, fields := Decode(tags)
	if info.InstanceID != "instance" ||
		info.Name != "orchestration" ||
		info.Version != "v1" ||
		info.ParentInstanceID != "parent" {
		t.Fatalf("unexpected info: %+v", info)
	}

	if fields["tenant"] != "alpha" {
		t.Fatalf("tenant = %q, want alpha", fields["tenant"])
	}
}

func TestEncodeSeparatesContextFieldsAndUserTags(t *testing.T) {
	tags := Encode(
		api.OrchestrationContextInfo{},
		api.ContextFields{"tenant": "context"},
		map[string]string{"team": "tag"},
	)
	_, fields := Decode(tags)
	if fields["tenant"] != "context" {
		t.Fatalf("tenant = %q, want context", fields["tenant"])
	}
	userTags := tagcodec.DecodeUserTags(tags)
	if userTags["team"] != "tag" {
		t.Fatalf("team = %q, want tag", userTags["team"])
	}
	if _, ok := fields["team"]; ok {
		t.Fatalf("user tag leaked into context fields: %v", fields)
	}
}

func TestEncodeOverwritesReservedCallerFields(t *testing.T) {
	tags := Encode(api.OrchestrationContextInfo{}, api.ContextFields{
		api.ReservedContextFieldPrefix + "orchestration_version": "spoofed",
	})
	info, fields := Decode(tags)
	if info.Version != "" {
		t.Fatalf("version = %q, want empty", info.Version)
	}
	if fields != nil {
		t.Fatalf("reserved field leaked into caller fields: %v", fields)
	}
}
