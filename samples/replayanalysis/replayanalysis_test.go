package main

import (
	"testing"
	"time"
)

func TestValidateReplayOutput(t *testing.T) {
	err := validateReplayOutput(replayOutput{
		StartedAt:   time.Unix(1, 0).UTC(),
		DurableID:   "guid",
		ActivityOut: "activity:safe-counterpart|guid",
	})
	if err != nil {
		t.Fatalf("validateReplayOutput failed: %v", err)
	}
	if err := validateReplayOutput(replayOutput{}); err == nil {
		t.Fatal("expected empty output to fail")
	}
	if err := validateReplayOutput(replayOutput{
		StartedAt: time.Unix(1, 0).UTC(), DurableID: "guid",
		ActivityOut: "activity:safe-counterpart|different-guid",
	}); err == nil {
		t.Fatal("activity and orchestration identifiers must agree")
	}
}
