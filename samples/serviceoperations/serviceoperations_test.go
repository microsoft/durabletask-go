package main

import (
	"strings"
	"testing"
)

func TestRejectsUnsafeHubBeforeConnecting(t *testing.T) {
	for _, test := range []struct {
		name string
		hub  string
		ack  string
	}{
		{name: "no acknowledgement", hub: "sample-disposable"},
		{name: "shared hub", hub: "tests", ack: "1"},
		{name: "invalid acknowledgement", hub: "sample-disposable", ack: "true"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("DTS_CONNECTION_STRING", "Endpoint=http://127.0.0.1:1;TaskHub="+test.hub+";Authentication=None")
			t.Setenv("DTS_SAMPLE_ALLOW_HUB_MAINTENANCE", test.ack)
			err := run()
			if err == nil || !strings.Contains(err.Error(), "disposable task hub") {
				t.Fatalf("expected safety rejection before any connection: %v", err)
			}
		})
	}
}
