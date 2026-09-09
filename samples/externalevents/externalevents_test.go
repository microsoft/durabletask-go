package main

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
)

func TestReadAndRaiseEvent(t *testing.T) {
	readError := errors.New("input unavailable")
	raiseError := errors.New("event rejected")
	for _, test := range []struct {
		name      string
		input     io.Reader
		raiseErr  error
		wantErr   error
		wantStage string
		wantRaise bool
	}{
		{name: "EOF", input: strings.NewReader(""), wantErr: io.EOF, wantStage: "read input"},
		{name: "read error", input: iotest.ErrReader(readError), wantErr: readError, wantStage: "read input"},
		{name: "raise error", input: strings.NewReader("Taylor\n"), raiseErr: raiseError, wantErr: raiseError, wantStage: "raise event", wantRaise: true},
		{name: "success", input: strings.NewReader("Taylor\n"), wantRaise: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			raised := false
			readAndRaiseEvent(ctx, cancel, test.input, func(callCtx context.Context, name string) error {
				raised = true
				if callCtx != ctx || name != "Taylor" {
					t.Fatalf("unexpected event context or name %q", name)
				}
				return test.raiseErr
			})
			if raised != test.wantRaise {
				t.Fatalf("event raised=%t, want %t", raised, test.wantRaise)
			}
			cause := context.Cause(ctx)
			if !errors.Is(cause, test.wantErr) {
				t.Fatalf("cancellation cause=%v, want %v", cause, test.wantErr)
			}
			if test.wantErr != nil {
				select {
				case <-ctx.Done():
				default:
					t.Fatal("input failure did not interrupt the completion context")
				}
				if !strings.Contains(cause.Error(), test.wantStage) {
					t.Fatalf("error %q does not describe %q", cause, test.wantStage)
				}
			} else if ctx.Err() != nil {
				t.Fatalf("successful input canceled the completion wait: %v", ctx.Err())
			}
		})
	}
}
