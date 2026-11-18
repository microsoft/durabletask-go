package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestTimeoutSemantics(t *testing.T) {
	if sampleHostTimeout != 5*time.Minute {
		t.Fatalf("sampleHostTimeout = %s, want 5m", sampleHostTimeout)
	}
	if nameEventTimeout != 30*time.Second {
		t.Fatalf("nameEventTimeout = %s, want 30s", nameEventTimeout)
	}
}

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

func TestRepeatedEventChannelStopAtCheckpointBoundary(t *testing.T) {
	checkpoint := func(index int, label string) *protos.HistoryEvent {
		payload, err := json.Marshal(checkpointEvent{Index: index, Label: label})
		if err != nil {
			t.Fatal(err)
		}
		return helpers.NewEventRaisedEvent("Checkpoint", wrapperspb.String(string(payload)))
	}
	stop := func() *protos.HistoryEvent {
		return helpers.NewEventRaisedEvent("Stop", nil)
	}
	for _, test := range []struct {
		name        string
		batches     [][]*protos.HistoryEvent
		want        []string
		wantFailure string
	}{
		{
			name: "no stop",
			batches: [][]*protos.HistoryEvent{
				{checkpoint(1, "received")},
				{checkpoint(2, "validated")},
				{checkpoint(3, "approved")},
			},
			want: []string{"01:received", "02:validated", "03:approved"},
		},
		{
			name:    "stop does not interrupt pending checkpoint",
			batches: [][]*protos.HistoryEvent{{stop()}, {checkpoint(1, "received")}},
			want:    []string{"01:received"},
		},
		{
			name:    "stop and checkpoint in same batch",
			batches: [][]*protos.HistoryEvent{{stop(), checkpoint(1, "received")}},
			want:    []string{"01:received"},
		},
		{
			name: "stop after prior work",
			batches: [][]*protos.HistoryEvent{
				{checkpoint(1, "received")},
				{stop()},
				{checkpoint(2, "validated")},
			},
			want: []string{"01:received", "02:validated"},
		},
		{
			name: "invalid final checkpoint surfaces error",
			batches: [][]*protos.HistoryEvent{
				{stop()},
				{helpers.NewEventRaisedEvent("Checkpoint", wrapperspb.String("{invalid"))},
			},
			wantFailure: "failed to decode task result",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := task.NewTaskRegistry()
			if err := registry.AddOrchestratorN("repeated", RepeatedEventChannelOrchestrator); err != nil {
				t.Fatal(err)
			}
			executor := task.NewTaskExecutor(registry)
			history := []*protos.HistoryEvent{
				helpers.NewOrchestratorStartedEvent(),
				helpers.NewExecutionStartedEvent("repeated", "instance", wrapperspb.String("3"), nil, nil, nil),
			}
			first, err := executor.ExecuteOrchestrator(context.Background(), "instance", nil, history, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(first.Response.Actions) != 0 {
				t.Fatalf("initial actions = %v, want pending checkpoint wait", first.Response.Actions)
			}
			for i, batch := range test.batches {
				events := append([]*protos.HistoryEvent{helpers.NewOrchestratorStartedEvent()}, batch...)
				turn, err := executor.ExecuteOrchestrator(context.Background(), "instance", history, events, nil)
				if err != nil {
					t.Fatal(err)
				}
				history = append(history, events...)
				if i < len(test.batches)-1 {
					if len(turn.Response.Actions) != 0 {
						t.Fatalf("batch %d unexpectedly completed: %v", i, turn.Response.Actions)
					}
					continue
				}
				if len(turn.Response.Actions) != 1 || turn.Response.Actions[0].GetCompleteOrchestration() == nil {
					t.Fatalf("final actions = %v, want completion", turn.Response.Actions)
				}
				completed := turn.Response.Actions[0].GetCompleteOrchestration()
				if test.wantFailure != "" {
					if completed.GetOrchestrationStatus() != api.RUNTIME_STATUS_FAILED ||
						!strings.Contains(completed.GetFailureDetails().GetErrorMessage(), test.wantFailure) {
						t.Fatalf("expected failure %q, got %v", test.wantFailure, completed)
					}
					continue
				}
				if completed.GetOrchestrationStatus() != api.RUNTIME_STATUS_COMPLETED {
					t.Fatalf("expected successful completion, got %v", completed)
				}
				var output []string
				if err := json.Unmarshal([]byte(completed.GetResult().GetValue()), &output); err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(output, test.want) {
					t.Fatalf("output = %v, want %v", output, test.want)
				}
			}
		})
	}
}
