package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/exporthistory"
	"google.golang.org/grpc"
)

type failedScheduleConnection struct{ grpc.ClientConnInterface }

func (failedScheduleConnection) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	return context.Canceled
}

func TestSourceIDsSurviveAnAmbiguousSchedulingFailure(t *testing.T) {
	client := &durabletaskscheduler.Client{
		TaskHubGrpcClient: durabletaskclient.NewTaskHubGrpcClient(failedScheduleConnection{}, api.DefaultLogger()),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sources, err := runKnownSources(ctx, client, "cleanup", 3)
	if !errors.Is(err, context.Canceled) || len(sources) != 1 ||
		!strings.HasPrefix(string(sources[0].id), "sample-exporthistory-cleanup-") {
		t.Fatalf("intended cleanup ID was lost: sources=%v err=%v", sources, err)
	}
}

func TestExportRequiresExplicitIsolationAcknowledgement(t *testing.T) {
	t.Setenv("DTS_SAMPLE_ISOLATED_TASKHUB", "")
	options := durabletaskscheduler.NewOptions("https://example.invalid", "sample-shared")
	if requireIsolatedTaskHub(options) == nil {
		t.Fatal("a sample-looking name is not proof of isolation")
	}
	t.Setenv("DTS_SAMPLE_ISOLATED_TASKHUB", "1")
	if err := requireIsolatedTaskHub(options); err != nil {
		t.Fatal(err)
	}
}

func TestDecodeHistoryObjectJSONAndJSONL(t *testing.T) {
	events := []api.HistoryEvent{{Type: api.HistoryEventExecutionStarted}, {Type: api.HistoryEventExecutionCompleted}}
	jsonBody, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeHistoryObject(jsonBody, exporthistory.ExportFormatJSON)
	if err != nil || len(decoded) != 2 {
		t.Fatalf("decode JSON len=%d err=%v", len(decoded), err)
	}

	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write([]byte(`{"type":"ExecutionStarted"}` + "\n" + `{"type":"ExecutionCompleted"}` + "\n")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	decoded, err = decodeHistoryObject(compressed.Bytes(), exporthistory.ExportFormatJSONL)
	if err != nil || len(decoded) != 2 {
		t.Fatalf("decode JSONL len=%d err=%v", len(decoded), err)
	}
}

func TestSelectScenarios(t *testing.T) {
	all, err := selectScenarios("all")
	if err != nil || len(all) != 4 {
		t.Fatalf("all scenarios len=%d err=%v", len(all), err)
	}
	one, err := selectScenarios("continuous-json")
	if err != nil || len(one) != 1 || one[0].name != "continuous-json" {
		t.Fatalf("continuous-json scenarios=%v err=%v", one, err)
	}
	if _, err := selectScenarios("missing"); err == nil {
		t.Fatal("expected unknown scenario to fail")
	}
}
