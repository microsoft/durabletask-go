package exporthistory

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type streamingSource struct {
	*fakeSource
	stream func(context.Context, api.HistoryEventHandler) error
}

func (s streamingSource) StreamOrchestrationHistory(ctx context.Context, _ api.InstanceID, _ api.HistoryQuery, handler api.HistoryEventHandler) error {
	return s.stream(ctx, handler)
}

type streamingStore func(context.Context, ExportObject) error

func (s streamingStore) Write(ctx context.Context, object ExportObject) error { return s(ctx, object) }

func streamTestRequest() ExportRequest {
	return ExportRequest{
		InstanceID: "subject", Destination: ExportDestination{Container: "container"}, Format: DefaultExportFormat(),
	}
}

func streamTestSource() *fakeSource {
	source := newFakeSource()
	source.addInstance("subject", api.RUNTIME_STATUS_COMPLETED, 1)
	return source
}

func TestExportStreamingFailuresNeverPublish(t *testing.T) {
	sentinel := errors.New("source unavailable")
	for _, format := range []ExportFormatKind{ExportFormatJSON, ExportFormatJSONL} {
		for _, failure := range []error{sentinel, io.EOF, io.ErrUnexpectedEOF} {
			t.Run(format.String()+"/"+failure.Error(), func(t *testing.T) {
				base := streamTestSource()
				source := streamingSource{fakeSource: base, stream: func(_ context.Context, handler api.HistoryEventHandler) error {
					if err := handler(base.history["subject"].Events[0]); err != nil {
						return err
					}
					return failure
				}}
				store := newMemoryStore()
				request := streamTestRequest()
				request.Format.Kind = format
				result, err := newTestRuntime(source, store).exportInstance(context.Background(), request)
				require.ErrorIs(t, err, failure)
				assert.False(t, result.Success)
				assert.Zero(t, store.count(), "partial output must never become visible")
			})
		}
	}
}

func TestExportStreamingCancelsBothDirections(t *testing.T) {
	t.Run("upload failure cancels a blocked source", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		entered := make(chan struct{})
		stopped := make(chan struct{})
		source := streamingSource{fakeSource: streamTestSource(), stream: func(ctx context.Context, _ api.HistoryEventHandler) error {
			close(entered)
			<-ctx.Done()
			close(stopped)
			return ctx.Err()
		}}
		failure := errors.New("upload rejected")
		store := streamingStore(func(context.Context, ExportObject) error {
			<-entered
			return failure
		})
		_, err := newTestRuntime(source, store).exportInstance(ctx, streamTestRequest())
		require.ErrorIs(t, err, failure)
		require.NoError(t, ctx.Err(), "producer must be canceled without waiting for the outer deadline")
		<-stopped
	})

	t.Run("source failure cancels a blocked uploader", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		failure := errors.New("source failed")
		source := streamingSource{fakeSource: streamTestSource(), stream: func(context.Context, api.HistoryEventHandler) error {
			return failure
		}}
		store := streamingStore(func(ctx context.Context, _ ExportObject) error {
			<-ctx.Done()
			return ctx.Err()
		})
		_, err := newTestRuntime(source, store).exportInstance(ctx, streamTestRequest())
		require.ErrorIs(t, err, failure)
		require.NoError(t, ctx.Err())
	})

	t.Run("outer cancellation joins both sides", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		entered := make(chan struct{})
		source := streamingSource{fakeSource: streamTestSource(), stream: func(ctx context.Context, _ api.HistoryEventHandler) error {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}}
		finished := make(chan error, 1)
		go func() {
			_, err := newTestRuntime(source, newMemoryStore()).exportInstance(ctx, streamTestRequest())
			finished <- err
		}()
		<-entered
		cancel()
		select {
		case err := <-finished:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("canceled stream leaked its producer or uploader")
		}
	})
}

func TestExportStreamingRetryReopensHistory(t *testing.T) {
	base := streamTestSource()
	reads := 0
	source := streamingSource{fakeSource: base, stream: func(_ context.Context, handler api.HistoryEventHandler) error {
		reads++
		return handler(base.history["subject"].Events[0])
	}}
	store := newMemoryStore()
	failure := errors.New("transient upload failure")
	store.failure = failure
	exporter := newTestRuntime(source, store)
	_, err := exporter.exportInstance(context.Background(), streamTestRequest())
	require.ErrorIs(t, err, failure)
	store.failure = nil
	result, err := exporter.exportInstance(context.Background(), streamTestRequest())
	require.NoError(t, err)
	require.True(t, result.Success)
	require.Equal(t, 2, reads)
	require.Equal(t, 1, result.EventCount)
	require.Equal(t, 1, store.count())
}

func TestExportStreamingLimits(t *testing.T) {
	for _, test := range []struct {
		name  string
		query api.HistoryQuery
	}{
		{"events", api.HistoryQuery{MaxEvents: 1}},
		{"bytes", api.HistoryQuery{MaxBytes: 1024}},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := streamTestSource()
			source.history["subject"].Events = append(source.history["subject"].Events,
				&api.HistoryEvent{Generic: &api.HistoryPayloadEvent{SerializedInput: strings.Repeat("x", 4096)}})
			store := newMemoryStore()
			exporter := newTestRuntime(source, store)
			exporter.historyPage = test.query
			result, err := exporter.exportInstance(context.Background(), streamTestRequest())
			require.ErrorIs(t, err, api.ErrHistoryLimitExceeded)
			require.False(t, result.Success)
			require.Zero(t, store.count())
		})
	}
}

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestExportStreamingCloseFailure(t *testing.T) {
	failure := errors.New("gzip trailer write failed")
	// With no events, gzip writes its header/trailer only from Close.
	_, err := writeHistory(context.Background(), failingWriter{failure}, DefaultExportFormat(),
		api.HistoryQuery{}, func(api.HistoryEventHandler) error { return nil })
	require.ErrorIs(t, err, failure)
	require.ErrorContains(t, err, "finish export compression")
}

func TestExportStreamingExactJSONShape(t *testing.T) {
	source := streamTestSource()
	source.history["subject"].Events = append(source.history["subject"].Events,
		&api.HistoryEvent{Generic: &api.HistoryPayloadEvent{SerializedInput: `"Unicode: 日本語 <&>"`}})
	for _, format := range []ExportFormatKind{ExportFormatJSON, ExportFormatJSONL} {
		request := streamTestRequest()
		request.Format.Kind = format
		var output bytes.Buffer
		store := streamingStore(func(_ context.Context, object ExportObject) error {
			_, err := io.Copy(&output, object.Content)
			return err
		})
		result, err := newTestRuntime(source, store).exportInstance(context.Background(), request)
		require.NoError(t, err)
		require.Equal(t, 2, result.EventCount)
		if format == ExportFormatJSON {
			expected, err := json.Marshal(source.history["subject"].Events)
			require.NoError(t, err)
			require.Equal(t, expected, output.Bytes())
		} else {
			var expected bytes.Buffer
			for _, event := range source.history["subject"].Events {
				encoded, err := json.Marshal(event)
				require.NoError(t, err)
				expected.Write(encoded)
				expected.WriteByte('\n')
			}
			require.Equal(t, expected.Bytes(), decompressGzip(t, output.Bytes()))
		}
	}
}

func TestExportStreamingMemoryDoesNotScaleWithHistory(t *testing.T) {
	for _, format := range []ExportFormatKind{ExportFormatJSON, ExportFormatJSONL} {
		t.Run(format.String(), func(t *testing.T) {
			measure := func(events int) uint64 {
				runtime.GC()
				var initial runtime.MemStats
				runtime.ReadMemStats(&initial)
				peak := initial.HeapAlloc
				base := streamTestSource()
				source := streamingSource{fakeSource: base, stream: func(_ context.Context, handler api.HistoryEventHandler) error {
					if err := handler(base.history["subject"].Events[0]); err != nil {
						return err
					}
					for i := 0; i < events; i++ {
						event := &api.HistoryEvent{
							Generic: &api.HistoryPayloadEvent{SerializedInput: strings.Repeat("x", 16*1024)},
						}
						if err := handler(event); err != nil {
							return err
						}
						if i%128 == 0 || i == events-1 {
							runtime.GC()
							var sample runtime.MemStats
							runtime.ReadMemStats(&sample)
							peak = max(peak, sample.HeapAlloc)
						}
					}
					return nil
				}}
				store := streamingStore(func(_ context.Context, object ExportObject) error {
					_, err := io.Copy(io.Discard, object.Content)
					return err
				})
				exporter := newTestRuntime(source, store)
				exporter.historyPage.MaxBytes = api.MaxHistoryMaxBytes
				request := streamTestRequest()
				request.Format.Kind = format
				result, err := exporter.exportInstance(context.Background(), request)
				require.NoError(t, err)
				require.Equal(t, events+1, result.EventCount)
				return peak - initial.HeapAlloc
			}
			small, large := measure(128), measure(4096)
			t.Logf("retained heap growth: 2 MiB history=%d, 64 MiB history=%d", small, large)
			require.Less(t, large, small+8*1024*1024, "64 MiB of history must not be retained")
		})
	}
}
