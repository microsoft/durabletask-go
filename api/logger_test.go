package api

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewSlogLoggerRoutesEveryLevel pins the adapter applications use to route
// SDK logging into their own slog handler: every Logger method must reach slog
// at the matching level, and the formatting variants must apply the format.
func TestNewSlogLoggerRoutesEveryLevel(t *testing.T) {
	for _, test := range []struct {
		name  string
		level slog.Level
		log   func(Logger)
		want  string
	}{
		{"Debug", slog.LevelDebug, func(l Logger) { l.Debug("a", "b") }, "ab"},
		{"Debugf", slog.LevelDebug, func(l Logger) { l.Debugf("a=%d", 1) }, "a=1"},
		{"Info", slog.LevelInfo, func(l Logger) { l.Info("a", "b") }, "ab"},
		{"Infof", slog.LevelInfo, func(l Logger) { l.Infof("a=%d", 1) }, "a=1"},
		{"Warn", slog.LevelWarn, func(l Logger) { l.Warn("a", "b") }, "ab"},
		{"Warnf", slog.LevelWarn, func(l Logger) { l.Warnf("a=%d", 1) }, "a=1"},
		{"Error", slog.LevelError, func(l Logger) { l.Error("a", "b") }, "ab"},
		{"Errorf", slog.LevelError, func(l Logger) { l.Errorf("a=%d", 1) }, "a=1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer
			var recorded []slog.Record
			handler := &recordingHandler{buf: &buf, records: &recorded}
			test.log(NewSlogLogger(slog.New(handler)))

			require.Len(t, recorded, 1)
			assert.Equal(t, test.level, recorded[0].Level)
			assert.Equal(t, test.want, recorded[0].Message)
		})
	}
}

// TestDefaultLoggerImplementsEveryLevel keeps the default logger usable as the
// zero-configuration option callers pass to NewClient and NewWorker.
func TestDefaultLoggerImplementsEveryLevel(t *testing.T) {
	log := DefaultLogger()
	require.NotNil(t, log)
	assert.NotPanics(t, func() {
		log.Debug("d")
		log.Debugf("d=%d", 1)
		log.Info("i")
		log.Infof("i=%d", 1)
		log.Warn("w")
		log.Warnf("w=%d", 1)
		log.Error("e")
		log.Errorf("e=%d", 1)
	})
}

type recordingHandler struct {
	buf     *bytes.Buffer
	records *[]slog.Record
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, record slog.Record) error {
	*h.records = append(*h.records, record)
	h.buf.WriteString(record.Message)
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *recordingHandler) WithGroup(string) slog.Handler { return h }
