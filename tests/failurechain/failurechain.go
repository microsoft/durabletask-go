// Package failurechain provides a shared, table-driven assertion for durable
// failure chains, so the Durable Task Scheduler tests validate the same
// cross-language contract the other language SDKs do.
package failurechain

import (
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/stretchr/testify/require"
)

// LeafError is an application error that opts into every durable failure
// enrichment hook: a stable cross-language error type, a stack trace, custom
// properties, and the non-retriable marker. Tests reuse it so the same leaf
// contract is exercised against a live service.
type LeafError struct {
	Message    string
	ErrorType  api.ErrorType
	Stack      string
	Properties map[string]any
	// IsNonRetriable backs the NonRetriable marker method, which cannot share
	// the field's name.
	IsNonRetriable bool
}

func (e *LeafError) Error() string { return e.Message }

func (e *LeafError) DurableTaskErrorType() api.ErrorType { return e.ErrorType }

func (e *LeafError) DurableTaskStackTrace() string { return e.Stack }

func (e *LeafError) DurableTaskErrorProperties() map[string]any { return e.Properties }

// NonRetriable reports the marker the durable retry policy consults.
func (e *LeafError) NonRetriable() bool { return e.IsNonRetriable }

var (
	_ api.DurableTaskErrorTypeProvider       = (*LeafError)(nil)
	_ api.DurableTaskStackTraceProvider      = (*LeafError)(nil)
	_ api.DurableTaskErrorPropertiesProvider = (*LeafError)(nil)
	_ api.NonRetriable                       = (*LeafError)(nil)
)

// Frame describes one expected frame of a durable failure chain.
type Frame struct {
	ErrorType       api.ErrorType
	MessageContains []string
	// StackContains is only checked when ExpectStack is true.
	StackContains string
	ExpectStack   bool
	NonRetriable  bool
	// Properties are checked by key. A nil map asserts the frame carries no
	// properties at all.
	Properties map[string]any
}

// Assert walks a failure chain and asserts it matches frames exactly, including
// that the chain is neither shorter nor deeper than expected.
func Assert(t *testing.T, details *api.FailureDetails, frames []Frame) {
	t.Helper()
	current := details
	for i, frame := range frames {
		require.NotNilf(t, current, "failure chain ended before frame %d", i)
		require.Equalf(t, frame.ErrorType, current.ErrorType, "frame %d error type", i)
		for _, fragment := range frame.MessageContains {
			require.Containsf(t, current.ErrorMessage, fragment, "frame %d message", i)
		}
		require.Equalf(t, frame.NonRetriable, current.IsNonRetriable, "frame %d non-retriable flag", i)
		if frame.ExpectStack {
			require.NotEmptyf(t, current.StackTrace, "frame %d stack trace", i)
			if frame.StackContains != "" {
				require.Containsf(t, current.StackTrace, frame.StackContains, "frame %d stack trace", i)
			}
		} else {
			require.Emptyf(t, current.StackTrace, "frame %d stack trace", i)
		}
		if frame.Properties == nil {
			require.Emptyf(t, current.Properties, "frame %d properties", i)
		} else {
			for key, want := range frame.Properties {
				require.Equalf(t, want, current.Properties[key], "frame %d property %q", i, key)
			}
		}
		current = current.InnerFailure
	}
	require.Nil(t, current, "failure chain is deeper than expected")
}
