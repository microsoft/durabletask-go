// Package failurechain supplies an enriched application error for Durable Task
// Scheduler failure-chain tests.
package failurechain

import "github.com/microsoft/durabletask-go/api"

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
