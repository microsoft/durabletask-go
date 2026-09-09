package api

import (
	"context"
	"maps"
)

// ReservedContextFieldPrefix is reserved for Durable Task runtime identity tags.
const ReservedContextFieldPrefix = "__durabletask.context."

// ContextFields are immutable caller-supplied values propagated into task contexts.
type ContextFields map[string]string

// OrchestrationContextInfo identifies the orchestration associated with a task context.
type OrchestrationContextInfo struct {
	InstanceID       InstanceID
	Name             string
	Version          string
	ParentInstanceID InstanceID
}

// ActivityContextInfo identifies the activity associated with a task context.
type ActivityContextInfo struct {
	InstanceID InstanceID
	Name       string
	Version    string
	TaskID     int32
}

// EntityContextInfo identifies the entity operation associated with a task context.
type EntityContextInfo struct {
	EntityID  EntityID
	Operation string
	RequestID string
	IsSignal  bool
}

type orchestrationContextInfoKey struct{}
type activityContextInfoKey struct{}
type entityContextInfoKey struct{}
type contextFieldsKey struct{}

// WithOrchestrationContextInfo returns a context containing orchestration identity.
func WithOrchestrationContextInfo(ctx context.Context, info OrchestrationContextInfo) context.Context {
	return context.WithValue(ctx, orchestrationContextInfoKey{}, info)
}

// OrchestrationContextInfoFromContext returns orchestration identity from ctx.
func OrchestrationContextInfoFromContext(ctx context.Context) (OrchestrationContextInfo, bool) {
	info, ok := ctx.Value(orchestrationContextInfoKey{}).(OrchestrationContextInfo)
	return info, ok
}

// WithActivityContextInfo returns a context containing activity identity.
func WithActivityContextInfo(ctx context.Context, info ActivityContextInfo) context.Context {
	return context.WithValue(ctx, activityContextInfoKey{}, info)
}

// ActivityContextInfoFromContext returns activity identity from ctx.
func ActivityContextInfoFromContext(ctx context.Context) (ActivityContextInfo, bool) {
	info, ok := ctx.Value(activityContextInfoKey{}).(ActivityContextInfo)
	return info, ok
}

// WithEntityContextInfo returns a context containing entity operation identity.
func WithEntityContextInfo(ctx context.Context, info EntityContextInfo) context.Context {
	return context.WithValue(ctx, entityContextInfoKey{}, info)
}

// EntityContextInfoFromContext returns entity operation identity from ctx.
func EntityContextInfoFromContext(ctx context.Context) (EntityContextInfo, bool) {
	info, ok := ctx.Value(entityContextInfoKey{}).(EntityContextInfo)
	return info, ok
}

// ContextWithFields merges fields with any fields already in ctx, with fields
// taking precedence on conflicts. The stored map is a defensive copy.
func ContextWithFields(ctx context.Context, fields ContextFields) context.Context {
	if len(fields) == 0 {
		return ctx
	}
	merged := ContextFieldsFromContext(ctx)
	if merged == nil {
		merged = make(ContextFields, len(fields))
	}
	maps.Copy(merged, fields)
	return context.WithValue(ctx, contextFieldsKey{}, merged)
}

// ContextFieldsFromContext returns a defensive copy of caller-supplied fields.
func ContextFieldsFromContext(ctx context.Context) ContextFields {
	fields, ok := ctx.Value(contextFieldsKey{}).(ContextFields)
	if !ok || len(fields) == 0 {
		return nil
	}
	return maps.Clone(fields)
}
