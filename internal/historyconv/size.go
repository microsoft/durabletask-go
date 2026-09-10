package historyconv

import (
	"reflect"

	"github.com/microsoft/durabletask-go/api"
)

// Saturating above every valid query budget also works on 32-bit platforms.
const maxEstimatedSize = api.MaxHistoryMaxBytes + 1
const maxPropertyDepth = 64

func addSize(size int, amounts ...int) int {
	if size >= maxEstimatedSize {
		return maxEstimatedSize
	}
	for _, amount := range amounts {
		if amount >= maxEstimatedSize-size {
			return maxEstimatedSize
		}
		size += amount
	}
	return size
}

func multiplySize(count, size int) int {
	if size == 0 {
		return 0
	}
	if count > maxEstimatedSize/size {
		return maxEstimatedSize
	}
	return count * size
}

// ApproximateEventSize estimates retained history content without serializing
// it. Shared converters are excluded; event/detail structs use fixed allowances.
// Cyclic or excessively nested values fail closed above every valid byte limit.
func ApproximateEventSize(event *api.HistoryEvent) int {
	if event == nil {
		return 0
	}
	size := addSize(512, len(event.Type), len(event.UnknownType))
	if value := event.ExecutionStarted; value != nil {
		size = addSize(size, len(value.Name), len(value.Version), len(value.InstanceID),
			len(value.ExecutionID), len(value.SerializedInput), len(value.OrchestrationSpanID),
			mapSize(value.Tags), mapSize(value.ContextFields), parentSize(value.Parent), traceSize(value.ParentTraceContext))
	}
	if value := event.ExecutionCompleted; value != nil {
		size = addSize(size, len(value.SerializedResult), failureSize(value.FailureDetails))
	}
	if value := event.ExecutionTerminated; value != nil {
		size = addSize(size, len(value.SerializedInput))
	}
	if value := event.TaskScheduled; value != nil {
		size = addSize(size, len(value.Name), len(value.Version), len(value.SerializedInput),
			mapSize(value.Tags), mapSize(value.ContextFields), traceSize(value.ParentTraceContext))
	}
	if value := event.TaskCompleted; value != nil {
		size = addSize(size, len(value.SerializedResult))
	}
	if value := event.TaskFailed; value != nil {
		size = addSize(size, failureSize(value.FailureDetails))
	}
	if value := event.SubOrchestrationInstanceCreated; value != nil {
		size = addSize(size, len(value.InstanceID), len(value.Name), len(value.Version), len(value.SerializedInput),
			mapSize(value.Tags), mapSize(value.ContextFields), traceSize(value.ParentTraceContext))
	}
	if value := event.SubOrchestrationInstanceCompleted; value != nil {
		size = addSize(size, len(value.SerializedResult))
	}
	if value := event.SubOrchestrationInstanceFailed; value != nil {
		size = addSize(size, failureSize(value.FailureDetails))
	}
	if value := event.EventSent; value != nil {
		size = addSize(size, len(value.InstanceID), len(value.Name), len(value.SerializedInput))
	}
	if value := event.EventRaised; value != nil {
		size = addSize(size, len(value.InstanceID), len(value.Name), len(value.SerializedInput))
	}
	if value := event.Generic; value != nil {
		size = addSize(size, len(value.SerializedInput))
	}
	if value := event.HistoryState; value != nil {
		size = addSize(size, metadataSize(value.State))
	}
	if value := event.ContinueAsNew; value != nil {
		size = addSize(size, len(value.SerializedInput))
	}
	if value := event.ExecutionSuspended; value != nil {
		size = addSize(size, len(value.SerializedInput))
	}
	if value := event.ExecutionResumed; value != nil {
		size = addSize(size, len(value.SerializedInput))
	}
	if value := event.Entity; value != nil {
		size = addSize(size, len(value.RequestID), len(value.Operation), len(value.TargetInstanceID),
			len(value.ParentInstanceID), len(value.ParentExecutionID), len(value.CriticalSectionID),
			len(value.SerializedInput), len(value.SerializedOutput), failureSize(value.FailureDetails),
			propertySize(reflect.ValueOf(value.LockSet), 0))
	}
	if value := event.ExecutionRewound; value != nil {
		size = addSize(size, len(value.Reason), len(value.Name), len(value.Version), len(value.InstanceID),
			len(value.ParentExecutionID), len(value.SerializedInput), mapSize(value.Tags), mapSize(value.ContextFields),
			parentSize(value.Parent), traceSize(value.ParentTraceContext))
	}
	return size
}

func mapSize(values map[string]string) int {
	if values == nil {
		return 0
	}
	size := 64
	for key, value := range values {
		size = addSize(size, 32, len(key), len(value))
		if size == maxEstimatedSize {
			break
		}
	}
	return size
}

func parentSize(value *api.HistoryParentInstanceInfo) int {
	if value == nil {
		return 0
	}
	return addSize(96, len(value.Name), len(value.Version), len(value.InstanceID), len(value.ExecutionID))
}

func traceSize(value *api.HistoryTraceContext) int {
	if value == nil {
		return 0
	}
	return addSize(64, len(value.TraceParent), len(value.TraceState), len(value.SpanID))
}

func metadataSize(value *api.OrchestrationMetadata) int {
	if value == nil {
		return 0
	}
	return addSize(384, len(value.InstanceID), len(value.Name), len(value.Version), len(value.ExecutionID),
		len(value.ParentInstanceID), len(value.SerializedInput), len(value.SerializedOutput),
		len(value.SerializedCustomStatus), mapSize(value.Tags), failureSize(value.FailureDetails))
}

func failureSize(value *api.FailureDetails) int {
	size := 0
	for depth := 0; value != nil; depth, value = depth+1, value.InnerFailure {
		if depth >= api.MaxFailureDetailsDepth {
			return maxEstimatedSize
		}
		size = addSize(size, 96, len(value.ErrorType), len(value.ErrorMessage), len(value.StackTrace),
			propertySize(reflect.ValueOf(value.Properties), 0))
		if size == maxEstimatedSize {
			return size
		}
	}
	return size
}

// The depth limit rejects cycles without allocating a visited set. Reflection
// also covers typed maps, slices, and structs supplied directly through the API.
func propertySize(value reflect.Value, depth int) int {
	if !value.IsValid() {
		return 0
	}
	if depth >= maxPropertyDepth {
		return maxEstimatedSize
	}
	size := 0
	switch value.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return int(value.Type().Size())
	case reflect.String:
		return addSize(16, value.Len())
	case reflect.Interface, reflect.Pointer:
		size = int(value.Type().Size())
		if !value.IsNil() {
			size = addSize(size, propertySize(value.Elem(), depth+1))
		}
	case reflect.Map:
		if value.IsNil() {
			return 0
		}
		size = 64
		iterator := value.MapRange()
		for size < maxEstimatedSize && iterator.Next() {
			size = addSize(size, 32, propertySize(iterator.Key(), depth+1), propertySize(iterator.Value(), depth+1))
		}
	case reflect.Slice, reflect.Array:
		if value.Kind() == reflect.Slice {
			if value.IsNil() {
				return 0
			}
			size = 24
			// The backing array, including elements beyond len, remains retained.
			value = value.Slice(0, value.Cap())
		}
		switch value.Type().Elem().Kind() {
		case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
			reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
			return addSize(size, multiplySize(value.Len(), int(value.Type().Elem().Size())))
		}
		for i := 0; i < value.Len() && size < maxEstimatedSize; i++ {
			size = addSize(size, propertySize(value.Index(i), depth+1))
		}
	case reflect.Struct:
		size = 32
		for i := 0; i < value.NumField() && size < maxEstimatedSize; i++ {
			size = addSize(size, propertySize(value.Field(i), depth+1))
		}
	default:
		// Closures, channels, and unsafe pointers cannot be safely inspected.
		return maxEstimatedSize
	}
	return size
}
