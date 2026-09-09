package failure

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"time"
	"unicode/utf8"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	maxErrorTypeBytes     = 1024
	maxErrorMessageBytes  = 16 * 1024
	maxStackTraceBytes    = 16 * 1024
	maxPropertyCount      = 64
	maxPropertyValueBytes = 32 * 1024
	maxPropertiesBytes    = 64 * 1024
)

const propertiesTruncatedKey = "go.propertiesTruncated"

type propertyBudget struct {
	count     int
	bytes     int
	truncated bool
}

// FromProto converts wire failure details into API-owned failure data.
func FromProto(details *protos.TaskFailureDetails) *api.FailureDetails {
	return fromProto(details, 0, new(propertyBudget))
}

func fromProto(details *protos.TaskFailureDetails, depth int, budget *propertyBudget) *api.FailureDetails {
	if details == nil {
		return nil
	}
	result := &api.FailureDetails{
		ErrorType:      api.ErrorType(truncateUTF8(details.GetErrorType(), maxErrorTypeBytes)),
		ErrorMessage:   truncateUTF8(details.GetErrorMessage(), maxErrorMessageBytes),
		IsNonRetriable: details.GetIsNonRetriable(),
		StackTrace:     truncateUTF8(details.GetStackTrace().GetValue(), maxStackTraceBytes),
	}
	if len(details.GetProperties()) > 0 {
		result.Properties = make(map[string]any, min(len(details.GetProperties()), maxPropertyCount))
		keys := make([]string, 0, len(details.GetProperties()))
		for key := range details.GetProperties() {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		truncated := false
		for _, key := range keys {
			size := len(key) + proto.Size(details.GetProperties()[key])
			if budget.count >= maxPropertyCount || budget.bytes+size > maxPropertiesBytes {
				truncated = true
				break
			}
			result.Properties[key] = valueFromProto(details.GetProperties()[key])
			budget.count++
			budget.bytes += size
		}
		if truncated && !budget.truncated {
			result.Properties[propertiesTruncatedKey] = true
			budget.truncated = true
		}
	}
	if depth+1 < api.MaxFailureDetailsDepth {
		result.InnerFailure = fromProto(details.GetInnerFailure(), depth+1, budget)
	}
	return result
}

// ToProto converts API-owned failure data to its wire representation.
func ToProto(details *api.FailureDetails) *protos.TaskFailureDetails {
	return toProto(details, 0, new(propertyBudget))
}

func toProto(details *api.FailureDetails, depth int, budget *propertyBudget) *protos.TaskFailureDetails {
	if details == nil {
		return nil
	}
	result := &protos.TaskFailureDetails{
		ErrorType:      truncateUTF8(string(details.ErrorType), maxErrorTypeBytes),
		ErrorMessage:   truncateUTF8(details.ErrorMessage, maxErrorMessageBytes),
		IsNonRetriable: details.IsNonRetriable,
	}
	if stack := truncateUTF8(details.StackTrace, maxStackTraceBytes); stack != "" {
		result.StackTrace = wrapperspb.String(stack)
	}
	addProperties(result, details.Properties, budget)
	if depth+1 < api.MaxFailureDetailsDepth {
		result.InnerFailure = toProto(details.InnerFailure, depth+1, budget)
	}
	return result
}

// FromError converts a Go error into wire failure details while preserving wrapped durable failures.
func FromError(err error, providers ...api.ErrorPropertiesProvider) *protos.TaskFailureDetails {
	var provider api.ErrorPropertiesProvider
	if len(providers) > 0 {
		provider = providers[0]
	}
	return fromError(err, provider, 0, new(propertyBudget))
}

func fromError(
	err error,
	provider api.ErrorPropertiesProvider,
	depth int,
	budget *propertyBudget,
) *protos.TaskFailureDetails {
	if err == nil {
		return nil
	}
	result := &protos.TaskFailureDetails{
		ErrorType:    truncateUTF8(string(errorType(err)), maxErrorTypeBytes),
		ErrorMessage: truncateUTF8(err.Error(), maxErrorMessageBytes),
	}
	var stackProvider api.DurableTaskStackTraceProvider
	if errors.As(err, &stackProvider) {
		if stack := truncateUTF8(stackProvider.DurableTaskStackTrace(), maxStackTraceBytes); stack != "" {
			result.StackTrace = wrapperspb.String(stack)
		}
	}
	var marker api.NonRetriable
	if errors.As(err, &marker) {
		result.IsNonRetriable = marker.NonRetriable()
	}
	var propertiesProvider api.DurableTaskErrorPropertiesProvider
	if errors.As(err, &propertiesProvider) {
		addProperties(result, propertiesProvider.DurableTaskErrorProperties(), budget)
	}
	if provider != nil {
		addProperties(result, provider.ErrorProperties(err), budget)
	}
	if depth+1 >= api.MaxFailureDetailsDepth {
		return result
	}
	var detailsProvider api.DurableTaskFailureDetailsProvider
	if errors.As(err, &detailsProvider) {
		result.InnerFailure = toProto(detailsProvider.DurableTaskFailureDetails(), depth+1, budget)
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		joinedErrors := joined.Unwrap()
		primary := joinedPrimary(joinedErrors)
		if result.InnerFailure == nil && len(joinedErrors) > 0 {
			result.InnerFailure = fromError(joinedErrors[primary], provider, depth+1, budget)
		}
		if len(joinedErrors) > 1 {
			addAdditionalErrors(result, joinedErrors, primary, provider, depth+1, budget)
		}
		return result
	}
	if result.InnerFailure == nil {
		result.InnerFailure = fromError(errors.Unwrap(err), provider, depth+1, budget)
	}
	return result
}

func errorType(err error) api.ErrorType {
	var provider api.DurableTaskErrorTypeProvider
	if errors.As(err, &provider) {
		if errorType := provider.DurableTaskErrorType(); errorType != "" {
			return errorType
		}
	}
	return api.ErrorType(reflect.TypeOf(err).String())
}

func joinedPrimary(joined []error) int {
	for i, err := range joined {
		var provider api.DurableTaskFailureDetailsProvider
		if errors.As(err, &provider) {
			return i
		}
	}
	for i, err := range joined {
		var provider api.DurableTaskErrorTypeProvider
		if errors.As(err, &provider) && provider.DurableTaskErrorType() != "" {
			return i
		}
	}
	return 0
}

func addAdditionalErrors(
	details *protos.TaskFailureDetails,
	joined []error,
	primary int,
	provider api.ErrorPropertiesProvider,
	depth int,
	budget *propertyBudget,
) {
	values := make([]any, 0, len(joined)-1)
	for i, err := range joined {
		if i == primary || err == nil {
			continue
		}
		branchBudget := new(propertyBudget)
		branch := fromError(err, provider, depth, branchBudget)
		values = append(values, failureDetailsProperty(branch, 0))
	}
	if len(values) == 0 {
		return
	}
	properties := map[string]any{"go.additionalErrors": values}
	addProperties(details, properties, budget)
}

func addProperties(
	details *protos.TaskFailureDetails,
	properties map[string]any,
	budget *propertyBudget,
) {
	if len(properties) == 0 {
		return
	}
	keys := make([]string, 0, len(properties))
	for key := range properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	truncated := false
	for _, key := range keys {
		value := valueToProto(properties[key], 0)
		size := len(key) + proto.Size(value)
		if size > maxPropertyValueBytes {
			value = structpb.NewStringValue("<property value truncated>")
			size = len(key) + proto.Size(value)
			truncated = true
		}
		oldValue, exists := details.GetProperties()[key]
		oldSize := 0
		if exists {
			oldSize = len(key) + proto.Size(oldValue)
		}
		countDelta := 1
		if exists {
			countDelta = 0
		}
		if budget.count+countDelta > maxPropertyCount ||
			budget.bytes-oldSize+size > maxPropertiesBytes {
			truncated = true
			break
		}
		if details.Properties == nil {
			details.Properties = make(map[string]*structpb.Value)
		}
		details.Properties[key] = value
		budget.count += countDelta
		budget.bytes = budget.bytes - oldSize + size
	}

	if truncated && !budget.truncated {
		if details.Properties == nil {
			details.Properties = make(map[string]*structpb.Value)
		}
		details.Properties[propertiesTruncatedKey] = structpb.NewBoolValue(true)
		budget.truncated = true
	}
}

func failureDetailsProperty(details *protos.TaskFailureDetails, depth int) map[string]any {
	if details == nil || depth >= api.MaxFailureDetailsDepth {
		return nil
	}
	result := map[string]any{
		"type":    details.GetErrorType(),
		"message": details.GetErrorMessage(),
	}
	if details.GetStackTrace().GetValue() != "" {
		result["stackTrace"] = details.GetStackTrace().GetValue()
	}
	if details.GetIsNonRetriable() {
		result["isNonRetriable"] = true
	}
	if len(details.GetProperties()) > 0 {
		properties := make(map[string]any, len(details.GetProperties()))
		for key, value := range details.GetProperties() {
			properties[key] = valueFromProto(value)
		}
		result["properties"] = properties
	}
	if inner := failureDetailsProperty(details.GetInnerFailure(), depth+1); inner != nil {
		result["innerFailure"] = inner
	}
	return result
}

func valueToProto(value any, depth int) *structpb.Value {
	if depth >= 16 {
		return structpb.NewStringValue("<property nesting truncated>")
	}
	reflected := reflect.ValueOf(value)
	for reflected.IsValid() && (reflected.Kind() == reflect.Interface || reflected.Kind() == reflect.Pointer) {
		if reflected.IsNil() {
			return structpb.NewNullValue()
		}
		reflected = reflected.Elem()
	}
	switch typed := value.(type) {
	case nil:
		return structpb.NewNullValue()
	case bool:
		return structpb.NewBoolValue(typed)
	case string:
		return structpb.NewStringValue(typed)
	case time.Time:
		return structpb.NewStringValue("dt:" + typed.Format(time.RFC3339Nano))
	case fmt.Stringer:
		return structpb.NewStringValue(typed.String())
	}

	if !reflected.IsValid() {
		return structpb.NewNullValue()
	}
	switch reflected.Kind() {
	case reflect.Bool:
		return structpb.NewBoolValue(reflected.Bool())
	case reflect.String:
		return structpb.NewStringValue(reflected.String())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return structpb.NewNumberValue(float64(reflected.Int()))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return structpb.NewNumberValue(float64(reflected.Uint()))
	case reflect.Float32, reflect.Float64:
		number := reflected.Float()
		if math.IsNaN(number) || math.IsInf(number, 0) {
			return structpb.NewStringValue(fmt.Sprint(number))
		}
		return structpb.NewNumberValue(number)
	case reflect.Map:
		if reflected.Type().Key().Kind() != reflect.String {
			return structpb.NewStringValue(fmt.Sprint(value))
		}
		keys := make([]string, 0, reflected.Len())
		for _, key := range reflected.MapKeys() {
			keys = append(keys, key.String())
		}
		sort.Strings(keys)
		fields := make(map[string]*structpb.Value, len(keys))
		for _, key := range keys {
			mapKey := reflect.ValueOf(key).Convert(reflected.Type().Key())
			fields[key] = valueToProto(reflected.MapIndex(mapKey).Interface(), depth+1)
		}
		return structpb.NewStructValue(&structpb.Struct{Fields: fields})
	case reflect.Array, reflect.Slice:
		values := make([]*structpb.Value, reflected.Len())
		for i := 0; i < reflected.Len(); i++ {
			values[i] = valueToProto(reflected.Index(i).Interface(), depth+1)
		}
		return structpb.NewListValue(&structpb.ListValue{Values: values})
	default:
		return structpb.NewStringValue(fmt.Sprint(value))
	}
}

func valueFromProto(value *structpb.Value) any {
	if value == nil {
		return nil
	}
	switch typed := value.Kind.(type) {
	case *structpb.Value_StringValue:
		return typed.StringValue
	case *structpb.Value_StructValue:
		result := make(map[string]any, len(typed.StructValue.Fields))
		for key, field := range typed.StructValue.Fields {
			result[key] = valueFromProto(field)
		}
		return result
	case *structpb.Value_ListValue:
		result := make([]any, len(typed.ListValue.Values))
		for i, item := range typed.ListValue.Values {
			result[i] = valueFromProto(item)
		}
		return result
	default:
		return value.AsInterface()
	}
}

func truncateUTF8(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	const marker = "\n... truncated"
	limit -= len(marker)
	for limit > 0 && !utf8.RuneStart(value[limit]) {
		limit--
	}
	return value[:limit] + marker
}
