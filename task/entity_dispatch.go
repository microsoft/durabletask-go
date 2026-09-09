package task

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
)

var (
	entityContextType = reflect.TypeFor[*EntityContext]()
	entityIDType      = reflect.TypeFor[api.EntityID]()
	errorType         = reflect.TypeFor[error]()
)

type entityParameterKind uint8

const (
	entityContextParameter entityParameterKind = iota
	entityIDParameter
	entityInputParameter
	optionalEntityInputParameter
)

type entityParameterBinding struct {
	kind          entityParameterKind
	parameterType reflect.Type
}

type entityMethodBinding struct {
	method       reflect.Method
	parameters   []entityParameterBinding
	numOut       int
	returnsError bool
}

type entityMethodBindingResult struct {
	binding *entityMethodBinding
	err     error
}

type entityMethodSet struct {
	methods  map[string]reflect.Method
	bindings sync.Map
}

func (set *entityMethodSet) call(ctx *EntityContext, receiver reflect.Value) (any, bool, error) {
	operation := helpers.ToLowerInvariant(ctx.Operation)
	method, found := set.methods[operation]
	if !found {
		return nil, false, nil
	}

	cached, ok := set.bindings.Load(operation)
	if !ok {
		binding, err := bindEntityMethod(ctx.Operation, method)
		cached, _ = set.bindings.LoadOrStore(operation, entityMethodBindingResult{
			binding: binding,
			err:     err,
		})
	}
	result := cached.(entityMethodBindingResult)
	if result.err != nil {
		return nil, true, result.err
	}

	output, err := result.binding.call(ctx, receiver)
	return output, true, err
}

// NewEntityFor creates an entity function that dispatches operations to methods
// on a converter-serializable state struct of type S.
//
// Supported method parameters are an optional *EntityContext, an optional
// api.EntityID, and at most one required input value or OptionalEntityInput,
// in any order. Supported return shapes are (), (result), (error), and
// (result, error).
func NewEntityFor[S any]() Entity {
	stateType := reflect.TypeFor[S]()
	if stateType.Kind() == reflect.Pointer {
		panic("NewEntityFor does not support pointer state types")
	}

	methods := make(map[string]reflect.Method)
	pointerType := reflect.PointerTo(stateType)
	for i := 0; i < pointerType.NumMethod(); i++ {
		method := pointerType.Method(i)
		name := helpers.ToLowerInvariant(method.Name)
		if existing, ok := methods[name]; ok {
			panic(fmt.Sprintf(
				"NewEntityFor found case-insensitive operation collision between %s and %s",
				existing.Name,
				method.Name,
			))
		}
		methods[name] = method
	}

	methodSet := entityMethodSet{methods: methods}
	return func(ctx *EntityContext) (any, error) {
		var state S
		if ctx.HasState() {
			if err := ctx.GetState(&state); err != nil {
				return nil, fmt.Errorf("failed to deserialize entity state: %w", err)
			}
		}

		output, found, err := methodSet.call(ctx, reflect.ValueOf(&state))
		if !found {
			if strings.EqualFold(ctx.Operation, "delete") {
				ctx.DeleteState()
				return nil, nil
			}
			return nil, fmt.Errorf("entity does not support operation %q", ctx.Operation)
		}
		if err != nil {
			return nil, err
		}
		if !ctx.stateDirty {
			if err := ctx.SetState(state); err != nil {
				return nil, fmt.Errorf("failed to save entity state: %w", err)
			}
		}
		return output, nil
	}
}

func bindEntityMethod(operation string, method reflect.Method) (*entityMethodBinding, error) {
	methodType := method.Type
	if methodType.IsVariadic() {
		return nil, fmt.Errorf("entity operation %q must not be variadic", operation)
	}

	binding := &entityMethodBinding{
		method:     method,
		parameters: make([]entityParameterBinding, 0, methodType.NumIn()-1),
		numOut:     methodType.NumOut(),
	}
	var sawContext, sawEntityID, sawInput bool
	for i := 1; i < methodType.NumIn(); i++ {
		parameterType := methodType.In(i)
		parameter := entityParameterBinding{parameterType: parameterType}
		switch parameterType {
		case entityContextType:
			if sawContext {
				return nil, fmt.Errorf("entity operation %q accepts *EntityContext more than once", operation)
			}
			sawContext = true
			parameter.kind = entityContextParameter
		case entityIDType:
			if sawEntityID {
				return nil, fmt.Errorf("entity operation %q accepts api.EntityID more than once", operation)
			}
			sawEntityID = true
			parameter.kind = entityIDParameter
		default:
			if sawInput {
				return nil, fmt.Errorf("entity operation %q accepts more than one input parameter", operation)
			}
			sawInput = true
			parameter.kind = entityInputParameter
			if _, ok := reflect.New(parameterType).Interface().(optionalEntityInputBinder); ok {
				parameter.kind = optionalEntityInputParameter
			}
		}
		binding.parameters = append(binding.parameters, parameter)
	}

	binding.returnsError = binding.numOut > 0 && methodType.Out(binding.numOut-1).Implements(errorType)
	switch binding.numOut {
	case 0, 1:
	case 2:
		if !binding.returnsError {
			return nil, fmt.Errorf("entity operation %q second return value must implement error", operation)
		}
	default:
		return nil, fmt.Errorf("entity operation %q has unsupported return count %d", operation, binding.numOut)
	}
	return binding, nil
}

func (binding *entityMethodBinding) call(ctx *EntityContext, receiver reflect.Value) (any, error) {
	args := make([]reflect.Value, 1, len(binding.parameters)+1)
	args[0] = receiver
	for _, parameter := range binding.parameters {
		switch parameter.kind {
		case entityContextParameter:
			args = append(args, reflect.ValueOf(ctx))
		case entityIDParameter:
			args = append(args, reflect.ValueOf(ctx.ID))
		case optionalEntityInputParameter:
			input := reflect.New(parameter.parameterType)
			if err := input.Interface().(optionalEntityInputBinder).bindEntityInput(ctx); err != nil {
				return nil, fmt.Errorf("failed to deserialize input for operation %q: %w", ctx.Operation, err)
			}
			args = append(args, input.Elem())
		default:
			if !ctx.HasInput() {
				return nil, fmt.Errorf(
					"failed to bind input for operation %q: the operation expected an input value, but none was provided",
					ctx.Operation,
				)
			}
			input := reflect.New(parameter.parameterType)
			if err := ctx.GetInput(input.Interface()); err != nil {
				return nil, fmt.Errorf("failed to deserialize input for operation %q: %w", ctx.Operation, err)
			}
			args = append(args, input.Elem())
		}
	}

	results := binding.method.Func.Call(args)
	switch {
	case binding.numOut == 0:
		return nil, nil
	case binding.numOut == 1 && binding.returnsError:
		return nil, reflectError(results[0])
	case binding.numOut == 1:
		return reflectResult(results[0]), nil
	default:
		return reflectResult(results[0]), reflectError(results[1])
	}
}

func reflectResult(value reflect.Value) any {
	if isNilable(value.Kind()) && value.IsNil() {
		return nil
	}
	return value.Interface()
}

func reflectError(value reflect.Value) error {
	if isNilable(value.Kind()) && value.IsNil() {
		return nil
	}
	return value.Interface().(error)
}

func isNilable(kind reflect.Kind) bool {
	switch kind {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return true
	default:
		return false
	}
}
