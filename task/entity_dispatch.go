package task

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/microsoft/durabletask-go/api"
)

// EntityDispatcher provides automatic operation dispatch for entities.
// It maps entity operations to methods on a state struct.
//
// Methods on the state struct are matched by name (case-insensitive).
// Each method can have one of these signatures:
//
//	func (s *State) OperationName() (any, error)
//	func (s *State) OperationName(input InputType) (any, error)
//	func (s *State) OperationName(ctx *EntityContext) (any, error)
//	func (s *State) OperationName(ctx *EntityContext, input InputType) (any, error)
//
// Example usage:
//
//	type Counter struct {
//	    Value int `json:"value"`
//	}
//
//	func (c *Counter) Add(amount int) (any, error) {
//	    c.Value += amount
//	    return c.Value, nil
//	}
//
//	func (c *Counter) Get() (any, error) {
//	    return c.Value, nil
//	}
//
//	func (c *Counter) Reset() (any, error) {
//	    c.Value = 0
//	    return nil, nil
//	}
//
//	// Register entity:
//	r.AddEntityN("counter", task.NewEntityFor[Counter]())

// NewEntityFor creates an entity function that automatically dispatches
// operations to methods on a state struct of type S.
//
// The state is automatically loaded from and saved to the entity context.
// If the entity has no state, a zero-value S is used.
//
// The special operation "delete" resets the entity state (unless a Delete method exists).
func NewEntityFor[S any]() Entity {
	return func(ctx *EntityContext) (any, error) {
		// Load state
		var state S
		if ctx.HasState() {
			if err := ctx.GetState(&state); err != nil {
				return nil, fmt.Errorf("failed to deserialize entity state: %w", err)
			}
		}

		// Handle implicit "delete" operation
		if strings.EqualFold(ctx.Operation, "delete") {
			// Check if a user-defined Delete method exists first
			if _, found := findMethod(reflect.TypeOf(&state), "delete"); !found {
				return nil, ctx.SetState(nil)
			}
		}

		// Dispatch to method
		result, err := dispatchToMethod(ctx, &state)
		if err != nil {
			return nil, err
		}

		// Save state back
		if !ctx.stateDirty {
			if err := ctx.SetState(state); err != nil {
				return nil, fmt.Errorf("failed to save entity state: %w", err)
			}
		}
		return result, nil
	}
}

func findMethod(t reflect.Type, name string) (reflect.Method, bool) {
	for i := 0; i < t.NumMethod(); i++ {
		m := t.Method(i)
		if strings.EqualFold(m.Name, name) {
			return m, true
		}
	}
	return reflect.Method{}, false
}

func dispatchToMethod[S any](ctx *EntityContext, state *S) (any, error) {
	stateVal := reflect.ValueOf(state)
	method, found := findMethod(stateVal.Type(), ctx.Operation)
	if !found {
		return nil, fmt.Errorf("entity does not support operation '%s'", ctx.Operation)
	}

	methodType := method.Type
	// numIn includes the receiver
	numIn := methodType.NumIn()

	var args []reflect.Value
	args = append(args, stateVal) // receiver

	for i := 1; i < numIn; i++ {
		paramType := methodType.In(i)

		// Check if it's *EntityContext
		if paramType == reflect.TypeOf((*EntityContext)(nil)) {
			args = append(args, reflect.ValueOf(ctx))
			continue
		}

		// Check if it's api.EntityID
		if paramType == reflect.TypeFor[api.EntityID]() {
			args = append(args, reflect.ValueOf(ctx.ID))
			continue
		}

		// Otherwise, treat as input parameter
		inputPtr := reflect.New(paramType)
		if err := ctx.GetInput(inputPtr.Interface()); err != nil {
			return nil, fmt.Errorf("failed to deserialize input for operation '%s': %w", ctx.Operation, err)
		}
		args = append(args, inputPtr.Elem())
	}

	results := method.Func.Call(args)

	// Parse return values: expect (any, error) or (error) or ()
	errorType := reflect.TypeOf((*error)(nil)).Elem()
	isNilValue := func(v reflect.Value) bool {
		switch v.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
			return v.IsNil()
		default:
			return false
		}
	}

	switch len(results) {
	case 0:
		return nil, nil
	case 1:
		if results[0].Type().Implements(errorType) {
			if isNilValue(results[0]) {
				return nil, nil
			}
			return nil, results[0].Interface().(error)
		}
		return results[0].Interface(), nil
	case 2:
		errVal := results[1]
		if !errVal.Type().Implements(errorType) {
			return nil, fmt.Errorf("method '%s' has unsupported error return type: %s", ctx.Operation, errVal.Type())
		}

		var retErr error
		if !isNilValue(errVal) {
			retErr = errVal.Interface().(error)
		}

		if isNilValue(results[0]) {
			return nil, retErr
		}
		return results[0].Interface(), retErr
	default:
		return nil, fmt.Errorf("method '%s' has unsupported number of return values: %d", ctx.Operation, len(results))
	}
}
