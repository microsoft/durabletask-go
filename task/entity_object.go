package task

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/microsoft/durabletask-go/internal/helpers"
)

// EntityObjectBase binds a persistent entity object to its current state and operation context.
// Embed it in an entity object used with NewEntityObjectFactory.
type EntityObjectBase[S any] struct {
	state   *S
	context *EntityContext
}

// State returns the mutable state for the current operation.
func (base *EntityObjectBase[S]) State() *S {
	return base.state
}

// Context returns the context for the current operation.
func (base *EntityObjectBase[S]) Context() *EntityContext {
	return base.context
}

func (base *EntityObjectBase[S]) bindEntityObject(ctx *EntityContext, state *S) {
	base.context = ctx
	base.state = state
}

// EntityObjectBinding is implemented by entity objects that embed EntityObjectBase.
type EntityObjectBinding[S any] interface {
	bindEntityObject(*EntityContext, *S)
}

type entityObjectOptions[S any] struct {
	initializer        func(*EntityContext) (S, error)
	allowStateDispatch bool
}

// EntityObjectOption configures reflected entity-object dispatch.
type EntityObjectOption[S any] func(*entityObjectOptions[S])

// WithEntityStateInitializer configures state creation when an operation starts without state.
func WithEntityStateInitializer[S any](
	initializer func(*EntityContext) (S, error),
) EntityObjectOption[S] {
	return func(options *entityObjectOptions[S]) {
		options.initializer = initializer
	}
}

// WithEntityStateDispatch enables reflected operation dispatch to the state
// after no matching entity-object method is found.
func WithEntityStateDispatch[S any]() EntityObjectOption[S] {
	return func(options *entityObjectOptions[S]) {
		options.allowStateDispatch = true
	}
}

// NewEntityObjectFactory creates one persistent entity object per batch while
// storing only S as durable state.
//
// Operation binding is validated when the factory is created, so a malformed
// entity object fails at startup instead of failing every batch at runtime.
func NewEntityObjectFactory[S any, E EntityObjectBinding[S]](
	factory func(EntityFactoryContext) (E, error),
	configure ...EntityObjectOption[S],
) EntityFactory {
	stateType := reflect.TypeFor[S]()
	if stateType.Kind() == reflect.Pointer {
		panic("NewEntityObjectFactory does not support pointer state types")
	}
	if factory == nil {
		panic("NewEntityObjectFactory requires an entity object factory")
	}
	options := entityObjectOptions[S]{}
	for _, apply := range configure {
		apply(&options)
	}

	// A concrete type parameter is validated eagerly. An interface type
	// parameter only reveals its operations once the object is created.
	var staticSets *entityObjectMethodSets
	if objectType := reflect.TypeFor[E](); objectType.Kind() != reflect.Interface {
		sets, err := newEntityObjectMethodSets(objectType, stateType)
		if err != nil {
			panic(err.Error())
		}
		staticSets = sets
	}

	return func(factoryContext EntityFactoryContext) (EntityBatch, error) {
		object, err := factory(factoryContext)
		if err != nil {
			return EntityBatch{}, err
		}
		if isNilEntityValue(object) {
			return EntityBatch{}, fmt.Errorf("entity object factory returned nil")
		}
		batch := EntityBatch{}
		if closer, ok := any(object).(EntityBatchCloser); ok {
			batch.Close = closer.CloseEntityBatch
		}

		objectValue := reflect.ValueOf(object)
		sets := staticSets
		if sets == nil {
			sets, err = newEntityObjectMethodSets(objectValue.Type(), stateType)
			if err != nil {
				return closeAfterSetupFailure(factoryContext, batch, err)
			}
		}

		batch.Entity = func(ctx *EntityContext) (any, error) {
			var state S
			if ctx.HasState() {
				if err := ctx.GetState(&state); err != nil {
					return nil, fmt.Errorf("failed to deserialize entity state: %w", err)
				}
			} else if options.initializer != nil {
				initialized, err := options.initializer(ctx)
				if err != nil {
					return nil, fmt.Errorf("failed to initialize entity state: %w", err)
				}
				state = initialized
			}
			object.bindEntityObject(ctx, &state)

			result, found, err := sets.object.call(ctx, objectValue)
			if err != nil {
				return nil, err
			}
			if !found && options.allowStateDispatch {
				result, found, err = sets.state.call(ctx, reflect.ValueOf(&state))
				if err != nil {
					return nil, err
				}
			}
			if !found {
				if strings.EqualFold(ctx.Operation, "delete") {
					ctx.DeleteState()
					return nil, nil
				}
				return nil, fmt.Errorf("entity does not support operation %q", ctx.Operation)
			}
			if !ctx.stateDirty {
				if err := ctx.SetState(state); err != nil {
					return nil, fmt.Errorf("failed to save entity state: %w", err)
				}
			}
			return result, nil
		}
		return batch, nil
	}
}

// entityObjectMethodSets holds the dispatch tables shared by every batch of one
// registered entity object.
type entityObjectMethodSets struct {
	object *entityMethodSet
	state  *entityMethodSet
}

func newEntityObjectMethodSets(
	objectType reflect.Type,
	stateType reflect.Type,
) (*entityObjectMethodSets, error) {
	objectMethods, err := collectEntityMethods(
		objectType,
		"closeentitybatch",
		"context",
		"state",
	)
	if err != nil {
		return nil, err
	}
	stateMethods, err := collectEntityMethods(reflect.PointerTo(stateType))
	if err != nil {
		return nil, err
	}
	return &entityObjectMethodSets{
		object: &entityMethodSet{methods: objectMethods},
		state:  &entityMethodSet{methods: stateMethods},
	}, nil
}

func closeAfterSetupFailure(
	factoryContext EntityFactoryContext,
	batch EntityBatch,
	setupErr error,
) (EntityBatch, error) {
	if batch.Close == nil {
		return EntityBatch{}, setupErr
	}
	if closeErr := batch.Close(factoryContext.Context); closeErr != nil {
		return EntityBatch{}, errors.Join(
			setupErr,
			fmt.Errorf("failed to close entity batch after setup failure: %w", closeErr),
		)
	}
	return EntityBatch{}, setupErr
}

func collectEntityMethods(
	targetType reflect.Type,
	excludedNames ...string,
) (map[string]reflect.Method, error) {
	methods := make(map[string]reflect.Method)
	for i := 0; i < targetType.NumMethod(); i++ {
		method := targetType.Method(i)
		name := helpers.ToLowerInvariant(method.Name)
		if slices.Contains(excludedNames, name) {
			continue
		}
		if existing, ok := methods[name]; ok {
			return nil, fmt.Errorf(
				"entity object found case-insensitive operation collision between %s and %s",
				existing.Name,
				method.Name,
			)
		}
		methods[name] = method
	}
	return methods, nil
}
