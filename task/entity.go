package task

import (
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Entity is the functional interface for entity implementations.
// An entity function receives an EntityContext and returns a result and error.
type Entity func(ctx *EntityContext) (any, error)

// EntityContext provides the execution context for an entity operation.
type EntityContext struct {
	ID        api.EntityID
	Operation string

	rawInput    []byte
	state       entityState
	actions     []*protos.OperationAction
	actionIDSeq int32
}

type entityState struct {
	value    []byte
	hasValue bool
}

// GetInput unmarshals the serialized entity operation input and saves the result into [v].
func (ctx *EntityContext) GetInput(v any) error {
	return unmarshalData(ctx.rawInput, v)
}

// HasState returns true if the entity has state set.
func (ctx *EntityContext) HasState() bool {
	return ctx.state.hasValue
}

// GetState unmarshals the entity state and saves the result into [v].
func (ctx *EntityContext) GetState(v any) error {
	if !ctx.state.hasValue {
		return fmt.Errorf("entity has no state")
	}
	return unmarshalData(ctx.state.value, v)
}

// SetState sets the entity state. The state must be JSON-serializable.
// Passing nil deletes the entity state.
func (ctx *EntityContext) SetState(state any) error {
	if state == nil {
		ctx.state.value = nil
		ctx.state.hasValue = false
		return nil
	}
	bytes, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal entity state: %w", err)
	}
	ctx.state.value = bytes
	ctx.state.hasValue = true
	return nil
}

// SignalEntity sends a fire-and-forget signal to another entity.
func (ctx *EntityContext) SignalEntity(entityID api.EntityID, operationName string, input any) error {
	var rawInput *wrapperspb.StringValue
	if input != nil {
		bytes, err := json.Marshal(input)
		if err != nil {
			return fmt.Errorf("failed to marshal signal input: %w", err)
		}
		rawInput = wrapperspb.String(string(bytes))
	}

	action := &protos.OperationAction{
		Id: ctx.nextActionID(),
		OperationActionType: &protos.OperationAction_SendSignal{
			SendSignal: &protos.SendSignalAction{
				InstanceId: entityID.String(),
				Name:       operationName,
				Input:      rawInput,
			},
		},
	}
	ctx.actions = append(ctx.actions, action)
	return nil
}

// StartNewOrchestration schedules a new orchestration from within an entity operation.
func (ctx *EntityContext) StartNewOrchestration(name string, opts ...entityStartOrchestrationOption) error {
	options := &entityStartOrchestrationOptions{}
	for _, configure := range opts {
		if err := configure(options); err != nil {
			return err
		}
	}
	if options.instanceID == "" {
		id := uuid.New()
		options.instanceID = hex.EncodeToString(id[:])
	}

	action := &protos.OperationAction{
		Id: ctx.nextActionID(),
		OperationActionType: &protos.OperationAction_StartNewOrchestration{
			StartNewOrchestration: &protos.StartNewOrchestrationAction{
				InstanceId: options.instanceID,
				Name:       name,
				Input:      options.rawInput,
			},
		},
	}
	ctx.actions = append(ctx.actions, action)
	return nil
}

func (ctx *EntityContext) nextActionID() int32 {
	id := ctx.actionIDSeq
	ctx.actionIDSeq++
	return id
}

// entityStartOrchestrationOptions holds options for starting orchestrations from entities.
type entityStartOrchestrationOptions struct {
	instanceID string
	rawInput   *wrapperspb.StringValue
}

// entityStartOrchestrationOption is a functional option for StartNewOrchestration.
type entityStartOrchestrationOption func(*entityStartOrchestrationOptions) error

// WithEntityStartOrchestrationInput sets the input for the new orchestration.
func WithEntityStartOrchestrationInput(input any) entityStartOrchestrationOption {
	return func(opts *entityStartOrchestrationOptions) error {
		bytes, err := json.Marshal(input)
		if err != nil {
			return fmt.Errorf("failed to marshal orchestration input: %w", err)
		}
		opts.rawInput = wrapperspb.String(string(bytes))
		return nil
	}
}

// WithEntityStartOrchestrationInstanceID sets the instance ID for the new orchestration.
func WithEntityStartOrchestrationInstanceID(instanceID string) entityStartOrchestrationOption {
	return func(opts *entityStartOrchestrationOptions) error {
		opts.instanceID = instanceID
		return nil
	}
}

// callEntityOption is a functional option type for the CallEntity orchestrator method.
type callEntityOption func(*callEntityOptions) error

type callEntityOptions struct {
	rawInput *wrapperspb.StringValue
}

// WithEntityInput configures an input for an entity operation invocation.
func WithEntityInput(input any) callEntityOption {
	return func(opt *callEntityOptions) error {
		data, err := marshalData(input)
		if err != nil {
			return err
		}
		opt.rawInput = wrapperspb.String(string(data))
		return nil
	}
}

// WithRawEntityInput configures a raw input for an entity operation invocation.
func WithRawEntityInput(input string) callEntityOption {
	return func(opt *callEntityOptions) error {
		opt.rawInput = wrapperspb.String(input)
		return nil
	}
}

// signalEntityOption is a functional option type for the SignalEntity orchestrator method.
type signalEntityOption func(*signalEntityOptions) error

type signalEntityOptions struct {
	rawInput *wrapperspb.StringValue
}

// WithSignalEntityInput configures an input for a signal entity invocation.
func WithSignalEntityInput(input any) signalEntityOption {
	return func(opt *signalEntityOptions) error {
		data, err := marshalData(input)
		if err != nil {
			return err
		}
		opt.rawInput = wrapperspb.String(string(data))
		return nil
	}
}

// WithRawSignalEntityInput configures a raw input for a signal entity invocation.
func WithRawSignalEntityInput(input string) signalEntityOption {
	return func(opt *signalEntityOptions) error {
		opt.rawInput = wrapperspb.String(input)
		return nil
	}
}
