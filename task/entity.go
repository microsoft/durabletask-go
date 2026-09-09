package task

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Entity is the functional interface for entity implementations.
// An entity function receives an EntityContext and returns a result and error.
type Entity func(ctx *EntityContext) (any, error)

// EntityFactoryContext identifies the entity batch for which an implementation is created.
type EntityFactoryContext struct {
	Context context.Context
	ID      api.EntityID
}

// EntityBatch contains one batch-scoped entity implementation and optional cleanup.
type EntityBatch struct {
	Entity Entity
	Close  func(context.Context) error
}

// EntityBatchCloser can release object resources after an entity batch completes.
type EntityBatchCloser interface {
	CloseEntityBatch(context.Context) error
}

// EntityFactory creates one entity implementation for an operation batch.
// Factories may run concurrently for different entity batches.
type EntityFactory func(EntityFactoryContext) (EntityBatch, error)

// EntityContext provides the execution context for an entity operation.
type EntityContext struct {
	ID        api.EntityID
	Operation string
	RequestID string
	IsSignal  bool

	rawInput    entityPayload
	state       entityState
	stateDirty  bool
	actions     []*protos.OperationAction
	actionIDSeq int32
	currentTime time.Time
	ctx         context.Context
	logger      *slog.Logger
	converter   api.DataConverter
	parentTrace *protos.TraceContext
}

type entityPayload struct {
	value   []byte
	present bool
}

type entityState struct {
	value    []byte
	hasValue bool
}

// HasInput reports whether the caller supplied an operation input.
func (ctx *EntityContext) HasInput() bool {
	return ctx.rawInput.present
}

// GetInput unmarshals the serialized entity operation input and saves the result into [v].
func (ctx *EntityContext) GetInput(v any) error {
	if !ctx.HasInput() {
		return fmt.Errorf("entity operation has no input")
	}
	if v == nil {
		return nil
	}
	return api.NormalizeDataConverter(ctx.converter).Deserialize(string(ctx.rawInput.value), v)
}

// SerializeInput serializes a value using the entity's configured data converter.
// It is useful when an entity needs to persist input for a later operation.
func (ctx *EntityContext) SerializeInput(value any) (string, error) {
	payload, err := marshalData(ctx.converter, value)
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

// GetRawState returns the entity's pre-serialized state and whether state is set.
// Callers are responsible for deserializing the returned value.
func (ctx *EntityContext) GetRawState() (string, bool) {
	return string(ctx.state.value), ctx.state.hasValue
}

// SetRawState stores pre-serialized entity state. Callers are responsible for
// producing data accepted by all readers of the entity state.
func (ctx *EntityContext) SetRawState(state string) {
	if state == "" {
		ctx.DeleteState()
		return
	}
	ctx.state.value = []byte(state)
	ctx.state.hasValue = true
	ctx.stateDirty = true
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
	if v == nil {
		return nil
	}
	return api.NormalizeDataConverter(ctx.converter).Deserialize(string(ctx.state.value), v)
}

// SetState serializes and stores entity state with the configured converter.
// Passing nil deletes the entity state.
func (ctx *EntityContext) SetState(state any) error {
	if isNilEntityValue(state) {
		ctx.DeleteState()
		return nil
	}
	payload, err := marshalData(ctx.converter, state)
	if err != nil {
		return fmt.Errorf("failed to serialize entity state: %w", err)
	}
	ctx.state.value = payload
	ctx.state.hasValue = true
	ctx.stateDirty = true
	return nil
}

// DeleteState removes the entity state.
func (ctx *EntityContext) DeleteState() {
	ctx.stateDirty = true
	ctx.state.value = nil
	ctx.state.hasValue = false
}

// Context returns the Go context for the current entity operation.
func (ctx *EntityContext) Context() context.Context {
	if ctx.ctx == nil {
		return context.Background()
	}
	return ctx.ctx
}

// Logger returns a logger enriched with entity operation identity.
func (ctx *EntityContext) Logger() *slog.Logger {
	if ctx.logger == nil {
		ctx.logger = slog.Default()
	}
	return ctx.logger
}

// CurrentTimeUTC returns the durable timestamp associated with this operation.
func (ctx *EntityContext) CurrentTimeUTC() time.Time {
	return ctx.currentTime
}

// SignalEntity sends a fire-and-forget signal to another entity.
func (ctx *EntityContext) SignalEntity(entityID api.EntityID, operationName string, input any) error {
	return ctx.signalEntity(entityID, operationName, input, time.Time{})
}

// SignalEntityAt schedules a fire-and-forget signal to another entity.
func (ctx *EntityContext) SignalEntityAt(entityID api.EntityID, scheduledTime time.Time, operationName string, input any) error {
	if scheduledTime.IsZero() {
		return fmt.Errorf("scheduled entity signal time must not be zero")
	}
	return ctx.signalEntity(entityID, operationName, input, scheduledTime)
}

func (ctx *EntityContext) signalEntity(entityID api.EntityID, operationName string, input any, scheduledTime time.Time) error {
	if err := helpers.ValidateEntityName(entityID.Name); err != nil {
		return err
	}
	if operationName == "" {
		return fmt.Errorf("entity operation name must not be empty")
	}

	rawInput, err := marshalEntityInput(ctx.converter, input)
	if err != nil {
		return fmt.Errorf("failed to serialize signal input: %w", err)
	}

	action := &protos.OperationAction{
		Id: ctx.nextActionID(),
		OperationActionType: &protos.OperationAction_SendSignal{
			SendSignal: &protos.SendSignalAction{
				InstanceId:         entityID.String(),
				Name:               operationName,
				Input:              rawInput,
				RequestTime:        timestampOrNil(ctx.currentTime),
				ScheduledTime:      timestampOrNil(scheduledTime),
				ParentTraceContext: helpers.CloneTraceContext(ctx.parentTrace),
			},
		},
	}
	ctx.actions = append(ctx.actions, action)
	return nil
}

// StartNewOrchestration schedules a new orchestration from within an entity operation.
func (ctx *EntityContext) StartNewOrchestration(name string, opts ...EntityStartOrchestrationOption) error {
	if name == "" {
		return fmt.Errorf("orchestration name must not be empty")
	}
	options := &entityStartOrchestrationOptions{}
	for _, configure := range opts {
		if err := configure(options, ctx.converter); err != nil {
			return err
		}
	}
	if options.instanceID == "" {
		seed := fmt.Sprintf("%s|%s|%d|start", ctx.ID.String(), ctx.RequestID, ctx.actionIDSeq)
		id := uuid.NewSHA1(entityActionNamespace, []byte(seed))
		options.instanceID = hex.EncodeToString(id[:])
	} else if err := helpers.ValidateOrchestrationInstanceID(options.instanceID); err != nil {
		return err
	}

	action := &protos.OperationAction{
		Id: ctx.nextActionID(),
		OperationActionType: &protos.OperationAction_StartNewOrchestration{
			StartNewOrchestration: &protos.StartNewOrchestrationAction{
				InstanceId:         options.instanceID,
				Name:               name,
				Version:            options.version,
				Input:              options.rawInput,
				ScheduledTime:      timestampOrNil(options.scheduledTime),
				RequestTime:        timestampOrNil(ctx.currentTime),
				ParentTraceContext: helpers.CloneTraceContext(ctx.parentTrace),
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
	instanceID    string
	version       *wrapperspb.StringValue
	rawInput      *wrapperspb.StringValue
	scheduledTime time.Time
}

// EntityStartOrchestrationOption is a functional option for StartNewOrchestration.
type EntityStartOrchestrationOption func(*entityStartOrchestrationOptions, api.DataConverter) error

// WithEntityStartOrchestrationInput sets the input for the new orchestration.
func WithEntityStartOrchestrationInput(input any) EntityStartOrchestrationOption {
	return func(opts *entityStartOrchestrationOptions, converter api.DataConverter) error {
		rawInput, err := marshalEntityInput(converter, input)
		if err != nil {
			return fmt.Errorf("failed to serialize orchestration input: %w", err)
		}
		opts.rawInput = rawInput
		return nil
	}
}

// WithRawEntityStartOrchestrationInput sets pre-serialized input for the new
// orchestration. Callers are responsible for ensuring the payload is accepted
// by the target orchestration's data converter.
func WithRawEntityStartOrchestrationInput(input string) EntityStartOrchestrationOption {
	return func(opts *entityStartOrchestrationOptions, _ api.DataConverter) error {
		opts.rawInput = wrapperspb.String(input)
		return nil
	}
}

// WithEntityStartOrchestrationInstanceID sets the instance ID for the new orchestration.
func WithEntityStartOrchestrationInstanceID(instanceID string) EntityStartOrchestrationOption {
	return func(opts *entityStartOrchestrationOptions, _ api.DataConverter) error {
		opts.instanceID = instanceID
		return nil
	}
}

// WithEntityStartOrchestrationVersion sets the version for the new orchestration.
func WithEntityStartOrchestrationVersion(version string) EntityStartOrchestrationOption {
	return func(opts *entityStartOrchestrationOptions, _ api.DataConverter) error {
		opts.version = wrapperspb.String(version)
		return nil
	}
}

// WithEntityStartOrchestrationScheduledTime schedules the new orchestration.
func WithEntityStartOrchestrationScheduledTime(scheduledTime time.Time) EntityStartOrchestrationOption {
	return func(opts *entityStartOrchestrationOptions, _ api.DataConverter) error {
		if scheduledTime.IsZero() {
			return fmt.Errorf("scheduled orchestration time must not be zero")
		}
		opts.scheduledTime = scheduledTime
		return nil
	}
}

// callEntityOption is a functional option type for the CallEntity orchestrator method.
type callEntityOption func(*callEntityOptions, api.DataConverter) error

type callEntityOptions struct {
	rawInput *wrapperspb.StringValue
}

// WithEntityInput configures an input for an entity operation invocation.
func WithEntityInput(input any) callEntityOption {
	return func(opt *callEntityOptions, converter api.DataConverter) error {
		rawInput, err := marshalEntityInput(converter, input)
		if err != nil {
			return err
		}
		opt.rawInput = rawInput
		return nil
	}
}

// WithRawEntityInput configures a raw input for an entity operation invocation.
func WithRawEntityInput(input string) callEntityOption {
	return func(opt *callEntityOptions, _ api.DataConverter) error {
		opt.rawInput = wrapperspb.String(input)
		return nil
	}
}

// signalEntityOption is a functional option type for the SignalEntity orchestrator method.
type signalEntityOption func(*signalEntityOptions, api.DataConverter) error

type signalEntityOptions struct {
	rawInput      *wrapperspb.StringValue
	scheduledTime *timestamppb.Timestamp
}

// WithSignalEntityInput configures an input for a signal entity invocation.
func WithSignalEntityInput(input any) signalEntityOption {
	return func(opt *signalEntityOptions, converter api.DataConverter) error {
		rawInput, err := marshalEntityInput(converter, input)
		if err != nil {
			return err
		}
		opt.rawInput = rawInput
		return nil
	}
}

// OptionalEntityInput binds an entity method input that may be omitted by the caller.
type OptionalEntityInput[T any] struct {
	Value   T
	Present bool
}

// Or returns the input value when present, or defaultValue when absent.
func (input OptionalEntityInput[T]) Or(defaultValue T) T {
	if input.Present {
		return input.Value
	}
	return defaultValue
}

type optionalEntityInputBinder interface {
	bindEntityInput(*EntityContext) error
}

func (input *OptionalEntityInput[T]) bindEntityInput(ctx *EntityContext) error {
	input.Present = ctx.HasInput()
	if !input.Present {
		return nil
	}
	return ctx.GetInput(&input.Value)
}

func marshalEntityInput(converter api.DataConverter, input any) (*wrapperspb.StringValue, error) {
	if isNilEntityValue(input) {
		return nil, nil
	}
	payload, err := marshalData(converter, input)
	if err != nil {
		return nil, err
	}
	return wrapperspb.String(string(payload)), nil
}

func isNilEntityValue(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	return isNilable(reflected.Kind()) && reflected.IsNil()
}

// WithRawSignalEntityInput configures a raw input for a signal entity invocation.
func WithRawSignalEntityInput(input string) signalEntityOption {
	return func(opt *signalEntityOptions, _ api.DataConverter) error {
		opt.rawInput = wrapperspb.String(input)
		return nil
	}
}

// WithSignalEntityScheduledTime configures a scheduled time for an entity signal.
func WithSignalEntityScheduledTime(scheduledTime time.Time) signalEntityOption {
	return func(opt *signalEntityOptions, _ api.DataConverter) error {
		if scheduledTime.IsZero() {
			return fmt.Errorf("scheduled entity signal time must not be zero")
		}
		opt.scheduledTime = timestamppb.New(scheduledTime)
		return nil
	}
}

var entityActionNamespace = uuid.MustParse("bd4e8d71-45f8-5f6d-a302-b9a785e665c6")

func timestampOrNil(value time.Time) *timestamppb.Timestamp {
	if value.IsZero() {
		return nil
	}
	return timestamppb.New(value)
}
