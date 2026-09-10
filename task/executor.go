package task

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"runtime/debug"
	"strings"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/contextprop"
	"github.com/microsoft/durabletask-go/internal/failure"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type taskExecutor struct {
	Registry                 *TaskRegistry
	versioning               *VersioningOptions
	orchestratorNotFound     OrchestratorNotFoundStrategy
	orchestrationOptions     OrchestrationOptions
	logger                   *slog.Logger
	metrics                  MetricsHooks
	contextFields            api.ContextFields
	errorProperties          api.ErrorPropertiesProvider
	converter                api.DataConverter
	unversionedOrchestrators map[string]struct{}
	unversionedActivities    map[string]struct{}
}

// TaskExecutorOption configures the in-memory task executor.
type TaskExecutorOption func(*taskExecutor)

// OrchestratorNotFoundStrategy determines whether missing orchestrators fail or reject work items.
type OrchestratorNotFoundStrategy int

const (
	// OrchestratorNotFoundFail completes the orchestration with an OrchestratorTaskNotFound failure.
	OrchestratorNotFoundFail OrchestratorNotFoundStrategy = iota
	// OrchestratorNotFoundReject abandons the work item so another worker or deployment can process it.
	OrchestratorNotFoundReject
)

// WithVersioning configures version-aware orchestration and activity dispatch.
func WithVersioning(options VersioningOptions) TaskExecutorOption {
	return func(executor *taskExecutor) {
		executor.versioning = &options
	}
}

// WithUnversionedOrchestratorNames allows named system orchestrators to bypass
// worker version matching when their work item explicitly selects no version.
func WithUnversionedOrchestratorNames(names ...string) TaskExecutorOption {
	return func(executor *taskExecutor) {
		addUnversionedTaskNames(&executor.unversionedOrchestrators, names)
	}
}

// WithUnversionedActivityNames allows named system activities to bypass worker
// version matching when their work item explicitly selects no version. System
// orchestrations run unversioned, and an activity inherits its caller's version,
// so their activities are dispatched unversioned as well.
func WithUnversionedActivityNames(names ...string) TaskExecutorOption {
	return func(executor *taskExecutor) {
		addUnversionedTaskNames(&executor.unversionedActivities, names)
	}
}

func addUnversionedTaskNames(allowed *map[string]struct{}, names []string) {
	if *allowed == nil {
		*allowed = make(map[string]struct{}, len(names))
	}
	for _, name := range names {
		if name = strings.TrimSpace(name); name != "" {
			(*allowed)[strings.ToLower(name)] = struct{}{}
		}
	}
}

func allowsUnversionedTask(allowed map[string]struct{}, name, version string) bool {
	if version != "" {
		return false
	}
	_, ok := allowed[strings.ToLower(name)]
	return ok
}

// WithErrorPropertiesProvider configures custom durable failure properties.
func WithErrorPropertiesProvider(provider api.ErrorPropertiesProvider) TaskExecutorOption {
	return func(executor *taskExecutor) {
		executor.errorProperties = provider
	}
}

// WithDataConverter configures application payload serialization.
func WithDataConverter(converter api.DataConverter) TaskExecutorOption {
	return func(executor *taskExecutor) {
		executor.converter = api.NormalizeDataConverter(converter)
	}
}

// WithOrchestratorNotFoundStrategy configures missing-orchestrator handling.
func WithOrchestratorNotFoundStrategy(strategy OrchestratorNotFoundStrategy) TaskExecutorOption {
	return func(executor *taskExecutor) {
		executor.orchestratorNotFound = strategy
	}
}

// WithOrchestrationOptions configures deterministic orchestration engine
// policies. It panics when a duration or event limit is negative.
func WithOrchestrationOptions(options OrchestrationOptions) TaskExecutorOption {
	return func(executor *taskExecutor) {
		executor.orchestrationOptions = normalizeOrchestrationOptions(options)
	}
}

// WithMaximumTimerInterval configures the maximum duration of one physical
// durable timer action. Zero restores [DefaultMaximumTimerInterval]. A negative
// interval panics when the option is applied.
func WithMaximumTimerInterval(interval time.Duration) TaskExecutorOption {
	return func(executor *taskExecutor) {
		options := executor.orchestrationOptions
		options.MaximumTimerInterval = interval
		executor.orchestrationOptions = normalizeOrchestrationOptions(options)
	}
}

// WithLogger configures the slog logger exposed to orchestration and activity code.
func WithLogger(logger *slog.Logger) TaskExecutorOption {
	return func(executor *taskExecutor) {
		if logger != nil {
			executor.logger = logger
		}
	}
}

// WithMetricsHooks configures optional transport-neutral metric callbacks.
func WithMetricsHooks(hooks MetricsHooks) TaskExecutorOption {
	return func(executor *taskExecutor) {
		executor.metrics = hooks
	}
}

// WithContextFields configures worker-local fields for activity and entity
// contexts. Root-orchestration fields must use [api.WithContextFields], and
// child fields must use [WithSubOrchestrationContextFields], so they are
// persisted and remain stable across replay and worker deployments.
func WithContextFields(fields api.ContextFields) TaskExecutorOption {
	return func(executor *taskExecutor) {
		executor.contextFields = make(api.ContextFields, len(fields))
		maps.Copy(executor.contextFields, fields)
	}
}

// NewTaskExecutor returns a [Executor] implementation that executes orchestrator and activity functions in-memory.
func NewTaskExecutor(registry *TaskRegistry, opts ...TaskExecutorOption) Executor {
	executor := &taskExecutor{
		Registry:             registry,
		orchestrationOptions: normalizeOrchestrationOptions(OrchestrationOptions{}),
		logger:               slog.Default(),
		converter:            api.DefaultDataConverter(),
	}
	for _, configure := range opts {
		configure(executor)
	}
	return executor
}

// ExecuteActivity implements Executor and executes an activity function in the current goroutine.
func (te *taskExecutor) ExecuteActivity(ctx context.Context, id api.InstanceID, e *protos.HistoryEvent) (response *protos.HistoryEvent, err error) {
	ts := e.GetTaskScheduled()
	if ts == nil {
		// No clean way to deal with this other than to abandon it
		return nil, fmt.Errorf("unexpected event type for ExecuteActivity: %v", e.EventType)
	}
	activityVersion := ts.GetVersion().GetValue()
	if versionErr := te.versioning.check(activityVersion); versionErr != nil &&
		!allowsUnversionedTask(te.unversionedActivities, ts.GetName(), activityVersion) {
		if te.versioning.FailureStrategy == VersionFailureReject {
			return nil, versionErr
		}
		return helpers.NewTaskFailedEvent(e.EventId, versionFailureDetails(versionErr)), nil
	}
	ctx = api.ContextWithFields(ctx, te.contextFields)
	ctx = helpers.ContextWithTraceContext(ctx, ts.GetParentTraceContext())
	tagInfo, tagFields := contextprop.Decode(ts.GetTags())
	ctx = api.ContextWithFields(ctx, tagFields)
	orchestrationInfo, _ := api.OrchestrationContextInfoFromContext(ctx)
	if orchestrationInfo.Name == "" {
		orchestrationInfo.Name = tagInfo.Name
	}
	if orchestrationInfo.Version == "" {
		orchestrationInfo.Version = tagInfo.Version
	}
	if orchestrationInfo.ParentInstanceID == "" {
		orchestrationInfo.ParentInstanceID = tagInfo.ParentInstanceID
	}
	if orchestrationInfo.InstanceID == "" {
		orchestrationInfo.InstanceID = tagInfo.InstanceID
		if orchestrationInfo.InstanceID == "" {
			orchestrationInfo.InstanceID = id
		}
	}
	ctx = api.WithOrchestrationContextInfo(ctx, orchestrationInfo)
	ctx = api.WithActivityContextInfo(ctx, api.ActivityContextInfo{
		InstanceID: id,
		Name:       ts.Name,
		Version:    ts.GetVersion().GetValue(),
		TaskID:     e.EventId,
	})
	ctx = withActivityLogger(ctx, te.logger)
	invoker, ok := te.Registry.getActivity(ts.Name, ts.GetVersion().GetValue())
	if !ok {
		notFound := newTaskNotRegisteredError(
			activityTaskNotFoundErrorType,
			ts.Name,
			ts.GetVersion().GetValue(),
		)
		return helpers.NewTaskFailedEvent(e.EventId, failure.FromError(notFound, nil)), nil
	}
	activityCtx := newTaskActivityContext(ctx, e.EventId, ts, te.converter)

	// Recovery must not re-enter application failure providers or panic causes.
	defer func() {
		panicVal := recover()
		if panicVal != nil {
			details := failure.ToProto(&api.FailureDetails{
				ErrorType:    api.ErrorTypeActivityPanic,
				ErrorMessage: fmt.Sprintf("panic: %v", panicVal),
				StackTrace:   string(debug.Stack()),
			})
			response = helpers.NewTaskFailedEvent(e.EventId, details)
			err = nil
		}
	}()

	result, err := invoker(activityCtx)
	if err != nil {
		return helpers.NewTaskFailedEvent(e.EventId, failure.FromError(err, te.errorProperties)), nil
	}

	bytes, err := marshalData(te.converter, result)
	if err != nil {
		return helpers.NewTaskFailedEvent(e.EventId, failure.FromError(err, te.errorProperties)), nil
	}
	var rawResult *wrapperspb.StringValue
	if len(bytes) > 0 {
		rawResult = wrapperspb.String(string(bytes))
	}
	return helpers.NewTaskCompletedEvent(e.EventId, rawResult), nil
}

// ExecuteOrchestrator implements Executor and executes an orchestrator function in the current goroutine.
func (te *taskExecutor) ExecuteOrchestrator(
	_ context.Context,
	id api.InstanceID,
	oldEvents []*protos.HistoryEvent,
	newEvents []*protos.HistoryEvent,
	entityParameters *protos.OrchestratorEntityParameters,
) (*ExecutionResults, error) {
	entitiesSupported, err := validateEntityParameters(entityParameters)
	if err != nil {
		return nil, err
	}
	if isRewindRequest(oldEvents, newEvents) {
		return buildRewindResult(id, oldEvents, newEvents)
	}
	started := startedEvent(oldEvents, newEvents)
	name := started.GetName()
	version := started.GetVersion().GetValue()
	if versionErr := te.versioning.check(version); versionErr != nil &&
		!allowsUnversionedTask(te.unversionedOrchestrators, name, version) {
		if te.versioning.FailureStrategy == VersionFailureReject {
			return nil, versionErr
		}
		return &ExecutionResults{
			Response: &protos.OrchestratorResponse{
				InstanceId: string(id),
				Actions: []*protos.OrchestratorAction{
					helpers.NewCompleteOrchestrationAction(
						0,
						protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
						nil,
						nil,
						versionFailureDetails(versionErr),
					),
				},
			},
		}, nil
	}
	if te.orchestratorNotFound == OrchestratorNotFoundReject && name != "" {
		if !te.Registry.hasOrchestrator(name, version) {
			return nil, newTaskNotRegisteredError(
				orchestratorTaskNotFoundErrorType,
				name,
				version,
			)
		}
	}
	orchestrationCtx := newOrchestrationContext(
		te.Registry,
		id,
		oldEvents,
		newEvents,
		te.orchestrationOptions,
		te.logger,
		te.metrics,
		te.errorProperties,
		te.versioning.defaultVersion(),
		te.converter,
		entitiesSupported,
	)
	actions := orchestrationCtx.start()
	te.reportHistoryMetric(orchestrationCtx)

	results := &ExecutionResults{
		Response: &protos.OrchestratorResponse{
			InstanceId:   string(id),
			Actions:      actions,
			CustomStatus: wrapperspb.String(orchestrationCtx.customStatus),
		},
	}
	if orchestrationCtx.maxEventsPerTurnExceeded && !orchestrationCtx.hasCompletionAction() {
		results.Response.NumEventsProcessed = wrapperspb.Int32(int32(orchestrationCtx.processedEventsThisTurn))
	}
	return results, nil
}

func validateEntityParameters(parameters *protos.OrchestratorEntityParameters) (bool, error) {
	if parameters == nil {
		return false, nil
	}
	window := parameters.EntityMessageReorderWindow
	if window == nil {
		return false, &unsupportedEntityParametersError{
			message: "entity message reorder window must be specified",
		}
	}
	if err := window.CheckValid(); err != nil {
		return false, &unsupportedEntityParametersError{
			message: fmt.Sprintf("invalid entity message reorder window: %v", err),
		}
	}
	duration := window.AsDuration()
	switch {
	case duration < 0:
		return false, &unsupportedEntityParametersError{
			message: "entity message reorder window must not be negative",
		}
	case duration > 0:
		return false, &unsupportedEntityParametersError{
			message: "positive entity message reorder windows are not supported by the DTS protocol",
		}
	default:
		return true, nil
	}
}

// ExecuteEntity executes an entity batch in the current goroutine.
func (te *taskExecutor) ExecuteEntity(
	ctx context.Context,
	req *protos.EntityBatchRequest,
) (result *protos.EntityBatchResult, err error) {
	if req == nil {
		return nil, fmt.Errorf("entity batch request must not be nil")
	}
	entityID, err := api.EntityIDFromString(req.InstanceId)
	if err != nil {
		return nil, fmt.Errorf("invalid entity instance ID: %w", err)
	}
	factory, ok := te.Registry.getEntityFactory(entityID.Name)
	if !ok {
		result := &protos.EntityBatchResult{
			EntityState: req.EntityState,
			Results:     make([]*protos.OperationResult, 0, len(req.Operations)),
		}
		for range req.Operations {
			now := time.Now().UTC()
			failureResult := entityOperationFailure(
				newTaskNotRegisteredError(
					entityTaskNotFoundErrorType,
					entityID.Name,
					"",
				),
				now,
				now,
				te.errorProperties,
			)
			result.Results = append(result.Results, failureResult)
		}
		return result, nil
	}
	batch, err := factory(EntityFactoryContext{Context: ctx, ID: entityID})
	if err != nil {
		return nil, fmt.Errorf("failed to create entity %q: %w", entityID.Name, err)
	}
	if batch.Close != nil {
		defer func() {
			if closeErr := batch.Close(ctx); closeErr != nil {
				result = nil
				err = fmt.Errorf("failed to close entity batch %q: %w", entityID.Name, closeErr)
			}
		}()
	}
	if batch.Entity == nil {
		return nil, fmt.Errorf("entity factory %q returned a nil implementation", entityID.Name)
	}

	var state entityState
	if req.EntityState != nil {
		state = entityState{value: []byte(req.EntityState.Value), hasValue: true}
	}
	result = &protos.EntityBatchResult{
		Results: make([]*protos.OperationResult, 0, len(req.Operations)),
		Actions: make([]*protos.OperationAction, 0),
	}
	nextActionID := int32(0)
	for _, operation := range req.Operations {
		if operation == nil {
			result.Results = append(result.Results, entityOperationFailure(
				fmt.Errorf("entity operation request must not be nil"),
				time.Now().UTC(),
				time.Now().UTC(),
				te.errorProperties,
			))
			continue
		}
		startedAt := time.Now().UTC()
		isSignal := req.Properties[helpers.EntitySignalProperty(operation.RequestId)].GetBoolValue()
		operationCtx := te.newEntityContext(ctx, entityID, operation, state, nextActionID, startedAt, isSignal)
		output, operationErr := invokeEntity(batch.Entity, operationCtx)
		endedAt := time.Now().UTC()

		var rawResult *wrapperspb.StringValue
		if operationErr == nil && !isNilEntityValue(output) {
			bytes, marshalErr := marshalData(te.converter, output)
			if marshalErr != nil {
				operationErr = fmt.Errorf("failed to marshal entity result: %w", marshalErr)
			} else if len(bytes) > 0 {
				rawResult = wrapperspb.String(string(bytes))
			}
		}

		if operationErr != nil {
			result.Results = append(result.Results, entityOperationFailure(
				operationErr,
				startedAt,
				endedAt,
				te.errorProperties,
			))
			continue
		}

		state = operationCtx.state
		nextActionID = operationCtx.actionIDSeq
		result.Actions = append(result.Actions, operationCtx.actions...)
		result.Results = append(result.Results, &protos.OperationResult{
			ResultType: &protos.OperationResult_Success{
				Success: &protos.OperationResultSuccess{
					Result:       rawResult,
					StartTimeUtc: timestamppb.New(startedAt),
					EndTimeUtc:   timestamppb.New(endedAt),
				},
			},
		})
	}

	if state.hasValue {
		result.EntityState = wrapperspb.String(string(state.value))
	}
	return result, nil
}

func (te *taskExecutor) newEntityContext(
	ctx context.Context,
	entityID api.EntityID,
	operation *protos.OperationRequest,
	state entityState,
	nextActionID int32,
	currentTime time.Time,
	isSignal bool,
) *EntityContext {
	ctx = api.ContextWithFields(ctx, te.contextFields)
	ctx = api.WithEntityContextInfo(ctx, api.EntityContextInfo{
		EntityID:  entityID,
		Operation: operation.Operation,
		RequestID: operation.RequestId,
		IsSignal:  isSignal,
	})
	logger := te.logger
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With(
		slog.String("durabletask.entity.id", entityID.String()),
		slog.String("durabletask.entity.operation", operation.Operation),
		slog.String("durabletask.entity.request_id", operation.RequestId),
	)
	ctx = context.WithValue(ctx, taskLoggerKey{}, logger)
	return &EntityContext{
		ID:        entityID,
		Operation: operation.Operation,
		RequestID: operation.RequestId,
		IsSignal:  isSignal,
		rawInput: entityPayload{
			value:   []byte(operation.Input.GetValue()),
			present: operation.Input != nil,
		},
		state:       state,
		actionIDSeq: nextActionID,
		currentTime: currentTime,
		ctx:         ctx,
		logger:      logger,
		converter:   te.converter,
	}
}

func invokeEntity(invoker Entity, entityCtx *EntityContext) (result any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = newPanicFailureError(
				api.ErrorTypeEntityOperationPanic,
				fmt.Sprintf("entity operation panic: %v", recovered),
				string(debug.Stack()),
				panicCause(recovered),
			)
		}
	}()
	return invoker(entityCtx)
}

func entityOperationFailure(
	err error,
	startedAt time.Time,
	endedAt time.Time,
	provider api.ErrorPropertiesProvider,
) *protos.OperationResult {
	return &protos.OperationResult{
		ResultType: &protos.OperationResult_Failure{
			Failure: &protos.OperationResultFailure{
				FailureDetails: failure.FromError(err, provider),
				StartTimeUtc:   timestamppb.New(startedAt),
				EndTimeUtc:     timestamppb.New(endedAt),
			},
		},
	}
}

func (te *taskExecutor) reportHistoryMetric(ctx *OrchestrationContext) {
	if te.metrics.History == nil {
		return
	}
	metric := HistoryMetric{
		InstanceID:           ctx.ID,
		OrchestrationName:    ctx.Name,
		OrchestrationVersion: ctx.Version,
		HistoryLength:        ctx.HistoryLength(),
		ProcessedEvents:      ctx.processedEventsThisTurn,
		HistoryLimitExceeded: ctx.HistoryLimitExceeded(),
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			ctx.Logger().Error("history metrics callback panicked", "error", recovered)
		}
	}()
	te.metrics.History(metric)
}

// startedEvent returns the ExecutionStarted event found across the given event lists, or nil if none is present.
func startedEvent(eventLists ...[]*protos.HistoryEvent) *protos.ExecutionStartedEvent {
	for _, events := range eventLists {
		for _, event := range events {
			if started := event.GetExecutionStarted(); started != nil {
				return started
			}
		}
	}
	return nil
}

func versionFailureDetails(err error) *protos.TaskFailureDetails {
	return failure.FromError(err)
}

func (te taskExecutor) Shutdown(ctx context.Context) error {
	// Nothing to do
	return nil
}

func unmarshalData(converter api.DataConverter, data []byte, v any) error {
	if v == nil || len(data) == 0 {
		return nil
	}
	return api.NormalizeDataConverter(converter).Deserialize(string(data), v)
}

func marshalData(converter api.DataConverter, v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	payload, err := api.SerializeData(converter, v)
	if err != nil {
		return nil, err
	}
	return []byte(payload), nil
}
