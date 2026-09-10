package task

import (
	"container/list"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/contextprop"
	"github.com/microsoft/durabletask-go/internal/failure"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/internal/tagcodec"
)

// Orchestrator is the functional interface for orchestrator functions.
type Orchestrator func(ctx *OrchestrationContext) (any, error)

type replayEvent struct {
	event       *protos.HistoryEvent
	isReplaying bool
}

// OrchestrationContext is the parameter type for orchestrator functions.
type OrchestrationContext struct {
	ID             api.InstanceID
	Name           string
	Version        string
	IsReplaying    bool
	CurrentTimeUtc time.Time

	contextFields        api.ContextFields
	errorProperties      api.ErrorPropertiesProvider
	orchestrationTags    map[string]string
	logger               *slog.Logger
	metrics              MetricsHooks
	orchestrationOptions OrchestrationOptions
	parentInstanceID     api.InstanceID
	executionID          string
	registry             *TaskRegistry
	rawInput             []byte
	oldEvents            []*protos.HistoryEvent
	newEvents            []*protos.HistoryEvent
	suspendedEvents      []replayEvent
	resumedEvents        []replayEvent
	isSuspended          bool
	isTerminated         bool
	historyIndex         int
	// processedEventsThisTurn follows DTS numEventsProcessed semantics and
	// excludes orchestration control markers.
	processedEventsThisTurn  int
	maxEventsPerTurnExceeded bool
	maxHistoryEventsExceeded bool
	sequenceNumber           int32
	newGuidCounter           uint64
	pendingActions           map[int32]*protos.OrchestratorAction
	pendingTasks             map[int32]*completableTask
	pendingEntityTasks       map[string]*completableTask
	continuedAsNew           bool
	continuedAsNewInput      any
	continuedAsNewVersion    *wrapperspb.StringValue
	customStatus             string
	defaultVersion           string
	converter                api.DataConverter
	entitiesSupported        bool
	scheduler                *coroutineScheduler
	root                     *OrchestrationContext
	scope                    *cancellationScope
	derived                  []*OrchestrationContext

	bufferedExternalEvents     map[string]*list.List
	pendingExternalEventTasks  map[string]*list.List
	eventChannels              map[string]any
	eventWaiters               map[string]map[*coroutine]struct{}
	saveBufferedExternalEvents bool

	criticalSectionID               string
	criticalSectionLocks            []string
	criticalSectionAvailable        map[string]bool
	criticalSectionRequestCommitted bool
	criticalSectionAbandoned        bool
}

// callSubOrchestratorOptions is a struct that holds the options for the CallSubOrchestrator orchestrator method.
type callSubOrchestratorOptions struct {
	instanceID string
	rawInput   *wrapperspb.StringValue
	version    *wrapperspb.StringValue

	retryPolicy   *RetryPolicy
	tags          map[string]string
	contextFields api.ContextFields
}

func (options *callSubOrchestratorOptions) versionOrDefault(defaultVersion string) *wrapperspb.StringValue {
	if options.version != nil {
		return options.version
	}
	if defaultVersion == "" {
		return nil
	}
	return wrapperspb.String(defaultVersion)
}

// SubOrchestratorOption configures [OrchestrationContext.CallSubOrchestrator].
type SubOrchestratorOption func(*callSubOrchestratorOptions, api.DataConverter) error

// ContinueAsNewOption is a functional option type for the ContinueAsNew orchestrator method.
type ContinueAsNewOption func(*OrchestrationContext)

// WithKeepUnprocessedEvents returns a ContinueAsNewOptions struct that instructs the
// runtime to carry forward any unprocessed external events to the new instance.
func WithKeepUnprocessedEvents() ContinueAsNewOption {
	return func(ctx *OrchestrationContext) {
		ctx.saveBufferedExternalEvents = true
	}
}

// WithContinueAsNewVersion migrates the next execution to a new orchestration version.
func WithContinueAsNewVersion(version string) ContinueAsNewOption {
	return func(ctx *OrchestrationContext) {
		if version != "" && strings.TrimSpace(version) == "" {
			ctx.continuedAsNewVersion = nil
			return
		}
		ctx.continuedAsNewVersion = wrapperspb.String(version)
	}
}

// WithSubOrchestratorInput is a functional option type for the CallSubOrchestrator
// orchestrator method that serializes an input value with the configured converter.
func WithSubOrchestratorInput(input any) SubOrchestratorOption {
	return func(opts *callSubOrchestratorOptions, converter api.DataConverter) error {
		bytes, err := marshalData(converter, input)
		if err != nil {
			return fmt.Errorf("failed to serialize input: %w", err)
		}
		opts.rawInput = wrapperspb.String(string(bytes))
		return nil
	}
}

// WithRawSubOrchestratorInput is a functional option type for the CallSubOrchestrator
// orchestrator method that takes a raw input value.
func WithRawSubOrchestratorInput(input string) SubOrchestratorOption {
	return func(opts *callSubOrchestratorOptions, _ api.DataConverter) error {
		opts.rawInput = wrapperspb.String(input)
		return nil
	}
}

// WithSubOrchestrationInstanceID is a functional option type for the CallSubOrchestrator
// orchestrator method that specifies the instance ID of the sub-orchestration.
func WithSubOrchestrationInstanceID(instanceID string) SubOrchestratorOption {
	return func(opts *callSubOrchestratorOptions, _ api.DataConverter) error {
		opts.instanceID = instanceID
		return nil
	}
}

// WithSubOrchestrationVersion configures the sub-orchestration version.
func WithSubOrchestrationVersion(version string) SubOrchestratorOption {
	return func(opts *callSubOrchestratorOptions, _ api.DataConverter) error {
		opts.version = wrapperspb.String(version)
		return nil
	}
}

// WithSubOrchestrationTags adds user tags to a sub-orchestration.
func WithSubOrchestrationTags(tags map[string]string) SubOrchestratorOption {
	return func(opts *callSubOrchestratorOptions, _ api.DataConverter) error {
		if err := validateUnreservedKeys("sub-orchestration tag", tags); err != nil {
			return err
		}
		opts.tags = maps.Clone(tags)
		return nil
	}
}

// WithSubOrchestrationContextFields adds immutable context fields to a
// sub-orchestration.
func WithSubOrchestrationContextFields(fields api.ContextFields) SubOrchestratorOption {
	return func(opts *callSubOrchestratorOptions, _ api.DataConverter) error {
		if err := validateUnreservedKeys("sub-orchestration context field", fields); err != nil {
			return err
		}
		opts.contextFields = maps.Clone(fields)
		return nil
	}
}

// validateUnreservedKeys rejects empty keys and keys that collide with
// reserved wire prefixes, using kind to build caller-facing error messages.
func validateUnreservedKeys[M ~map[string]string](kind string, values M) error {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if key == "" {
			return fmt.Errorf("%s key cannot be empty", kind)
		}
		if err := checkUnreservedKey(kind, key); err != nil {
			return err
		}
	}
	return nil
}

// checkUnreservedKey rejects caller-supplied keys that collide with the
// reserved prefixes used to carry orchestration context on the wire.
func checkUnreservedKey(kind, key string) error {
	if strings.HasPrefix(key, api.ReservedContextFieldPrefix) ||
		strings.HasPrefix(key, tagcodec.UserTagPrefix) {
		return fmt.Errorf("%s %q uses a reserved prefix", kind, key)
	}
	return nil
}

// WithSubOrchestrationRetryPolicy snapshots policy when this option is created.
// Later caller mutations do not affect sub-orchestration retries.
func WithSubOrchestrationRetryPolicy(policy *RetryPolicy) SubOrchestratorOption {
	if policy == nil {
		return func(*callSubOrchestratorOptions, api.DataConverter) error { return nil }
	}
	snapshot := *policy
	return func(opt *callSubOrchestratorOptions, _ api.DataConverter) error {
		normalized, err := snapshot.Normalized()
		if err != nil {
			return err
		}
		opt.retryPolicy = &normalized
		return nil
	}
}

func newOrchestrationContext(
	registry *TaskRegistry,
	id api.InstanceID,
	oldEvents []*protos.HistoryEvent,
	newEvents []*protos.HistoryEvent,
	options OrchestrationOptions,
	logger *slog.Logger,
	metrics MetricsHooks,
	errorProperties api.ErrorPropertiesProvider,
	defaultVersion string,
	converter api.DataConverter,
	entitiesSupported bool,
) *OrchestrationContext {
	ctx := &OrchestrationContext{
		ID:                        id,
		errorProperties:           errorProperties,
		logger:                    logger,
		metrics:                   metrics,
		orchestrationOptions:      normalizeOrchestrationOptions(options),
		defaultVersion:            defaultVersion,
		converter:                 api.NormalizeDataConverter(converter),
		entitiesSupported:         entitiesSupported,
		registry:                  registry,
		oldEvents:                 oldEvents,
		newEvents:                 newEvents,
		bufferedExternalEvents:    make(map[string]*list.List),
		pendingExternalEventTasks: make(map[string]*list.List),
		eventChannels:             make(map[string]any),
		eventWaiters:              make(map[string]map[*coroutine]struct{}),
	}
	ctx.maxHistoryEventsExceeded = options.MaxHistoryEvents > 0 &&
		len(oldEvents)+len(newEvents) > options.MaxHistoryEvents
	ctx.scope = newCancellationScope(nil)
	return ctx
}

func (ctx *OrchestrationContext) engineContext() *OrchestrationContext {
	if ctx.root != nil {
		return ctx.root
	}
	return ctx
}

func (ctx *OrchestrationContext) syncDerivedContexts() {
	active := ctx.derived[:0]
	for _, derived := range ctx.derived {
		if derived.scope.isCanceled() {
			continue
		}
		derived.ID = ctx.ID
		derived.Name = ctx.Name
		derived.Version = ctx.Version
		derived.IsReplaying = ctx.IsReplaying
		derived.CurrentTimeUtc = ctx.CurrentTimeUtc
		derived.executionID = ctx.executionID
		derived.scheduler = ctx.scheduler
		active = append(active, derived)
	}
	ctx.derived = active
}

// WithCancel creates a child orchestration context whose tasks, nested scopes,
// and coroutines are canceled together at the next scheduler step.
// A child of an already-canceled scope is canceled immediately.
func (ctx *OrchestrationContext) WithCancel() (*OrchestrationContext, func()) {
	engine := ctx.engineContext()
	if engine.scheduler == nil {
		panic("cancellation scope created outside orchestrator execution")
	}
	child := &OrchestrationContext{
		ID:             engine.ID,
		Name:           engine.Name,
		Version:        engine.Version,
		IsReplaying:    engine.IsReplaying,
		CurrentTimeUtc: engine.CurrentTimeUtc,
		root:           engine,
		scope:          newCancellationScope(ctx.scope),
		scheduler:      engine.scheduler,
	}
	if !child.scope.isCanceled() {
		engine.derived = append(engine.derived, child)
	}
	cancel := func() {
		scheduler := engine.scheduler
		if scheduler == nil {
			panic("orchestration cancel called outside orchestrator execution")
		}
		if scheduler.isStopping() {
			return
		}
		scheduler.mustCurrent()
		scheduler.requestCancellation(child.scope)
	}
	return child, cancel
}

func (ctx *OrchestrationContext) start() (actions []*protos.OrchestratorAction) {
	ctx.historyIndex = 0
	ctx.processedEventsThisTurn = 0
	ctx.sequenceNumber = 0
	ctx.newGuidCounter = 0
	ctx.pendingActions = make(map[int32]*protos.OrchestratorAction)
	ctx.pendingTasks = make(map[int32]*completableTask)
	ctx.pendingEntityTasks = make(map[string]*completableTask)
	ctx.clearCriticalSection()
	ctx.scheduler = newCoroutineScheduler(ctx)
	defer func() {
		ctx.scheduler.shutdown()
		ctx.scheduler = nil
	}()
	defer func() {
		if value := recover(); value != nil {
			ctx.continuedAsNew = false
			ctx.isSuspended = false
			ctx.clearCompletionActions()
			// Do not re-enter the converter or failure provider that panicked.
			panicErr := newPanicFailureError(
				api.ErrorTypeOrchestratorPanic,
				fmt.Sprintf("orchestration execution panicked: %v", value),
				string(debug.Stack()),
				nil,
			)
			_ = ctx.setCompleteInternal(
				nil,
				protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
				failure.FromError(panicErr),
			)
			actions = ctx.actions()
		}
	}()

	terminal := false
	markTerminal := func() {
		terminal = true
		ctx.scheduler.shutdown()
	}
	for {
		if !terminal && ctx.scheduler.terminalErr != nil {
			_ = ctx.setFailed(ctx.scheduler.terminalErr)
			markTerminal()
		}

		if !terminal && ctx.scheduler.hasRunnable() {
			ctx.scheduler.runNext()
			if ctx.scheduler.terminalErr != nil {
				continue
			}
			if ctx.scheduler.isRootCompleted() && !ctx.scheduler.rootFinalized {
				ctx.scheduler.rootFinalized = true
				if err := ctx.completeRootCoroutine(); err != nil {
					_ = ctx.setFailed(err)
				}
				markTerminal()
			}
			continue
		}

		ok, err := ctx.processNextEvent()
		if err != nil {
			_ = ctx.setFailed(err)
			markTerminal()
			break
		}
		if !ok {
			break
		}
		if ctx.isTerminated && !terminal {
			markTerminal()
		}
	}
	if ctx.maxHistoryEventsExceeded &&
		!ctx.continuedAsNew &&
		!ctx.isTerminated &&
		!ctx.hasCompletionAction() {
		ctx.enforceHistoryLimit()
	}
	return ctx.actions()
}

func (ctx *OrchestrationContext) completeRootCoroutine() error {
	switch {
	case ctx.scheduler.rootErr != nil:
		return ctx.setFailed(ctx.scheduler.rootErr)
	case ctx.continuedAsNew:
		return ctx.setContinuedAsNew()
	default:
		return ctx.setComplete(ctx.scheduler.rootResult)
	}
}

func (ctx *OrchestrationContext) processNextEvent() (bool, error) {
	e, ok := ctx.getNextHistoryEvent()
	if !ok {
		// No more history
		return false, nil
	}

	if err := ctx.processEvent(e); err != nil {
		// Internal failure processing event
		return true, err
	}
	return true, nil
}

func (ctx *OrchestrationContext) getNextHistoryEvent() (*protos.HistoryEvent, bool) {
	if len(ctx.resumedEvents) > 0 {
		next := ctx.resumedEvents[0]
		ctx.resumedEvents = ctx.resumedEvents[1:]
		ctx.IsReplaying = next.isReplaying
		return next.event, true
	}

	var historyList []*protos.HistoryEvent
	index := ctx.historyIndex
	switch {
	case ctx.historyIndex >= len(ctx.oldEvents)+len(ctx.newEvents):
		return nil, false
	case ctx.historyIndex < len(ctx.oldEvents):
		ctx.IsReplaying = true
		historyList = ctx.oldEvents
	default:
		ctx.IsReplaying = false
		historyList = ctx.newEvents
		index -= len(ctx.oldEvents)
	}

	event := historyList[index]
	if !ctx.IsReplaying && countsTowardTurnBudget(event) {
		if ctx.orchestrationOptions.MaxEventsPerTurn > 0 &&
			ctx.processedEventsThisTurn >= ctx.orchestrationOptions.MaxEventsPerTurn {
			ctx.maxEventsPerTurnExceeded = true
			return nil, false
		}
		ctx.processedEventsThisTurn++
	}

	ctx.historyIndex++
	return event, true
}

func countsTowardTurnBudget(event *protos.HistoryEvent) bool {
	return event.GetOrchestratorStarted() == nil &&
		event.GetOrchestratorCompleted() == nil &&
		event.GetGenericEvent() == nil &&
		event.GetExecutionRewound() == nil &&
		event.GetExecutionTerminated() == nil &&
		event.GetExecutionSuspended() == nil &&
		event.GetExecutionResumed() == nil
}

// HistoryLength returns the total old and new history supplied to this execution.
func (ctx *OrchestrationContext) HistoryLength() int {
	engine := ctx.engineContext()
	return len(engine.oldEvents) + len(engine.newEvents)
}

// HistoryLimitExceeded reports whether MaxHistoryEvents was exceeded.
func (ctx *OrchestrationContext) HistoryLimitExceeded() bool {
	engine := ctx.engineContext()
	return engine.maxHistoryEventsExceeded
}

func (ctx *OrchestrationContext) processEvent(e *protos.HistoryEvent) error {
	defer ctx.syncDerivedContexts()
	// Buffer certain events if we're in a suspended state
	if ctx.isSuspended && (e.GetExecutionResumed() == nil && e.GetExecutionTerminated() == nil) {
		if e.GetExecutionSuspended() != nil {
			// A redundant suspend is a no-op. Buffering it would re-suspend the
			// orchestration when the matching resume drains the buffer.
			return nil
		}
		ctx.suspendedEvents = append(ctx.suspendedEvents, replayEvent{
			event:       e,
			isReplaying: ctx.IsReplaying,
		})
		return nil
	}

	var err error = nil
	if os := e.GetOrchestratorStarted(); os != nil {
		// OrchestratorStarted is only used to update the current orchestration time
		ctx.CurrentTimeUtc = e.Timestamp.AsTime()
	} else if es := e.GetExecutionStarted(); es != nil {
		err = ctx.onExecutionStarted(es)
	} else if ts := e.GetTaskScheduled(); ts != nil {
		err = ctx.onTaskScheduled(e.EventId, ts)
	} else if tc := e.GetTaskCompleted(); tc != nil {
		err = ctx.onTaskCompleted(tc.TaskScheduledId, tc.Result)
	} else if tf := e.GetTaskFailed(); tf != nil {
		err = ctx.onTaskFailed(tf.TaskScheduledId, tf.FailureDetails)
	} else if ts := e.GetSubOrchestrationInstanceCreated(); ts != nil {
		err = ctx.onSubOrchestrationScheduled(e.EventId, ts)
	} else if sc := e.GetSubOrchestrationInstanceCompleted(); sc != nil {
		err = ctx.onTaskCompleted(sc.TaskScheduledId, sc.Result)
	} else if sf := e.GetSubOrchestrationInstanceFailed(); sf != nil {
		err = ctx.onTaskFailed(sf.TaskScheduledId, sf.FailureDetails)
	} else if tc := e.GetTimerCreated(); tc != nil {
		err = ctx.onTimerCreated(e)
	} else if tf := e.GetTimerFired(); tf != nil {
		err = ctx.onTimerFired(tf)
	} else if es := e.GetEventSent(); es != nil {
		err = ctx.onEventSent(e.EventId, es)
	} else if signaled := e.GetEntityOperationSignaled(); signaled != nil {
		err = ctx.onEntityOperationSent(e.EventId, signaled.RequestId)
	} else if called := e.GetEntityOperationCalled(); called != nil {
		err = ctx.onEntityOperationSent(e.EventId, called.RequestId)
	} else if completed := e.GetEntityOperationCompleted(); completed != nil {
		err = ctx.onEntityOperationCompleted(completed)
	} else if failed := e.GetEntityOperationFailed(); failed != nil {
		err = ctx.onEntityOperationFailed(failed)
	} else if requested := e.GetEntityLockRequested(); requested != nil {
		err = ctx.onEntityLockRequested(e.EventId, requested)
	} else if granted := e.GetEntityLockGranted(); granted != nil {
		err = ctx.onEntityLockGranted(granted)
	} else if unlocked := e.GetEntityUnlockSent(); unlocked != nil {
		err = ctx.onEntityUnlockSent(e.EventId, unlocked)
	} else if er := e.GetEventRaised(); er != nil {
		err = ctx.onExternalEventRaised(e)
	} else if es := e.GetExecutionSuspended(); es != nil {
		err = ctx.onExecutionSuspended(es)
	} else if er := e.GetExecutionResumed(); er != nil {
		err = ctx.onExecutionResumed(er)
	} else if et := e.GetExecutionTerminated(); et != nil {
		err = ctx.onExecutionTerminated(et)
	} else if oc := e.GetOrchestratorCompleted(); oc != nil {
		// Nothing to do
	} else if e.GetGenericEvent() != nil || e.GetExecutionRewound() != nil {
		// Service control markers are replay no-ops.
	} else {
		err = fmt.Errorf("don't know how to handle event: %v", e)
	}
	return err
}

// SetCustomStatus stores a raw, pre-serialized custom status string.
// Use SetCustomStatusValue to apply the configured data converter.
func (octx *OrchestrationContext) SetCustomStatus(cs string) {
	octx.engineContext().customStatus = cs
}

// SetCustomStatusValue serializes and stores a typed custom status value.
func (octx *OrchestrationContext) SetCustomStatusValue(value any) error {
	engine := octx.engineContext()
	payload, err := api.SerializeData(engine.converter, value)
	if err != nil {
		return fmt.Errorf("failed to serialize custom status: %w", err)
	}
	engine.customStatus = payload
	return nil
}

// SetRawCustomStatus stores a pre-serialized custom status value.
func (octx *OrchestrationContext) SetRawCustomStatus(payload string) {
	octx.engineContext().customStatus = payload
}

var guidNamespace = uuid.MustParse("9e952958-5e33-4daf-827f-2fa12937b875")

// NewGuid returns a deterministic UUID that is stable across orchestration replay.
func (ctx *OrchestrationContext) NewGuid() string {
	engine := ctx.engineContext()
	timestamp := engine.CurrentTimeUtc.UTC().Format("2006-01-02T15:04:05.0000000Z")
	name := fmt.Sprintf("%s_%s_%d", engine.ID, timestamp, engine.newGuidCounter)
	engine.newGuidCounter++
	return uuid.NewSHA1(guidNamespace, []byte(name)).String()
}

// GetInput unmarshals the serialized orchestration input and stores it in [v].
func (octx *OrchestrationContext) GetInput(v any) error {
	engine := octx.engineContext()
	return unmarshalData(engine.converter, engine.rawInput, v)
}

// CallActivity schedules an asynchronous invocation of an activity function. The [activity]
// parameter can be either the name of an activity as a string or can be a pointer to the function
// that implements the activity, in which case the name is obtained via reflection.
func (ctx *OrchestrationContext) CallActivity(activity any, opts ...CallActivityOption) Task {
	engine := ctx.engineContext()
	options := new(callActivityOptions)
	for _, configure := range opts {
		if err := configure(options, engine.converter); err != nil {
			return ctx.newFailedTask(engine, api.WrapInvalidArgument(err))
		}
	}
	if ctx.scope.isCanceled() {
		return newTaskInScope(engine, ctx.scope)
	}

	if options.retryPolicy != nil {
		retryInfo := retryTaskInfo{
			kind:    WorkItemKindActivity,
			name:    helpers.GetTaskFunctionName(activity),
			version: options.versionOrInherited(engine.Version).GetValue(),
		}
		return engine.internalScheduleTaskWithRetries(engine.CurrentTimeUtc, func() Task {
			return engine.internalScheduleActivity(activity, options, ctx.scope)
		}, *options.retryPolicy, 0, ctx, retryInfo)
	}

	return engine.internalScheduleActivity(activity, options, ctx.scope)
}

// newFailedTask creates a completed, already-failed task in the calling context's
// cancellation scope. Used when option validation fails before any action is scheduled.
func (ctx *OrchestrationContext) newFailedTask(engine *OrchestrationContext, err error) Task {
	failedTask := newTaskInScope(engine, ctx.scope)
	failedTask.failLocal(err)
	return failedTask
}

// Go starts a coroutine that is cooperatively scheduled with the orchestration.
// Only one orchestration coroutine runs at a time, in monotonically increasing ID order.
func (ctx *OrchestrationContext) Go(fn func(ctx *OrchestrationContext)) {
	if fn == nil {
		panic("orchestration coroutine function must be non-nil")
	}
	engine := ctx.engineContext()
	if engine.scheduler == nil {
		panic("orchestration coroutine started outside orchestrator execution")
	}
	engine.scheduler.spawn(ctx, func() {
		fn(ctx)
	})
}

// NewWaitGroup creates a deterministic coroutine wait group.
func (ctx *OrchestrationContext) NewWaitGroup() WaitGroup {
	engine := ctx.engineContext()
	if engine.scheduler == nil {
		panic("orchestration wait group created outside orchestrator execution")
	}
	return newOrchestrationWaitGroup(engine.scheduler)
}

func (ctx *OrchestrationContext) internalScheduleActivity(
	activity any,
	options *callActivityOptions,
	scope *cancellationScope,
) Task {
	if scope.isCanceled() {
		return newTaskInScope(ctx, scope)
	}
	scheduleTaskAction := helpers.NewScheduleTaskAction(
		ctx.getNextSequenceNumber(),
		helpers.GetTaskFunctionName(activity),
		options.rawInput,
		options.versionOrInherited(ctx.Version))
	scheduleTaskAction.GetScheduleTask().Tags = contextprop.Encode(api.OrchestrationContextInfo{
		InstanceID:       ctx.ID,
		Name:             ctx.Name,
		Version:          ctx.Version,
		ParentInstanceID: ctx.parentInstanceID,
	}, ctx.contextFields, mergeStringMaps(ctx.orchestrationTags, options.tags))

	ctx.pendingActions[scheduleTaskAction.Id] = scheduleTaskAction

	task := newTaskInScope(ctx, scope)
	task.taskName = scheduleTaskAction.GetScheduleTask().GetName()
	task.taskVersion = scheduleTaskAction.GetScheduleTask().GetVersion().GetValue()
	task.taskID = scheduleTaskAction.Id
	ctx.pendingTasks[scheduleTaskAction.Id] = task
	return task
}

func (ctx *OrchestrationContext) CallSubOrchestrator(orchestrator any, opts ...SubOrchestratorOption) Task {
	engine := ctx.engineContext()
	if engine.criticalSectionID != "" {
		return ctx.newFailedTask(engine, fmt.Errorf("sub-orchestrations cannot be started while holding entity locks"))
	}
	options := new(callSubOrchestratorOptions)
	for _, configure := range opts {
		if err := configure(options, engine.converter); err != nil {
			return ctx.newFailedTask(engine, api.WrapInvalidArgument(err))
		}
	}
	if ctx.scope.isCanceled() {
		return newTaskInScope(engine, ctx.scope)
	}

	if options.retryPolicy != nil {
		retryInfo := retryTaskInfo{
			kind:    WorkItemKindOrchestration,
			name:    helpers.GetTaskFunctionName(orchestrator),
			version: options.versionOrDefault(engine.defaultVersion).GetValue(),
		}
		return engine.internalScheduleTaskWithRetries(engine.CurrentTimeUtc, func() Task {
			return engine.internalCallSubOrchestrator(orchestrator, options, ctx.scope)
		}, *options.retryPolicy, 0, ctx, retryInfo)
	}

	return engine.internalCallSubOrchestrator(orchestrator, options, ctx.scope)
}

func (ctx *OrchestrationContext) internalCallSubOrchestrator(
	orchestrator any,
	options *callSubOrchestratorOptions,
	scope *cancellationScope,
) Task {
	if scope.isCanceled() {
		return newTaskInScope(ctx, scope)
	}
	createSubOrchestrationAction := helpers.NewCreateSubOrchestrationAction(
		ctx.getNextSequenceNumber(),
		helpers.GetTaskFunctionName(orchestrator),
		options.instanceID,
		options.rawInput,
		options.versionOrDefault(ctx.defaultVersion),
	)
	createSubOrchestrationAction.GetCreateSubOrchestration().Tags = contextprop.Encode(
		api.OrchestrationContextInfo{
			InstanceID:       ctx.ID,
			Name:             ctx.Name,
			Version:          ctx.Version,
			ParentInstanceID: ctx.parentInstanceID,
		},
		mergeStringMaps(ctx.contextFields, options.contextFields),
		mergeStringMaps(ctx.orchestrationTags, options.tags),
	)
	if createSubOrchestrationAction.GetCreateSubOrchestration().GetInstanceId() == "" {
		createSubOrchestrationAction.GetCreateSubOrchestration().InstanceId = fmt.Sprintf(
			"%s:%04x",
			ctx.ID,
			createSubOrchestrationAction.Id,
		)
	}
	ctx.pendingActions[createSubOrchestrationAction.Id] = createSubOrchestrationAction

	task := newTaskInScope(ctx, scope)
	task.taskName = createSubOrchestrationAction.GetCreateSubOrchestration().GetName()
	task.taskVersion = createSubOrchestrationAction.GetCreateSubOrchestration().GetVersion().GetValue()
	task.taskID = createSubOrchestrationAction.Id
	ctx.pendingTasks[createSubOrchestrationAction.Id] = task
	return task
}

func (ctx *OrchestrationContext) internalScheduleTaskWithRetries(
	initialAttempt time.Time,
	schedule func() Task,
	policy RetryPolicy,
	retryCount int,
	owner *OrchestrationContext,
	retryInfo retryTaskInfo,
) Task {
	result := newTaskInScope(ctx, owner.scope)
	attempt := schedule()
	ctx.scheduler.spawn(owner, func() {
		current := attempt
		count := retryCount
		for {
			err := current.Await(nil)
			if err == nil {
				result.completeFrom(current, nil)
				return
			}
			if count+1 >= policy.MaxAttempts {
				result.completeFrom(current, err)
				return
			}

			nextDelay := computeNextDelay(ctx.CurrentTimeUtc, policy, count, initialAttempt, err)
			if nextDelay == 0 {
				result.completeFrom(current, err)
				return
			}
			ctx.reportRetry(retryInfo, count+1, policy, nextDelay, err)
			if timerErr := ctx.createTimerInternal(nextDelay, owner.scope).Await(nil); timerErr != nil {
				if errors.Is(timerErr, ErrTaskCanceled) {
					result.cancel()
				} else {
					result.failLocal(errors.Join(timerErr, err))
				}
				return
			}
			count++
			current = schedule()
		}
	})
	return result
}

func (t *completableTask) completeFrom(source Task, fallback error) {
	state, ok := taskState(source)
	if !ok {
		if fallback != nil {
			t.failLocal(fallback)
		} else {
			t.complete(nil)
		}
		return
	}
	t.taskName = state.taskName
	t.taskVersion = state.taskVersion
	t.taskID = state.taskID
	t.entityID = state.entityID
	t.entityOperation = state.entityOperation
	switch {
	case state.failureDetails != nil:
		t.fail(state.failureDetails)
	case state.localErr != nil:
		t.failLocal(state.localErr)
	case state.isCanceled:
		t.cancel()
	default:
		t.complete(state.rawResult)
	}
}

// computeNextDelay returns the delay before the next retry attempt, or zero to
// stop retrying. Every input is derived from replayed history, so the same
// failure event always yields the same decision. A delay that would push the
// next attempt past RetryTimeout stops the retries instead.
func computeNextDelay(currentTimeUtc time.Time, policy RetryPolicy, attempt int, firstAttempt time.Time, err error) time.Duration {
	if errors.Is(err, ErrTaskCanceled) {
		return 0
	}
	details := failureDetailsFromError(err)
	if details == nil || details.NonRetriable() {
		return 0
	}
	totalRetryTime := currentTimeUtc.Sub(firstAttempt)
	if totalRetryTime < 0 {
		totalRetryTime = 0
	}
	remaining := time.Duration(math.MaxInt64)
	if policy.RetryTimeout != math.MaxInt64 {
		remaining = firstAttempt.Add(policy.RetryTimeout).Sub(currentTimeUtc)
		if remaining <= 0 {
			return 0
		}
	}
	if policy.Handle != nil && !policy.Handle(RetryContext{
		LastAttemptNumber: attempt + 1,
		LastFailure:       details,
		TotalRetryTime:    totalRetryTime,
	}) {
		return 0
	}
	nextDelay := min(policy.InitialRetryInterval, policy.MaxRetryInterval)
	if attempt != 0 && policy.BackoffCoefficient != 1 {
		scaled := float64(policy.InitialRetryInterval) * math.Pow(policy.BackoffCoefficient, float64(attempt))
		nextDelay = policy.MaxRetryInterval
		// Cap before converting to avoid overflow; a positive backoff must not
		// underflow to the zero sentinel that stops retries.
		if scaled < float64(nextDelay) {
			nextDelay = max(time.Nanosecond, time.Duration(scaled))
		}
	}
	if nextDelay > remaining {
		return 0
	}
	return nextDelay
}

// CreateTimer schedules a durable timer that expires after the specified delay.
func (ctx *OrchestrationContext) CreateTimer(delay time.Duration) Task {
	engine := ctx.engineContext()
	if ctx.scope.isCanceled() {
		return newTaskInScope(engine, ctx.scope)
	}
	return engine.createTimerInternal(delay, ctx.scope)
}

func (ctx *OrchestrationContext) createTimerInternal(
	delay time.Duration,
	scope *cancellationScope,
) *completableTask {
	if scope.isCanceled() {
		return newTaskInScope(ctx, scope)
	}

	logicalTimer := newTaskInScope(ctx, scope)
	deadline := ctx.CurrentTimeUtc.Add(delay)
	if err := timestamppb.New(deadline).CheckValid(); err != nil {
		logicalTimer.failLocal(api.WrapInvalidArgument(fmt.Errorf("timer deadline %v is out of range: %w", deadline, err)))
		return logicalTimer
	}

	var scheduleNextChunk func()
	scheduleNextChunk = func() {
		if logicalTimer.isCompleted {
			return
		}

		fireAt := deadline
		isFinalChunk := true
		maximumInterval := ctx.orchestrationOptions.MaximumTimerInterval
		if deadline.Sub(ctx.CurrentTimeUtc) > maximumInterval {
			fireAt = ctx.CurrentTimeUtc.Add(maximumInterval)
			isFinalChunk = false
		}
		chunk := ctx.createTimerAction(fireAt, scope)
		chunk.onCompleted(func() {
			if logicalTimer.isCompleted {
				return
			}
			// A pre-splitting worker recorded the logical deadline as one timer.
			// If replay reaches that historical TimerFired event, CurrentTimeUtc
			// is already at or beyond the deadline and no intermediate chunk may
			// consume the next sequence number.
			if isFinalChunk ||
				(!chunk.timerFireAt.IsZero() && !deadline.After(chunk.timerFireAt)) ||
				!deadline.After(ctx.CurrentTimeUtc) ||
				chunk.isCanceled || chunk.localErr != nil || chunk.failureDetails != nil {
				logicalTimer.completeFrom(chunk, nil)
				return
			}
			scheduleNextChunk()
		})
	}
	scheduleNextChunk()
	return logicalTimer
}

func (ctx *OrchestrationContext) createTimerAction(
	fireAt time.Time,
	scope *cancellationScope,
) *completableTask {
	timerAction := helpers.NewCreateTimerAction(ctx.getNextSequenceNumber(), fireAt)
	ctx.pendingActions[timerAction.Id] = timerAction

	task := newTaskInScope(ctx, scope)
	ctx.pendingTasks[timerAction.Id] = task
	return task
}

// WaitForSingleEvent creates a task that is completed only after an event named [eventName] is received by this orchestration
// or when the specified timeout expires.
//
// The [timeout] parameter can be used to define a timeout for receiving the event. If the timeout expires before the
// named event is received, the task will be completed and will return a timeout error value [ErrTaskCanceled] when
// awaited. Otherwise, the awaited task will return the deserialized payload of the received event. A Duration value
// of zero returns a canceled task if the event isn't already available in the history. Use a negative Duration to
// wait indefinitely for the event to be received.
//
// Orchestrators can wait for the same event name multiple times, so waiting for multiple events with the same name
// is allowed. Each event received by an orchestrator will complete just one task returned by this method. Live
// waiters use LIFO ordering, so the newest waiter receives the next event. Events buffered before any waiter exists
// retain FIFO arrival order. This ordering is part of the deterministic replay contract.
//
// Note that event names are case-insensitive.
func (ctx *OrchestrationContext) WaitForSingleEvent(eventName string, timeout time.Duration) Task {
	engine := ctx.engineContext()
	task := newTaskInScope(engine, ctx.scope)
	if ctx.scope.isCanceled() {
		return task
	}
	key := strings.ToUpper(eventName)
	if buffered, ok := engine.takeBufferedEvent(key); ok {
		// An event with this name arrived already and can be consumed immediately.
		task.complete([]byte(buffered.event.GetEventRaised().GetInput().GetValue()))
	} else if timeout == 0 {
		// Zero-timeout means fail immediately if the event isn't already buffered.
		task.cancel()
	} else {
		// Keep a reference to this task so we can complete it when the event of this name arrives
		taskList, ok := engine.pendingExternalEventTasks[key]
		if !ok {
			taskList = list.New()
			engine.pendingExternalEventTasks[key] = taskList
		}
		taskElement := taskList.PushBack(task)
		task.onCompleted(func() {
			engine.removePendingEventTask(key, taskElement)
		})

		if timeout > 0 {
			engine.createTimerInternal(timeout, ctx.scope).onCompleted(func() {
				task.cancel()
			})
		}
	}
	return task
}

// CallEntity sends an operation request to an entity and waits for its response.
func (ctx *OrchestrationContext) CallEntity(entityID api.EntityID, operationName string, opts ...callEntityOption) Task {
	engine := ctx.engineContext()
	if engine.isTerminated || ctx.scope.isCanceled() {
		task := newTaskInScope(engine, ctx.scope)
		task.cancel()
		return task
	}
	if !engine.entitiesSupported {
		return ctx.newFailedTask(engine, errEntitiesUnsupported)
	}
	options := new(callEntityOptions)
	for _, configure := range opts {
		if err := configure(options, engine.converter); err != nil {
			return ctx.newFailedTask(engine, api.WrapInvalidArgument(err))
		}
	}
	if err := helpers.ValidateEntityName(entityID.Name); err != nil {
		return ctx.newFailedTask(engine, api.WrapInvalidArgument(err))
	}
	if operationName == "" {
		return ctx.newFailedTask(engine, api.WrapInvalidArgument(errors.New("entity operation name must not be empty")))
	}
	entityKey := entityID.String()
	if engine.criticalSectionID != "" {
		if engine.criticalSectionAvailable == nil {
			return ctx.newFailedTask(engine, fmt.Errorf("entity lock acquisition is still pending"))
		}
		available, locked := engine.criticalSectionAvailable[entityKey]
		if !locked {
			return ctx.newFailedTask(engine, fmt.Errorf("entity %s is not part of the current critical section", entityKey))
		}
		if !available {
			return ctx.newFailedTask(engine, fmt.Errorf("entity %s already has an outstanding call in the current critical section", entityKey))
		}
		engine.criticalSectionAvailable[entityKey] = false
	}

	requestID := ctx.NewGuid()
	action := helpers.NewEntityOperationCalledAction(
		engine.getNextSequenceNumber(),
		requestID,
		entityKey,
		string(engine.ID),
		engine.executionID,
		operationName,
		options.rawInput,
	)
	engine.pendingActions[action.Id] = action
	task := newTaskInScope(engine, ctx.scope)
	task.entityID = entityID
	task.entityOperation = operationName
	engine.pendingEntityTasks[requestID] = task
	if sectionID := engine.criticalSectionID; sectionID != "" {
		task.onCompleted(func() {
			if engine.criticalSectionID == sectionID && engine.criticalSectionAvailable != nil {
				engine.criticalSectionAvailable[entityKey] = true
			}
		})
	}
	return task
}

// SignalEntity sends a fire-and-forget entity operation.
func (ctx *OrchestrationContext) SignalEntity(entityID api.EntityID, operationName string, opts ...signalEntityOption) error {
	engine := ctx.engineContext()
	if engine.isTerminated || ctx.scope.isCanceled() {
		return ErrTaskCanceled
	}
	if !engine.entitiesSupported {
		return errEntitiesUnsupported
	}
	options := new(signalEntityOptions)
	for _, configure := range opts {
		if err := configure(options, engine.converter); err != nil {
			return api.WrapInvalidArgument(err)
		}
	}
	if err := helpers.ValidateEntityName(entityID.Name); err != nil {
		return api.WrapInvalidArgument(err)
	}
	if operationName == "" {
		return api.WrapInvalidArgument(errors.New("entity operation name must not be empty"))
	}
	entityKey := entityID.String()
	if engine.criticalSectionID != "" && engine.criticalSectionAvailable != nil {
		if _, locked := engine.criticalSectionAvailable[entityKey]; locked {
			return fmt.Errorf("signals to locked entity %s are not allowed inside a critical section", entityKey)
		}
	}

	action := helpers.NewEntityOperationSignaledAction(
		engine.getNextSequenceNumber(),
		ctx.NewGuid(),
		entityKey,
		operationName,
		options.rawInput,
		options.scheduledTime,
	)
	engine.pendingActions[action.Id] = action
	return nil
}

// LockEntities acquires an ordered critical section over a set of entities.
// If cancellation follows a committed request, critical-section restrictions
// remain in effect until the eventual grant is received and automatically released.
func (ctx *OrchestrationContext) LockEntities(entityIDs ...api.EntityID) (func(), error) {
	engine := ctx.engineContext()
	if engine.isTerminated || ctx.scope.isCanceled() {
		return nil, ErrTaskCanceled
	}
	if !engine.entitiesSupported {
		return nil, errEntitiesUnsupported
	}
	if engine.criticalSectionID != "" {
		return nil, fmt.Errorf("nested entity critical sections are not supported")
	}
	if len(entityIDs) == 0 {
		return nil, fmt.Errorf("at least one entity is required for a critical section")
	}
	lockSet := make([]string, 0, len(entityIDs))
	seen := make(map[string]struct{}, len(entityIDs))
	for _, entityID := range entityIDs {
		if err := helpers.ValidateEntityName(entityID.Name); err != nil {
			return nil, err
		}
		key := entityID.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		lockSet = append(lockSet, key)
	}
	sort.Strings(lockSet)

	criticalSectionID := ctx.NewGuid()
	action := helpers.NewEntityLockRequestedAction(
		engine.getNextSequenceNumber(),
		criticalSectionID,
		string(engine.ID),
		lockSet,
	)
	engine.pendingActions[action.Id] = action
	engine.criticalSectionID = criticalSectionID
	engine.criticalSectionLocks = append([]string(nil), lockSet...)
	engine.criticalSectionRequestCommitted = false
	lockTask := newTaskInScope(engine, ctx.scope)
	engine.pendingEntityTasks[criticalSectionID] = lockTask
	if err := lockTask.Await(nil); err != nil {
		if engine.criticalSectionID == criticalSectionID {
			delete(engine.pendingEntityTasks, criticalSectionID)
			// Coroutines run before subsequent history markers. A request may
			// already be committed even though its marker has not been replayed.
			if !engine.criticalSectionRequestCommitted {
				engine.criticalSectionRequestCommitted = engine.hasHistoricalEntityLockRequest(action.Id, criticalSectionID)
			}
			if engine.criticalSectionRequestCommitted {
				// The lock set is acquired sequentially. Keep the critical
				// section reserved until the full grant makes releasing safe.
				engine.criticalSectionAbandoned = true
			} else {
				delete(engine.pendingActions, action.Id)
				engine.clearCriticalSection()
			}
		}
		return nil, err
	}
	engine.criticalSectionAvailable = make(map[string]bool, len(lockSet))
	for _, entity := range lockSet {
		engine.criticalSectionAvailable[entity] = true
	}

	released := false
	return func() {
		if released {
			return
		}
		if engine.scheduler != nil && engine.scheduler.isStopping() {
			return
		}
		released = true
		engine.releaseCriticalSection(criticalSectionID)
	}, nil
}

// IsInCriticalSection reports whether the orchestration currently holds entity locks.
func (ctx *OrchestrationContext) IsInCriticalSection() bool {
	return ctx.engineContext().criticalSectionID != ""
}

func (ctx *OrchestrationContext) ContinueAsNew(newInput any, options ...ContinueAsNewOption) {
	engine := ctx.engineContext()
	engine.continuedAsNew = true
	engine.continuedAsNewInput = newInput
	for _, option := range options {
		option(engine)
	}
}

// SendEvent sends an event to another orchestration instance as part of the
// current durable orchestration transaction.
func (ctx *OrchestrationContext) SendEvent(instanceID api.InstanceID, eventName string, payload any) error {
	engine := ctx.engineContext()
	raw, err := marshalData(engine.converter, payload)
	if err != nil {
		return fmt.Errorf("failed to marshal event payload: %w", err)
	}
	if engine.isTerminated || ctx.scope.isCanceled() {
		return ErrTaskCanceled
	}
	action := helpers.NewSendEventAction(string(instanceID), eventName, wrapperspb.String(string(raw)))
	action.Id = engine.getNextSequenceNumber()
	engine.pendingActions[action.Id] = action
	return nil
}

func (ctx *OrchestrationContext) onExecutionStarted(es *protos.ExecutionStartedEvent) error {
	orchestrator, ok := ctx.registry.getOrchestrator(es.Name, es.GetVersion().GetValue())
	if !ok {
		return newTaskNotRegisteredError(
			orchestratorTaskNotFoundErrorType,
			es.Name,
			es.GetVersion().GetValue(),
		)
	}
	ctx.Name = es.Name
	ctx.Version = es.GetVersion().GetValue()
	ctx.executionID = es.GetOrchestrationInstance().GetExecutionId().GetValue()
	_, fields := contextprop.Decode(es.GetTags())
	ctx.contextFields = mergeStringMaps(ctx.contextFields, fields)
	ctx.orchestrationTags = tagcodec.DecodeUserTagsOrPlain(es.GetTags())
	if parent := es.GetParentInstance(); parent != nil {
		ctx.parentInstanceID = api.InstanceID(parent.GetOrchestrationInstance().GetInstanceId())
	}
	if es.Input != nil {
		ctx.rawInput = []byte(es.Input.Value)
	}

	return ctx.scheduler.startRoot(orchestrator)
}

func (ctx *OrchestrationContext) onTaskScheduled(taskID int32, ts *protos.TaskScheduledEvent) error {
	action, ok := ctx.pendingActions[taskID]
	scheduled := action.GetScheduleTask()
	if !ok || scheduled == nil ||
		!strings.EqualFold(scheduled.GetName(), ts.GetName()) ||
		!versionsMatchReplayHistory(scheduled.GetVersion(), ts.GetVersion(), ctx.Version) {
		return fmt.Errorf(
			"a previous execution called CallActivity for '%s' with version '%s' and sequence number %d at this point in the orchestration logic, but the current execution doesn't have a matching action",
			ts.Name,
			ts.GetVersion().GetValue(),
			taskID,
		)
	}
	delete(ctx.pendingActions, taskID)
	return nil
}

func (ctx *OrchestrationContext) onTaskCompleted(taskID int32, result *wrapperspb.StringValue) error {
	task, ok := ctx.pendingTasks[taskID]
	if !ok {
		// TODO: This could be a duplicate event or it could be a non-deterministic orchestration.
		//       Duplicate events should be handled gracefully with a warning. Otherwise, the
		//       orchestration should probably fail with an error.
		return nil
	}
	delete(ctx.pendingTasks, taskID)

	if result != nil {
		task.complete([]byte(result.Value))
	} else {
		task.complete(nil)
	}
	return nil
}

func (ctx *OrchestrationContext) onTaskFailed(taskID int32, details *protos.TaskFailureDetails) error {
	task, ok := ctx.pendingTasks[taskID]
	if !ok {
		// TODO: This could be a duplicate event or it could be a non-deterministic orchestration.
		//       Duplicate events should be handled gracefully with a warning. Otherwise, the
		//       orchestration should probably fail with an error.
		return nil
	}
	delete(ctx.pendingTasks, taskID)

	// completing a task will resume the corresponding Await() call
	task.fail(details)
	return nil
}

func (ctx *OrchestrationContext) onSubOrchestrationScheduled(taskID int32, ts *protos.SubOrchestrationInstanceCreatedEvent) error {
	action, ok := ctx.pendingActions[taskID]
	scheduled := action.GetCreateSubOrchestration()
	if !ok || scheduled == nil ||
		!strings.EqualFold(scheduled.GetName(), ts.GetName()) ||
		!versionsMatchReplayHistory(scheduled.GetVersion(), ts.GetVersion(), ctx.defaultVersion) {
		return fmt.Errorf(
			"a previous execution called CallSubOrchestrator for '%s' with version '%s' and sequence number %d at this point in the orchestration logic, but the current execution doesn't have a matching action",
			ts.Name,
			ts.GetVersion().GetValue(),
			taskID,
		)
	}
	delete(ctx.pendingActions, taskID)
	return nil
}

func versionsMatchReplayHistory(
	scheduled *wrapperspb.StringValue,
	historical *wrapperspb.StringValue,
	legacyDefault string,
) bool {
	if strings.EqualFold(scheduled.GetValue(), historical.GetValue()) {
		return true
	}
	// A nil historical version predates version inheritance/defaulting. A non-nil
	// empty wrapper is an explicit unversioned selection and must still mismatch.
	return historical == nil &&
		legacyDefault != "" &&
		strings.EqualFold(scheduled.GetValue(), legacyDefault)
}

func (ctx *OrchestrationContext) onTimerCreated(e *protos.HistoryEvent) error {
	if a, ok := ctx.pendingActions[e.EventId]; !ok || a.GetCreateTimer() == nil {
		return fmt.Errorf(
			"a previous execution called CreateTimer with sequence number %d, but the current execution doesn't have this action with this sequence number",
			e.EventId,
		)
	}
	delete(ctx.pendingActions, e.EventId)
	return nil
}

func (ctx *OrchestrationContext) onTimerFired(tf *protos.TimerFiredEvent) error {
	timerID := tf.TimerId
	task, ok := ctx.pendingTasks[timerID]
	if !ok {
		// TODO: This could be a duplicate event or it could be a non-deterministic orchestration.
		//       Duplicate events should be handled gracefully with a warning. Otherwise, the
		//       orchestration should probably fail with an error.
		return nil
	}
	delete(ctx.pendingTasks, timerID)

	// completing a task will resume the corresponding Await() call
	if tf.GetFireAt() != nil {
		task.timerFireAt = tf.GetFireAt().AsTime()
	}
	task.complete(nil)
	return nil
}

func (ctx *OrchestrationContext) onExternalEventRaised(e *protos.HistoryEvent) error {
	er := e.GetEventRaised()
	key := strings.ToUpper(er.GetName())
	if pendingTasks, ok := ctx.pendingExternalEventTasks[key]; ok && pendingTasks.Len() > 0 {
		task := pendingTasks.Back().Value.(*completableTask)
		task.complete([]byte(er.Input.GetValue()))
		return nil
	}

	// No live waiter consumed the event, so keep it for a future receiver.
	eventList, ok := ctx.bufferedExternalEvents[key]
	if !ok {
		eventList = list.New()
		ctx.bufferedExternalEvents[key] = eventList
	}
	eventList.PushBack(&bufferedEvent{
		event: e,
		order: ctx.scheduler.nextCompletionID(),
	})
	for waiter := range ctx.eventWaiters[key] {
		ctx.scheduler.makeRunnable(waiter)
	}
	return nil
}

func (ctx *OrchestrationContext) onEventSent(eventID int32, event *protos.EventSentEvent) error {
	action, ok := ctx.pendingActions[eventID]
	sent := action.GetSendEvent()
	if !ok || sent == nil ||
		sent.GetInstance().GetInstanceId() != event.GetInstanceId() ||
		sent.GetName() != event.GetName() ||
		sent.GetData().GetValue() != event.GetInput().GetValue() {
		return fmt.Errorf(
			"a previous execution sent event %q to %q with sequence number %d, but the current execution doesn't have this action",
			event.GetName(),
			event.GetInstanceId(),
			eventID,
		)
	}
	delete(ctx.pendingActions, eventID)
	return nil
}

func (ctx *OrchestrationContext) peekBufferedEvent(key string) (*bufferedEvent, bool) {
	eventList, ok := ctx.bufferedExternalEvents[key]
	if !ok || eventList.Len() == 0 {
		return nil, false
	}
	return eventList.Front().Value.(*bufferedEvent), true
}

func (ctx *OrchestrationContext) takeBufferedEvent(key string) (*bufferedEvent, bool) {
	eventList, ok := ctx.bufferedExternalEvents[key]
	if !ok || eventList.Len() == 0 {
		return nil, false
	}
	next := eventList.Front()
	event := next.Value.(*bufferedEvent)
	eventList.Remove(next)
	if eventList.Len() == 0 {
		delete(ctx.bufferedExternalEvents, key)
	}
	return event, true
}

// removePendingEventTask removes one waiting task from the named list, dropping
// the list entirely once it becomes empty.
func (ctx *OrchestrationContext) removePendingEventTask(key string, element *list.Element) {
	taskList, ok := ctx.pendingExternalEventTasks[key]
	if !ok {
		return
	}
	taskList.Remove(element)
	if taskList.Len() == 0 {
		delete(ctx.pendingExternalEventTasks, key)
	}
}

func (ctx *OrchestrationContext) addEventWaiter(key string, co *coroutine) {
	waiters, ok := ctx.eventWaiters[key]
	if !ok {
		waiters = make(map[*coroutine]struct{})
		ctx.eventWaiters[key] = waiters
	}
	waiters[co] = struct{}{}
}

func (ctx *OrchestrationContext) removeEventWaiter(key string, co *coroutine) {
	waiters, ok := ctx.eventWaiters[key]
	if !ok {
		return
	}
	delete(waiters, co)
	if len(waiters) == 0 {
		delete(ctx.eventWaiters, key)
	}
}

func (ctx *OrchestrationContext) onExecutionSuspended(er *protos.ExecutionSuspendedEvent) error {
	ctx.isSuspended = true
	return nil
}

func (ctx *OrchestrationContext) onExecutionResumed(er *protos.ExecutionResumedEvent) error {
	ctx.isSuspended = false
	ctx.resumedEvents = append(ctx.resumedEvents, ctx.suspendedEvents...)
	ctx.suspendedEvents = nil
	return nil
}

func (ctx *OrchestrationContext) onExecutionTerminated(et *protos.ExecutionTerminatedEvent) error {
	ctx.isTerminated = true
	// Termination wins over suspension: clearing the flag lets the completion
	// action be emitted and discards events that can no longer be processed.
	ctx.isSuspended = false
	ctx.suspendedEvents = nil
	// Termination can arrive in the same work item that lets the root coroutine
	// complete naturally. Keep exactly one terminal action, with termination
	// taking precedence over the pending natural completion.
	ctx.clearCompletionActions()
	return ctx.setCompleteInternal(et.Input, protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED, nil)
}

func (ctx *OrchestrationContext) onEntityOperationSent(eventID int32, requestID string) error {
	action, ok := ctx.pendingActions[eventID]
	if !ok {
		return fmt.Errorf("entity operation %q does not match pending action %d", requestID, eventID)
	}
	message := action.GetSendEntityMessage()
	if message == nil {
		return fmt.Errorf("entity operation %q does not match pending action %d", requestID, eventID)
	}
	sentRequestID := ""
	if called := message.GetEntityOperationCalled(); called != nil {
		sentRequestID = called.RequestId
	} else if signaled := message.GetEntityOperationSignaled(); signaled != nil {
		sentRequestID = signaled.RequestId
	}
	if sentRequestID != requestID {
		return fmt.Errorf("entity operation request ID changed from %q to %q", sentRequestID, requestID)
	}
	delete(ctx.pendingActions, eventID)
	return nil
}

func (ctx *OrchestrationContext) onEntityOperationCompleted(event *protos.EntityOperationCompletedEvent) error {
	if task := ctx.takePendingEntityTask(event.RequestId); task != nil {
		task.complete([]byte(event.Output.GetValue()))
	}
	return nil
}

func (ctx *OrchestrationContext) onEntityOperationFailed(event *protos.EntityOperationFailedEvent) error {
	if task := ctx.takePendingEntityTask(event.RequestId); task != nil {
		task.fail(event.FailureDetails)
	}
	return nil
}

func (ctx *OrchestrationContext) onEntityLockRequested(eventID int32, event *protos.EntityLockRequestedEvent) error {
	action, ok := ctx.pendingActions[eventID]
	if !ok {
		return fmt.Errorf("entity lock request %q does not match pending action %d", event.CriticalSectionId, eventID)
	}
	message := action.GetSendEntityMessage()
	if message == nil || message.GetEntityLockRequested() == nil ||
		message.GetEntityLockRequested().CriticalSectionId != event.CriticalSectionId {
		return fmt.Errorf("entity lock request %q does not match pending action %d", event.CriticalSectionId, eventID)
	}
	if ctx.criticalSectionID == event.CriticalSectionId {
		ctx.criticalSectionRequestCommitted = true
	}
	delete(ctx.pendingActions, eventID)
	return nil
}

func (ctx *OrchestrationContext) hasHistoricalEntityLockRequest(actionID int32, criticalSectionID string) bool {
	for _, history := range [][]*protos.HistoryEvent{ctx.oldEvents, ctx.newEvents} {
		for _, event := range history {
			if requested := event.GetEntityLockRequested(); requested != nil &&
				event.EventId == actionID && requested.CriticalSectionId == criticalSectionID {
				return true
			}
		}
	}
	return false
}

func (ctx *OrchestrationContext) onEntityLockGranted(event *protos.EntityLockGrantedEvent) error {
	if ctx.criticalSectionID != event.CriticalSectionId {
		return nil
	}
	if ctx.criticalSectionAbandoned {
		ctx.releaseCriticalSection(event.CriticalSectionId)
		return nil
	}
	if task := ctx.takePendingEntityTask(event.CriticalSectionId); task != nil {
		task.complete(nil)
	}
	return nil
}

func (ctx *OrchestrationContext) onEntityUnlockSent(eventID int32, event *protos.EntityUnlockSentEvent) error {
	action, ok := ctx.pendingActions[eventID]
	if !ok {
		return fmt.Errorf("entity unlock %q does not match pending action %d", event.CriticalSectionId, eventID)
	}
	message := action.GetSendEntityMessage()
	if message == nil || message.GetEntityUnlockSent() == nil ||
		message.GetEntityUnlockSent().CriticalSectionId != event.CriticalSectionId {
		return fmt.Errorf("entity unlock %q does not match pending action %d", event.CriticalSectionId, eventID)
	}
	delete(ctx.pendingActions, eventID)
	return nil
}

// takePendingEntityTask removes and returns the task awaiting the given entity request
// or critical section ID. Unknown IDs yield nil so that responses that no longer have a
// waiter, such as those replayed after a ContinueAsNew, are ignored.
func (ctx *OrchestrationContext) takePendingEntityTask(requestID string) *completableTask {
	task, ok := ctx.pendingEntityTasks[requestID]
	if !ok {
		return nil
	}
	delete(ctx.pendingEntityTasks, requestID)
	return task
}

func (ctx *OrchestrationContext) setComplete(output any) error {
	var rawOutput *wrapperspb.StringValue
	if output != nil {
		bytes, err := marshalData(ctx.converter, output)
		if err != nil {
			return fmt.Errorf("failed to serialize output: %w", err)
		}
		rawOutput = wrapperspb.String(string(bytes))
	}
	return ctx.setCompleteInternal(rawOutput, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, nil)
}

func (ctx *OrchestrationContext) setFailed(appError error) error {
	ctx.clearCompletionActions()
	provider := ctx.errorProperties
	if errors.Is(appError, api.ErrTaskNotRegistered) {
		provider = nil
	}
	return ctx.setCompleteInternal(
		nil,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
		failure.FromError(appError, provider),
	)
}

func (ctx *OrchestrationContext) enforceHistoryLimit() {
	ctx.clearCompletionActions()
	unprocessedEventCount := len(ctx.unprocessedExternalEvents())
	handler := ctx.orchestrationOptions.OnHistoryLimitExceeded
	if handler == nil {
		_ = ctx.setHistoryLimitFailed(ctx.newHistoryLimitError(nil))
		return
	}

	input, err := invokeHistoryLimitHandler(handler, HistoryLimitInfo{
		InstanceID:            ctx.ID,
		OrchestrationName:     ctx.Name,
		OrchestrationVersion:  ctx.Version,
		HistoryLength:         ctx.HistoryLength(),
		MaxHistoryEvents:      ctx.orchestrationOptions.MaxHistoryEvents,
		UnprocessedEventCount: unprocessedEventCount,
		SerializedInput:       string(ctx.rawInput),
		Converter:             ctx.converter,
	})
	if err != nil {
		_ = ctx.setHistoryLimitFailed(ctx.newHistoryLimitError(err))
		return
	}

	ctx.continuedAsNew = true
	ctx.continuedAsNewInput = input
	ctx.saveBufferedExternalEvents = true
	if maxHistoryEvents := ctx.orchestrationOptions.MaxHistoryEvents; maxHistoryEvents > 0 &&
		2+unprocessedEventCount > maxHistoryEvents {
		_ = ctx.setHistoryLimitFailed(ctx.newHistoryLimitError(fmt.Errorf(
			"continue-as-new would retain %d external event(s) and exceed MaxHistoryEvents=%d",
			unprocessedEventCount,
			maxHistoryEvents,
		)))
		return
	}
	if err := ctx.setContinuedAsNew(); err != nil {
		_ = ctx.setHistoryLimitFailed(ctx.newHistoryLimitError(err))
	}
}

func invokeHistoryLimitHandler(handler HistoryLimitHandler, info HistoryLimitInfo) (input any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic: %v", recovered)
		}
	}()
	return handler(info)
}

func (ctx *OrchestrationContext) newHistoryLimitError(policyError error) *HistoryLimitError {
	return &HistoryLimitError{
		InstanceID:       ctx.ID,
		HistoryLength:    ctx.HistoryLength(),
		MaxHistoryEvents: ctx.orchestrationOptions.MaxHistoryEvents,
		PolicyError:      policyError,
	}
}

func (ctx *OrchestrationContext) setHistoryLimitFailed(err error) error {
	ctx.continuedAsNew = false
	ctx.clearCompletionActions()
	return ctx.setCompleteInternal(
		nil,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
		failure.FromError(err, ctx.errorProperties),
	)
}

func (ctx *OrchestrationContext) clearCompletionActions() {
	for id, action := range ctx.pendingActions {
		if action.GetCompleteOrchestration() != nil {
			delete(ctx.pendingActions, id)
		}
	}
}

func (ctx *OrchestrationContext) hasCompletionAction() bool {
	for _, action := range ctx.pendingActions {
		if action.GetCompleteOrchestration() != nil {
			return true
		}
	}
	return false
}

func (ctx *OrchestrationContext) setContinuedAsNew() error {
	var newRawInput *wrapperspb.StringValue
	if ctx.continuedAsNewInput != nil {
		bytes, err := marshalData(ctx.converter, ctx.continuedAsNewInput)
		if err != nil {
			return fmt.Errorf("failed to serialize continue-as-new payload: %w", err)
		}
		newRawInput = wrapperspb.String(string(bytes))
	}
	return ctx.setCompleteInternal(
		newRawInput,
		protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW,
		nil,
	)
}

func (ctx *OrchestrationContext) setCompleteInternal(
	rawResult *wrapperspb.StringValue,
	status protos.OrchestrationStatus,
	failureDetails *protos.TaskFailureDetails,
) error {
	ctx.releaseCriticalSection(ctx.criticalSectionID)
	sequenceNumber := ctx.getNextSequenceNumber()
	completedAction := helpers.NewCompleteOrchestrationAction(
		sequenceNumber,
		status,
		rawResult,
		nil, // carryoverEvents is assigned later
		failureDetails,
	)
	completed := completedAction.GetCompleteOrchestration()
	if status == protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW {
		completed.NewVersion = ctx.continuedAsNewVersion
		version := ctx.Version
		if ctx.continuedAsNewVersion != nil {
			version = ctx.continuedAsNewVersion.GetValue()
		}
		completed.Tags = contextprop.Encode(api.OrchestrationContextInfo{
			InstanceID:       ctx.ID,
			Name:             ctx.Name,
			Version:          version,
			ParentInstanceID: ctx.parentInstanceID,
		}, ctx.contextFields, ctx.orchestrationTags)
	} else {
		completed.Tags = tagcodec.EncodeUserTags(ctx.orchestrationTags)
	}
	ctx.pendingActions[sequenceNumber] = completedAction
	return nil
}

func (ctx *OrchestrationContext) releaseCriticalSection(criticalSectionID string) {
	if criticalSectionID == "" || ctx.criticalSectionID != criticalSectionID {
		return
	}
	for _, entityID := range ctx.criticalSectionLocks {
		action := helpers.NewEntityUnlockSentAction(
			ctx.getNextSequenceNumber(),
			criticalSectionID,
			string(ctx.ID),
			entityID,
		)
		ctx.pendingActions[action.Id] = action
	}
	ctx.clearCriticalSection()
}

func (ctx *OrchestrationContext) clearCriticalSection() {
	ctx.criticalSectionID = ""
	ctx.criticalSectionLocks = nil
	ctx.criticalSectionAvailable = nil
	ctx.criticalSectionRequestCommitted = false
	ctx.criticalSectionAbandoned = false
}

func (ctx *OrchestrationContext) getNextSequenceNumber() int32 {
	current := ctx.sequenceNumber
	ctx.sequenceNumber++
	return current
}

func (ctx *OrchestrationContext) actions() []*protos.OrchestratorAction {
	if ctx.isSuspended {
		return nil
	}

	actions := make([]*protos.OrchestratorAction, 0, len(ctx.pendingActions))
	for id := int32(0); id < ctx.sequenceNumber; id++ {
		a, ok := ctx.pendingActions[id]
		if !ok {
			continue
		}
		actions = append(actions, a)
		if ctx.continuedAsNew && ctx.saveBufferedExternalEvents {
			if co := a.GetCompleteOrchestration(); co != nil {
				co.CarryoverEvents = append(co.CarryoverEvents, ctx.unprocessedExternalEvents()...)
			}
		}
	}
	return actions
}

func (ctx *OrchestrationContext) unprocessedExternalEvents() []*protos.HistoryEvent {
	buffered := make([]*bufferedEvent, 0)
	for _, eventList := range ctx.bufferedExternalEvents {
		for item := eventList.Front(); item != nil; item = item.Next() {
			buffered = append(buffered, item.Value.(*bufferedEvent))
		}
	}
	sort.Slice(buffered, func(i, j int) bool {
		return buffered[i].order < buffered[j].order
	})

	events := make([]*protos.HistoryEvent, 0, len(buffered))
	for _, event := range buffered {
		events = append(events, event.event)
	}
	for _, event := range ctx.resumedEvents {
		if event.event.GetEventRaised() != nil {
			events = append(events, event.event)
		}
	}
	for _, event := range ctx.suspendedEvents {
		if event.event.GetEventRaised() != nil {
			events = append(events, event.event)
		}
	}

	firstUnprocessedNewEvent := max(ctx.historyIndex-len(ctx.oldEvents), 0)
	for _, event := range ctx.newEvents[firstUnprocessedNewEvent:] {
		if event.GetEventRaised() != nil {
			events = append(events, event)
		}
	}
	return events
}
