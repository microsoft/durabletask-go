package task

import (
	"container/list"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/helpers"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/kit/ptr"
)

// Orchestrator is the functional interface for orchestrator functions.
type Orchestrator func(ctx *OrchestrationContext) (any, error)

// OrchestrationContext is the parameter type for orchestrator functions.
type OrchestrationContext struct {
	ID             api.InstanceID
	Name           string
	IsReplaying    bool
	CurrentTimeUtc time.Time
	appID          *string

	registry            *TaskRegistry
	rawInput            []byte
	oldEvents           []*protos.HistoryEvent
	newEvents           []*protos.HistoryEvent
	suspendedEvents     []*protos.HistoryEvent
	isSuspended         bool
	historyIndex        int
	sequenceNumber      int32
	pendingActions      map[int32]*protos.OrchestratorAction
	pendingTasks        map[int32]*completableTask
	continuedAsNew      bool
	continuedAsNewInput any
	customStatus        string

	bufferedExternalEvents     map[string]*list.List
	pendingExternalEventTasks  map[string]*list.List
	saveBufferedExternalEvents bool
}

// callSubOrchestratorOptions is a struct that holds the options for the CallSubOrchestrator orchestrator method.
type callSubOrchestratorOptions struct {
	instanceID  string
	rawInput    *wrapperspb.StringValue
	targetAppID *string
	retryPolicy *RetryPolicy
}

// subOrchestratorOption is a functional option type for the CallSubOrchestrator orchestrator method.
type subOrchestratorOption func(*callSubOrchestratorOptions) error

// ContinueAsNewOption is a functional option type for the ContinueAsNew orchestrator method.
type ContinueAsNewOption func(*OrchestrationContext)

// WithSubOrchestratorAppID is a functional option type for the CallSubOrchestrator orchestrator method that specifies the app ID of the target activity.
func WithSubOrchestratorAppID(appID string) subOrchestratorOption {
	return func(opts *callSubOrchestratorOptions) error {
		opts.targetAppID = &appID
		return nil
	}
}

// WithKeepUnprocessedEvents returns a ContinueAsNewOptions struct that instructs the
// runtime to carry forward any unprocessed external events to the new instance.
func WithKeepUnprocessedEvents() ContinueAsNewOption {
	return func(ctx *OrchestrationContext) {
		ctx.saveBufferedExternalEvents = true
	}
}

// WithSubOrchestratorInput is a functional option type for the CallSubOrchestrator
// orchestrator method that takes an input value and marshals it to JSON.
func WithSubOrchestratorInput(input any) subOrchestratorOption {
	return func(opts *callSubOrchestratorOptions) error {
		bytes, err := marshalData(input)
		if err != nil {
			return fmt.Errorf("failed to marshal input to JSON: %w", err)
		}
		opts.rawInput = wrapperspb.String(string(bytes))
		return nil
	}
}

// WithRawSubOrchestratorInput is a functional option type for the CallSubOrchestrator
// orchestrator method that takes a raw input value.
func WithRawSubOrchestratorInput(input *wrapperspb.StringValue) subOrchestratorOption {
	return func(opts *callSubOrchestratorOptions) error {
		opts.rawInput = input
		return nil
	}
}

// WithSubOrchestrationInstanceID is a functional option type for the CallSubOrchestrator
// orchestrator method that specifies the instance ID of the sub-orchestration.
func WithSubOrchestrationInstanceID(instanceID string) subOrchestratorOption {
	return func(opts *callSubOrchestratorOptions) error {
		opts.instanceID = instanceID
		return nil
	}
}

func WithSubOrchestrationRetryPolicy(policy *RetryPolicy) subOrchestratorOption {
	return func(opt *callSubOrchestratorOptions) error {
		if policy == nil {
			return nil
		}
		err := policy.Validate()
		if err != nil {
			return err
		}
		opt.retryPolicy = policy
		return nil
	}
}

// NewOrchestrationContext returns a new [OrchestrationContext] struct with the specified parameters.
func NewOrchestrationContext(registry *TaskRegistry, id api.InstanceID, oldEvents []*protos.HistoryEvent, newEvents []*protos.HistoryEvent) *OrchestrationContext {
	return &OrchestrationContext{
		ID:                        id,
		registry:                  registry,
		oldEvents:                 oldEvents,
		newEvents:                 newEvents,
		bufferedExternalEvents:    make(map[string]*list.List),
		pendingExternalEventTasks: make(map[string]*list.List),
	}
}

func (ctx *OrchestrationContext) start() (actions []*protos.OrchestratorAction) {
	ctx.historyIndex = 0
	ctx.sequenceNumber = 0
	ctx.pendingActions = make(map[int32]*protos.OrchestratorAction)
	ctx.pendingTasks = make(map[int32]*completableTask)

	defer func() {
		result := recover()
		if result == ErrTaskBlocked {
			// Expected, normal part of execution
			actions = ctx.actions()
		} else if result != nil {
			// Unexpected panic!
			panic(result)
		}
	}()

	for {
		if ok, err := ctx.processNextEvent(); err != nil {
			ctx.setFailed(err)
			break
		} else if !ok {
			// Orchestrator finished, break out of the loop and return any pending actions
			break
		}
	}
	return ctx.actions()
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
	var historyList []*protos.HistoryEvent
	index := ctx.historyIndex
	if ctx.historyIndex >= len(ctx.oldEvents)+len(ctx.newEvents) {
		return nil, false
	} else if ctx.historyIndex < len(ctx.oldEvents) {
		ctx.IsReplaying = true
		historyList = ctx.oldEvents
	} else {
		ctx.IsReplaying = false
		historyList = ctx.newEvents
		index -= len(ctx.oldEvents)
	}

	ctx.historyIndex++
	e := historyList[index]
	return e, true
}

func (ctx *OrchestrationContext) processEvent(e *backend.HistoryEvent) error {
	// Buffer certain events if we're in a suspended state
	if ctx.isSuspended && (e.GetExecutionResumed() == nil && e.GetExecutionTerminated() == nil) {
		ctx.suspendedEvents = append(ctx.suspendedEvents, e)
		return nil
	}

	var err error = nil
	if os := e.GetOrchestratorStarted(); os != nil {
		// OrchestratorStarted is only used to update the current orchestration time
		ctx.CurrentTimeUtc = e.Timestamp.AsTime()
	} else if es := e.GetExecutionStarted(); es != nil {
		// Extract source AppID from HistoryEvent Router if this is ExecutionStartedEvent
		if e.GetRouter() != nil {
			router := e.GetRouter()
			// For cross-app suborchestrations, if we have a target, use that as our appID
			// since that's where we're actually executing
			if router.TargetAppID != nil {
				ctx.appID = ptr.Of(router.GetTargetAppID())
			} else {
				ctx.appID = ptr.Of(router.GetSourceAppID())
			}
		}
		err = ctx.onExecutionStarted(es)
	} else if ts := e.GetTaskScheduled(); ts != nil {
		err = ctx.onTaskScheduled(e.EventId, ts)
	} else if tc := e.GetTaskCompleted(); tc != nil {
		err = ctx.onTaskCompleted(tc)
	} else if tf := e.GetTaskFailed(); tf != nil {
		err = ctx.onTaskFailed(tf)
	} else if ts := e.GetSubOrchestrationInstanceCreated(); ts != nil {
		err = ctx.onSubOrchestrationScheduled(e.EventId, ts)
	} else if sc := e.GetSubOrchestrationInstanceCompleted(); sc != nil {
		err = ctx.onSubOrchestrationCompleted(sc)
	} else if sf := e.GetSubOrchestrationInstanceFailed(); sf != nil {
		err = ctx.onSubOrchestrationFailed(sf)
	} else if tc := e.GetTimerCreated(); tc != nil {
		err = ctx.onTimerCreated(e)
	} else if tf := e.GetTimerFired(); tf != nil {
		err = ctx.onTimerFired(tf)
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
	} else {
		err = fmt.Errorf("don't know how to handle event: %v", e)
	}
	return err
}

func (octx *OrchestrationContext) SetCustomStatus(cs string) {
	octx.customStatus = cs
}

// GetInput unmarshals the serialized orchestration input and stores it in [v].
func (octx *OrchestrationContext) GetInput(v any) error {
	return unmarshalData(octx.rawInput, v)
}

// CallActivity schedules an asynchronous invocation of an activity function. The [activity]
// parameter can be either the name of an activity as a string or can be a pointer to the function
// that implements the activity, in which case the name is obtained via reflection.
func (ctx *OrchestrationContext) CallActivity(activity interface{}, opts ...callActivityOption) Task {
	options := new(callActivityOptions)
	for _, configure := range opts {
		if err := configure(options); err != nil {
			failedTask := newTask(ctx)
			failedTask.fail(&protos.TaskFailureDetails{
				ErrorType:    reflect.TypeOf(err).String(),
				ErrorMessage: err.Error(),
			})
			return failedTask
		}
	}

	activityName := helpers.GetTaskFunctionName(activity)

	if options.retryPolicy != nil {
		return ctx.internalScheduleTaskWithRetries(activityName+"-retry", ctx.CurrentTimeUtc, func(taskExecutionId string) Task {
			return ctx.internalScheduleActivity(activityName, taskExecutionId, options)
		}, *options.retryPolicy, 0, uuid.NewString())
	}

	return ctx.internalScheduleActivity(activityName, uuid.NewString(), options)
}

func (ctx *OrchestrationContext) internalScheduleActivity(activityName, taskExecutionId string, options *callActivityOptions) Task {
	scheduleTaskAction := &protos.OrchestratorAction{
		Id: ctx.getNextSequenceNumber(),
		OrchestratorActionType: &protos.OrchestratorAction_ScheduleTask{
			ScheduleTask: &protos.ScheduleTaskAction{Name: activityName, TaskExecutionId: taskExecutionId, Input: options.rawInput},
		},
	}

	// Add TaskRouter support for cross-app activities
	if ctx.appID != nil {
		scheduleTaskAction.Router = &protos.TaskRouter{
			SourceAppID: *ctx.appID, // Current orchestrator app ID
		}

		if options.targetAppID != nil {
			scheduleTaskAction.Router.TargetAppID = options.targetAppID // Target activity app ID
		}
	}

	ctx.pendingActions[scheduleTaskAction.Id] = scheduleTaskAction

	task := newTask(ctx)
	ctx.pendingTasks[scheduleTaskAction.Id] = task
	return task
}

// TODO: cassie wire appID into suborchestration options too for cross app wf
func (ctx *OrchestrationContext) CallSubOrchestrator(orchestrator interface{}, opts ...subOrchestratorOption) Task {
	options := new(callSubOrchestratorOptions)
	for _, configure := range opts {
		if err := configure(options); err != nil {
			failedTask := newTask(ctx)
			failedTask.fail(&protos.TaskFailureDetails{
				ErrorType:    reflect.TypeOf(err).String(),
				ErrorMessage: err.Error(),
			})
			return failedTask
		}
	}

	orchestratorName := helpers.GetTaskFunctionName(orchestrator)

	if options.retryPolicy != nil {
		return ctx.internalScheduleTaskWithRetries(orchestratorName+"-retry", ctx.CurrentTimeUtc, func(_ string) Task {
			return ctx.internalCallSubOrchestrator(orchestratorName, options)
		}, *options.retryPolicy, 0, uuid.NewString())
	}

	return ctx.internalCallSubOrchestrator(orchestratorName, options)
}

func (ctx *OrchestrationContext) internalCallSubOrchestrator(orchestratorName string, options *callSubOrchestratorOptions) Task {
	createSubOrchestrationAction := &protos.OrchestratorAction{
		Id: ctx.getNextSequenceNumber(),
		OrchestratorActionType: &protos.OrchestratorAction_CreateSubOrchestration{
			CreateSubOrchestration: &protos.CreateSubOrchestrationAction{
				Name:       orchestratorName,
				Input:      options.rawInput,
				InstanceId: options.instanceID,
			},
		},
	}
	if ctx.appID != nil {
		createSubOrchestrationAction.Router = &protos.TaskRouter{
			SourceAppID: *ctx.appID,
		}

		if options.targetAppID != nil {
			createSubOrchestrationAction.Router.TargetAppID = options.targetAppID
		}
	}
	ctx.pendingActions[createSubOrchestrationAction.Id] = createSubOrchestrationAction

	task := newTask(ctx)
	ctx.pendingTasks[createSubOrchestrationAction.Id] = task
	return task
}

func (ctx *OrchestrationContext) internalScheduleTaskWithRetries(name string, initialAttempt time.Time, schedule func(taskExecutionId string) Task, policy RetryPolicy, retryCount int, taskExecutionId string) Task {
	return &taskWrapper{
		delegate: schedule(taskExecutionId),
		onAwaitResult: func(v any, taskExecutionId string, err error) error {
			if err == nil {
				return nil
			}

			if retryCount+1 >= policy.MaxAttempts {
				// next try will exceed the max attempts, dont continue
				return err
			}

			nextDelay := computeNextDelay(ctx.CurrentTimeUtc, policy, retryCount, initialAttempt, err)
			if nextDelay == 0 {
				return err
			}
			timerErr := ctx.createTimerInternal(&name, nextDelay).Await(nil)
			if timerErr != nil {
				// TODO use errors.Join when updating golang
				return fmt.Errorf("%v %w", timerErr, err)
			}

			t := ctx.internalScheduleTaskWithRetries(name, initialAttempt, schedule, policy, retryCount+1, taskExecutionId)
			err = t.Await(v)
			if err == nil {
				return nil
			}

			return err
		},
	}
}

func computeNextDelay(currentTimeUtc time.Time, policy RetryPolicy, attempt int, firstAttempt time.Time, err error) time.Duration {
	if policy.Handle(err) {
		isExpired := false
		if policy.RetryTimeout != math.MaxInt64 {
			isExpired = currentTimeUtc.After(firstAttempt.Add(policy.RetryTimeout))
		}
		if !isExpired {
			nextDelayMs := float64(policy.InitialRetryInterval.Milliseconds()) * math.Pow(policy.BackoffCoefficient, float64(attempt))
			if nextDelayMs < float64(policy.MaxRetryInterval.Milliseconds()) {
				return time.Duration(int64(nextDelayMs) * int64(time.Millisecond))
			}
			return policy.MaxRetryInterval
		}
	}
	return 0
}

// CreateTimer schedules a durable timer that expires after the specified delay.
func (ctx *OrchestrationContext) CreateTimer(delay time.Duration, opts ...createTimerOption) Task {
	options := new(createTimerOptions)
	for _, configure := range opts {
		if err := configure(options); err != nil {
			failedTask := newTask(ctx)
			failedTask.fail(&protos.TaskFailureDetails{
				ErrorType:    reflect.TypeOf(err).String(),
				ErrorMessage: err.Error(),
			})
			return failedTask
		}
	}
	return ctx.createTimerInternal(options.name, delay)
}

func (ctx *OrchestrationContext) createTimerInternal(name *string, delay time.Duration) *completableTask {
	fireAt := ctx.CurrentTimeUtc.Add(delay)
	timerAction := &protos.OrchestratorAction{
		Id: ctx.getNextSequenceNumber(),
		OrchestratorActionType: &protos.OrchestratorAction_CreateTimer{
			CreateTimer: &protos.CreateTimerAction{
				FireAt: timestamppb.New(fireAt),
				Name:   name,
			},
		},
	}
	ctx.pendingActions[timerAction.Id] = timerAction

	task := newTask(ctx)
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
// is allowed. Each event received by an orchestrator will complete just one task returned by this method.
//
// Note that event names are case-insensitive.
func (ctx *OrchestrationContext) WaitForSingleEvent(eventName string, timeout time.Duration) Task {
	task := newTask(ctx)
	key := strings.ToUpper(eventName)
	if eventList, ok := ctx.bufferedExternalEvents[key]; ok {
		// An event with this name arrived already and can be consumed immediately.
		next := eventList.Front()
		if eventList.Len() > 1 {
			eventList.Remove(next)
		} else {
			delete(ctx.bufferedExternalEvents, key)
		}
		rawValue := []byte(next.Value.(*protos.HistoryEvent).GetEventRaised().GetInput().GetValue())
		task.complete(rawValue)
	} else if timeout == 0 {
		// Zero-timeout means fail immediately if the event isn't already buffered.
		task.cancel()
	} else {
		// Keep a reference to this task so we can complete it when the event of this name arrives
		var taskList *list.List
		var ok bool
		if taskList, ok = ctx.pendingExternalEventTasks[key]; !ok {
			taskList = list.New()
			ctx.pendingExternalEventTasks[key] = taskList
		}
		taskElement := taskList.PushBack(task)

		if timeout > 0 {
			ctx.createTimerInternal(&eventName, timeout).onCompleted(func() {
				task.cancel()
				if taskList.Len() > 1 {
					taskList.Remove(taskElement)
				} else {
					delete(ctx.pendingExternalEventTasks, key)
				}
			})
		}
	}
	return task
}

func (ctx *OrchestrationContext) ContinueAsNew(newInput any, options ...ContinueAsNewOption) {
	ctx.continuedAsNew = true
	ctx.continuedAsNewInput = newInput
	for _, option := range options {
		option(ctx)
	}
}

func (ctx *OrchestrationContext) onExecutionStarted(es *protos.ExecutionStartedEvent) error {
	orchestrator, ok := ctx.registry.orchestrators[es.Name]
	if !ok {
		// try looking for a "default" orchestrator
		orchestrator, ok = ctx.registry.orchestrators["*"]
		if !ok {
			return fmt.Errorf("orchestrator named '%s' is not registered", es.Name)
		}
	}
	ctx.Name = es.Name
	if es.Input != nil {
		ctx.rawInput = []byte(es.Input.Value)
	}

	output, appError := orchestrator(ctx)

	var err error
	if appError != nil {
		err = ctx.setFailed(appError)
	} else if ctx.continuedAsNew {
		err = ctx.setContinuedAsNew()
	} else {
		err = ctx.setComplete(output)
	}

	if appError == nil && err != nil {
		completionErr := fmt.Errorf("failed to complete the orchestration: %w", err)
		if err2 := ctx.setFailed(completionErr); err2 != nil {
			return completionErr
		}
	}
	return nil
}

func (ctx *OrchestrationContext) onTaskScheduled(taskID int32, ts *protos.TaskScheduledEvent) error {
	if a, ok := ctx.pendingActions[taskID]; !ok || a.GetScheduleTask() == nil {
		return fmt.Errorf(
			"a previous execution called CallActivity for '%s' and sequence number %d at this point in the orchestration logic, but the current execution doesn't have this action with this sequence number",
			ts.Name,
			taskID,
		)
	}
	delete(ctx.pendingActions, taskID)
	return nil
}

func (ctx *OrchestrationContext) onTaskCompleted(tc *protos.TaskCompletedEvent) error {
	taskID := tc.TaskScheduledId
	task, ok := ctx.pendingTasks[taskID]
	if !ok {
		// TODO: This could be a duplicate event or it could be a non-deterministic orchestration.
		//       Duplicate events should be handled gracefully with a warning. Otherwise, the
		//       orchestration should probably fail with an error.
		return nil
	}
	delete(ctx.pendingTasks, taskID)

	if tc.Result != nil {
		task.complete([]byte(tc.Result.Value))
	} else {
		task.complete(nil)
	}
	return nil
}

func (ctx *OrchestrationContext) onTaskFailed(tf *protos.TaskFailedEvent) error {
	taskID := tf.TaskScheduledId
	task, ok := ctx.pendingTasks[taskID]
	if !ok {
		// TODO: This could be a duplicate event or it could be a non-deterministic orchestration.
		//       Duplicate events should be handled gracefully with a warning. Otherwise, the
		//       orchestration should probably fail with an error.
		return nil
	}
	delete(ctx.pendingTasks, taskID)

	// completing a task will resume the corresponding Await() call
	task.fail(tf.FailureDetails)
	task.taskExecutionId = tf.TaskExecutionId
	return nil
}

func (ctx *OrchestrationContext) onSubOrchestrationScheduled(taskID int32, ts *protos.SubOrchestrationInstanceCreatedEvent) error {
	if a, ok := ctx.pendingActions[taskID]; !ok || a.GetCreateSubOrchestration() == nil {
		return fmt.Errorf(
			"a previous execution called CallSubOrchestrator for '%s' and sequence number %d at this point in the orchestration logic, but the current execution doesn't have this action with this sequence number",
			ts.Name,
			taskID,
		)
	}
	delete(ctx.pendingActions, taskID)
	return nil
}

func (ctx *OrchestrationContext) onSubOrchestrationCompleted(soc *protos.SubOrchestrationInstanceCompletedEvent) error {
	taskID := soc.TaskScheduledId
	task, ok := ctx.pendingTasks[taskID]
	if !ok {
		// TODO: This could be a duplicate event or it could be a non-deterministic orchestration.
		//       Duplicate events should be handled gracefully with a warning. Otherwise, the
		//       orchestration should probably fail with an error.
		return nil
	}
	delete(ctx.pendingTasks, taskID)

	// completing a task will resume the corresponding Await() call
	if soc.Result != nil {
		task.complete([]byte(soc.Result.Value))
	} else {
		task.complete(nil)
	}
	return nil
}

func (ctx *OrchestrationContext) onSubOrchestrationFailed(sof *protos.SubOrchestrationInstanceFailedEvent) error {
	taskID := sof.TaskScheduledId
	task, ok := ctx.pendingTasks[taskID]
	if !ok {
		// TODO: This could be a duplicate event or it could be a non-deterministic orchestration.
		//       Duplicate events should be handled gracefully with a warning. Otherwise, the
		//       orchestration should probably fail with an error.
		return nil
	}
	delete(ctx.pendingTasks, taskID)

	// completing a task will resume the corresponding Await() call
	task.fail(sof.FailureDetails)
	return nil
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
	task.complete(nil)
	return nil
}

func (ctx *OrchestrationContext) onExternalEventRaised(e *protos.HistoryEvent) error {
	er := e.GetEventRaised()
	key := strings.ToUpper(er.GetName())
	if pendingTasks, ok := ctx.pendingExternalEventTasks[key]; ok {
		// Complete the previously allocated task associated with this event name.
		elem := pendingTasks.Front()
		task := elem.Value.(*completableTask)
		if pendingTasks.Len() > 1 {
			pendingTasks.Remove(elem)
		} else {
			delete(ctx.pendingExternalEventTasks, key)
		}
		rawValue := []byte(er.Input.GetValue())
		task.complete(rawValue)
	} else {
		// Add this event to the buffered list of events with this name.
		var eventList *list.List
		var ok bool
		if eventList, ok = ctx.bufferedExternalEvents[key]; !ok {
			eventList = list.New()
			ctx.bufferedExternalEvents[key] = eventList
		}
		eventList.PushBack(e)
	}
	return nil
}

func (ctx *OrchestrationContext) onExecutionSuspended(er *protos.ExecutionSuspendedEvent) error {
	ctx.isSuspended = true
	return nil
}

func (ctx *OrchestrationContext) onExecutionResumed(er *protos.ExecutionResumedEvent) error {
	ctx.isSuspended = false
	for _, e := range ctx.suspendedEvents {
		if err := ctx.processEvent(e); err != nil {
			return err
		}
	}
	ctx.suspendedEvents = nil
	return nil
}

func (ctx *OrchestrationContext) onExecutionTerminated(et *protos.ExecutionTerminatedEvent) error {
	if err := ctx.setCompleteInternal(et.Input, protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED, nil); err != nil {
		return err
	}
	return nil
}

func (ctx *OrchestrationContext) setComplete(output any) error {
	status := protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED
	var rawOutput *wrapperspb.StringValue
	if output != nil {
		bytes, err := json.Marshal(output)
		if err != nil {
			return fmt.Errorf("failed to marshal output to JSON: %w", err)
		}
		rawOutput = wrapperspb.String(string(bytes))
	}
	if err := ctx.setCompleteInternal(rawOutput, status, nil); err != nil {
		return err
	}
	return nil
}

func (ctx *OrchestrationContext) setFailed(appError error) error {
	fd := &protos.TaskFailureDetails{
		ErrorType:    reflect.TypeOf(appError).String(),
		ErrorMessage: appError.Error(),
	}
	failedStatus := protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED
	if err := ctx.setCompleteInternal(nil, failedStatus, fd); err != nil {
		return err
	}
	return nil
}

func (ctx *OrchestrationContext) setContinuedAsNew() error {
	status := protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW
	var newRawInput *wrapperspb.StringValue
	if ctx.continuedAsNewInput != nil {
		bytes, err := json.Marshal(ctx.continuedAsNewInput)
		if err != nil {
			return fmt.Errorf("failed to marshal continue-as-new payload to JSON: %w", err)
		}
		newRawInput = wrapperspb.String(string(bytes))
	}
	if err := ctx.setCompleteInternal(newRawInput, status, nil); err != nil {
		return err
	}
	return nil
}

func (ctx *OrchestrationContext) setCompleteInternal(
	rawResult *wrapperspb.StringValue,
	status protos.OrchestrationStatus,
	failureDetails *protos.TaskFailureDetails,
) error {
	sequenceNumber := ctx.getNextSequenceNumber()
	completedAction := &protos.OrchestratorAction{
		Id: sequenceNumber,
		OrchestratorActionType: &protos.OrchestratorAction_CompleteOrchestration{
			CompleteOrchestration: &protos.CompleteOrchestrationAction{
				OrchestrationStatus: status,
				Result:              rawResult,
				FailureDetails:      failureDetails,
			},
		},
	}

	if ctx.appID != nil {
		completedAction.Router = &protos.TaskRouter{
			SourceAppID: *ctx.appID,
		}
	}

	ctx.pendingActions[sequenceNumber] = completedAction
	return nil
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

	var actions []*protos.OrchestratorAction
	for _, a := range ctx.pendingActions {
		actions = append(actions, a)
		if ctx.continuedAsNew && ctx.saveBufferedExternalEvents {
			if co := a.GetCompleteOrchestration(); co != nil {
				for _, eventList := range ctx.bufferedExternalEvents {
					for item := eventList.Front(); item != nil; item = item.Next() {
						e := item.Value.(*protos.HistoryEvent)
						co.CarryoverEvents = append(co.CarryoverEvents, e)
					}
				}
			}
		}
	}
	return actions
}
