package tests

import (
	"context"
	"fmt"
	"github.com/microsoft/durabletask-go/backend/postgres"
	"os"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var (
	ctx                   = context.Background()
	logger                = backend.DefaultLogger()
	sqliteInMemoryOptions = sqlite.NewSqliteOptions("")
	sqliteFileOptions     = sqlite.NewSqliteOptions("test.sqlite3")
)

func getRunnableBackends() []backend.Backend {
	var runnableBackends []backend.Backend

	runnableBackends = append(runnableBackends, sqlite.NewSqliteBackend(sqliteFileOptions, logger))
	runnableBackends = append(runnableBackends, sqlite.NewSqliteBackend(sqliteInMemoryOptions, logger))

	if os.Getenv("POSTGRES_ENABLED") == "true" {
		runnableBackends = append(runnableBackends, postgres.NewPostgresBackend(nil, logger))
	}

	return runnableBackends
}

var backends = getRunnableBackends()

var completionStatusValues = []protos.OrchestrationStatus{
	protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
	protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED,
	protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
}

const (
	defaultName  = "testing"
	defaultInput = "Hello, 世界!"
)

// Test_NewOrchestrationWorkItem_Single enqueues a single work item into the backend
// store and attempts to fetch it immediately afterwards.
func Test_NewOrchestrationWorkItem_Single(t *testing.T) {
	for i, be := range backends {
		initTest(t, be, i, true)

		expectedID := "myinstance"
		if createOrchestrationInstance(t, be, expectedID) {
			if wi, ok := getOrchestrationWorkItem(t, be, expectedID); ok {
				if assert.Equal(t, 1, len(wi.NewEvents)) {
					startEvent := wi.NewEvents[0].GetExecutionStarted()
					if assert.NotNil(t, startEvent) {
						assert.Equal(t, expectedID, startEvent.OrchestrationInstance.GetInstanceId())
						assert.Equal(t, defaultName, startEvent.Name)
						assert.Equal(t, defaultInput, startEvent.Input.GetValue())
					}
				}
				if state, ok := getOrchestrationRuntimeState(t, be, wi); ok {
					// initial state should be empty since this is a new instance
					iid := state.InstanceID()
					assert.Equal(t, wi.InstanceID, iid)
					_, err := state.Name()
					assert.ErrorIs(t, err, api.ErrNotStarted)
					_, err = state.Input()
					assert.ErrorIs(t, err, api.ErrNotStarted)
					assert.Equal(t, 0, len(state.NewEvents()))
					assert.Equal(t, 0, len(state.OldEvents()))
				}

				// Ensure no more work items
				_, err := be.GetOrchestrationWorkItem(ctx)
				assert.ErrorIs(t, err, backend.ErrNoWorkItems)
			}
		}
	}
}

// Test_NewOrchestrationWorkItem_Multiple enqueues multiple work items into the sqlite backend
// store and then attempts to fetch them one-at-a-time, in order.
func Test_NewOrchestrationWorkItem_Multiple(t *testing.T) {
	for i, be := range backends {
		initTest(t, be, i, true)

		const WorkItems = 4

		// Create multiple work items up front
		for j := 0; j < WorkItems; j++ {
			expectedID := fmt.Sprintf("instance_%d", j)
			createOrchestrationInstance(t, be, expectedID)
		}

		for j := 0; j < WorkItems; j++ {
			expectedID := fmt.Sprintf("instance_%d", j)
			if wi, ok := getOrchestrationWorkItem(t, be, expectedID); ok {
				if assert.Equal(t, 1, len(wi.NewEvents)) {
					startEvent := wi.NewEvents[0].GetExecutionStarted()
					if assert.NotNil(t, startEvent) {
						assert.Equal(t, expectedID, startEvent.OrchestrationInstance.GetInstanceId())
						assert.Equal(t, defaultName, startEvent.Name)
						assert.Equal(t, defaultInput, startEvent.Input.GetValue())
					}
				}
				if state, ok := getOrchestrationRuntimeState(t, be, wi); ok {
					// initial state should be empty since this is a new instance
					_, err := state.Name()
					assert.ErrorIs(t, err, api.ErrNotStarted)
					_, err = state.Input()
					assert.ErrorIs(t, err, api.ErrNotStarted)
					assert.Equal(t, 0, len(state.NewEvents()))
					assert.Equal(t, 0, len(state.OldEvents()))
				}
			}
		}

		// Ensure no more work items
		_, err := be.GetOrchestrationWorkItem(ctx)
		assert.ErrorIs(t, err, backend.ErrNoWorkItems)
	}
}

func Test_CompleteOrchestration(t *testing.T) {
	for i, be := range backends {
		for _, expectedStatus := range completionStatusValues {
			initTest(t, be, i, true)

			expectedResult := "done!"
			stackTraceBuffer := make([]byte, 256)
			var expectedStackTrace string = ""

			// Produce an ExecutionCompleted event with a particular output
			getOrchestratorActions := func() []*protos.OrchestratorAction {
				completeAction := &protos.CompleteOrchestrationAction{OrchestrationStatus: expectedStatus}
				if expectedStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED {
					runtime.Stack(stackTraceBuffer, false)
					expectedStackTrace = string(stackTraceBuffer)
					completeAction.FailureDetails = &protos.TaskFailureDetails{
						ErrorType:    "MyError",
						ErrorMessage: "Kah-BOOOM!!",
						StackTrace:   wrapperspb.String(expectedStackTrace),
					}
				} else {
					completeAction.Result = wrapperspb.String(expectedResult)
				}

				return []*protos.OrchestratorAction{{
					OrchestratorActionType: &protos.OrchestratorAction_CompleteOrchestration{
						CompleteOrchestration: completeAction,
					},
				}}
			}

			validateMetadata := func(metadata *api.OrchestrationMetadata) {
				assert.True(t, metadata.IsComplete())
				assert.False(t, metadata.IsRunning())

				if expectedStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED {
					assert.Equal(t, "MyError", metadata.FailureDetails.ErrorType)
					assert.Equal(t, "Kah-BOOOM!!", metadata.FailureDetails.ErrorMessage)
					assert.Equal(t, expectedStackTrace, metadata.FailureDetails.StackTrace.GetValue())
				} else {
					assert.Equal(t, expectedResult, metadata.SerializedOutput)
				}
			}

			// Execute the test, which calls the above callbacks
			workItemProcessingTestLogic(t, be, getOrchestratorActions, validateMetadata)

			// Ensure no more work items
			_, err := be.GetOrchestrationWorkItem(ctx)
			assert.ErrorIs(t, err, backend.ErrNoWorkItems)
		}
	}
}

func Test_ScheduleActivityTasks(t *testing.T) {
	expectedInput := "Hello, activity!"
	expectedName := "MyActivity"
	expectedResult := "42"
	expectedTaskID := int32(7)

	for i, be := range backends {
		initTest(t, be, i, true)

		wi, err := be.GetActivityWorkItem(ctx)
		if !assert.ErrorIs(t, err, backend.ErrNoWorkItems) {
			continue
		}

		// Produce a TaskScheduled event with a particular input
		getOrchestratorActions := func() []*protos.OrchestratorAction {
			return []*protos.OrchestratorAction{
				helpers.NewScheduleTaskAction(expectedTaskID, expectedName, wrapperspb.String(expectedInput)),
			}
		}

		// Make sure the metadata reflects that the orchestration is running
		validateMetadata := func(metadata *api.OrchestrationMetadata) {
			assert.True(t, metadata.IsRunning())
		}

		// Execute the test, which calls the above callbacks
		workItemProcessingTestLogic(t, be, getOrchestratorActions, validateMetadata)

		// Ensure no more orchestration work items
		_, err = be.GetOrchestrationWorkItem(ctx)
		assert.ErrorIs(t, err, backend.ErrNoWorkItems)

		// However, there should be an activity work item
		wi, err = be.GetActivityWorkItem(ctx)
		if assert.NoError(t, err) && assert.NotNil(t, wi) {
			assert.Equal(t, expectedName, wi.NewEvent.GetTaskScheduled().GetName())
			assert.Equal(t, expectedInput, wi.NewEvent.GetTaskScheduled().GetInput().GetValue())
		}

		// Ensure no more activity work items
		_, err = be.GetActivityWorkItem(ctx)
		assert.ErrorIs(t, err, backend.ErrNoWorkItems)

		// Complete the fetched activity work item
		wi.Result = helpers.NewTaskCompletedEvent(expectedTaskID, wrapperspb.String(expectedResult))
		err = be.CompleteActivityWorkItem(ctx, wi)
		if assert.NoError(t, err) {
			// Completing the activity work item should create a new TaskCompleted event
			wi, err := be.GetOrchestrationWorkItem(ctx)
			if assert.NoError(t, err) && assert.NotNil(t, wi) && assert.Len(t, wi.NewEvents, 1) {
				assert.Equal(t, expectedTaskID, wi.NewEvents[0].GetTaskCompleted().GetTaskScheduledId())
				assert.Equal(t, expectedResult, wi.NewEvents[0].GetTaskCompleted().GetResult().GetValue())
			}
		}
	}
}

func Test_ScheduleTimerTasks(t *testing.T) {
	for i, be := range backends {
		initTest(t, be, i, true)

		timerDuration := 1 * time.Second
		expectedFireAt := time.Now().Add(timerDuration)

		// Produce a TimerCreated event with a particular fireat time
		getOrchestratorActions := func() []*protos.OrchestratorAction {
			return []*protos.OrchestratorAction{{
				OrchestratorActionType: &protos.OrchestratorAction_CreateTimer{
					CreateTimer: &protos.CreateTimerAction{FireAt: timestamppb.New(expectedFireAt)},
				},
			}}
		}

		// Make sure the metadata reflects that the orchestration is running
		validateMetadata := func(metadata *api.OrchestrationMetadata) {
			assert.True(t, metadata.IsRunning())
		}

		// Execute the test, which calls the above callbacks
		workItemProcessingTestLogic(t, be, getOrchestratorActions, validateMetadata)

		// Validate that the timer work-item isn't yet visible
		_, err := be.GetOrchestrationWorkItem(ctx)
		assert.ErrorIs(t, err, backend.ErrNoWorkItems)

		// Sleep until the expected visibility time expires
		time.Sleep(timerDuration)

		// Validate that the timer work-item is now visible
		wi, err := be.GetOrchestrationWorkItem(ctx)
		if assert.NoError(t, err) && assert.Equal(t, 1, len(wi.NewEvents)) {
			e := wi.NewEvents[0]
			tf := e.GetTimerFired()
			if assert.NotNil(t, tf) {
				assert.WithinDuration(t, expectedFireAt, tf.FireAt.AsTime(), 0)
			}
		}
	}
}

func Test_AbandonOrchestrationWorkItem(t *testing.T) {
	iid := "abc"

	for i, be := range backends {
		initTest(t, be, i, true)

		if createOrchestrationInstance(t, be, iid) {
			if wi, ok := getOrchestrationWorkItem(t, be, iid); ok {
				if err := be.AbandonOrchestrationWorkItem(ctx, wi); assert.NoError(t, err) {
					// Make sure we can fetch it again immediately after abandoning
					getOrchestrationWorkItem(t, be, iid)
				}
			}
		}
	}
}

func Test_AbandonActivityWorkItem(t *testing.T) {
	for i, be := range backends {
		initTest(t, be, i, true)

		getOrchestratorActions := func() []*protos.OrchestratorAction {
			return []*protos.OrchestratorAction{
				helpers.NewScheduleTaskAction(123, "MyActivity", nil),
			}
		}

		// Make sure the metadata reflects that the orchestration is running
		validateMetadata := func(metadata *api.OrchestrationMetadata) {
			assert.True(t, metadata.IsRunning())
		}

		// Execute the test, which calls the above callbacks
		workItemProcessingTestLogic(t, be, getOrchestratorActions, validateMetadata)

		// The NewScheduleTaskAction should have created an activity work item
		wi, err := be.GetActivityWorkItem(ctx)
		if assert.NoError(t, err) && assert.NotNil(t, wi) {
			// Ensure no more activity work items
			_, err = be.GetActivityWorkItem(ctx)
			assert.ErrorIs(t, err, backend.ErrNoWorkItems)

			if err := be.AbandonActivityWorkItem(ctx, wi); assert.NoError(t, err) {
				// Re-fetch the abandoned activity work item
				wi, err = be.GetActivityWorkItem(ctx)
				assert.Equal(t, "MyActivity", wi.NewEvent.GetTaskScheduled().GetName())
				assert.Equal(t, int32(123), wi.NewEvent.EventId)
				assert.Nil(t, wi.NewEvent.GetTaskScheduled().GetInput())
			}
		}
	}
}

func Test_UninitializedBackend(t *testing.T) {
	for i, be := range backends {
		initTest(t, be, i, false)

		err := be.AbandonOrchestrationWorkItem(ctx, nil)
		assert.Equal(t, err, backend.ErrNotInitialized)
		err = be.CompleteOrchestrationWorkItem(ctx, nil)
		assert.Equal(t, err, backend.ErrNotInitialized)
		err = be.CreateOrchestrationInstance(ctx, nil)
		assert.Equal(t, err, backend.ErrNotInitialized)
		_, err = be.GetOrchestrationMetadata(ctx, api.InstanceID(""))
		assert.Equal(t, err, backend.ErrNotInitialized)
		_, err = be.GetOrchestrationRuntimeState(ctx, nil)
		assert.Equal(t, err, backend.ErrNotInitialized)
		_, err = be.GetOrchestrationWorkItem(ctx)
		assert.Equal(t, err, backend.ErrNotInitialized)
		_, err = be.GetActivityWorkItem(ctx)
		assert.Equal(t, err, backend.ErrNotInitialized)
	}
}

func Test_GetNonExistingMetadata(t *testing.T) {
	for i, be := range backends {
		initTest(t, be, i, true)

		_, err := be.GetOrchestrationMetadata(ctx, api.InstanceID("bogus"))
		assert.ErrorIs(t, err, api.ErrInstanceNotFound)
	}
}

func Test_PurgeOrchestrationState(t *testing.T) {
	for i, be := range backends {
		initTest(t, be, i, true)

		expectedResult := "done!"

		// Produce an ExecutionCompleted event with a particular output
		getOrchestratorActions := func() []*protos.OrchestratorAction {
			return []*protos.OrchestratorAction{{
				OrchestratorActionType: &protos.OrchestratorAction_CompleteOrchestration{
					CompleteOrchestration: &protos.CompleteOrchestrationAction{
						OrchestrationStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
						Result:              wrapperspb.String(expectedResult),
					},
				},
			}}
		}

		// Make sure the orchestration actually completed and get the instance ID
		var instanceID api.InstanceID
		validateMetadata := func(metadata *api.OrchestrationMetadata) {
			instanceID = metadata.InstanceID
			assert.True(t, metadata.IsComplete())
			assert.False(t, metadata.IsRunning())
		}

		// Execute the test, which calls the above callbacks
		workItemProcessingTestLogic(t, be, getOrchestratorActions, validateMetadata)

		// Purge the workflow state
		if err := be.PurgeOrchestrationState(ctx, instanceID); !assert.NoError(t, err) {
			return
		}

		// The metadata should be gone
		if _, err := be.GetOrchestrationMetadata(ctx, instanceID); !assert.ErrorIs(t, err, api.ErrInstanceNotFound) {
			return
		}

		wi := &backend.OrchestrationWorkItem{InstanceID: instanceID}
		state, err := be.GetOrchestrationRuntimeState(ctx, wi)
		assert.NoError(t, err)

		// The state should be empty
		assert.Equal(t, 0, len(state.NewEvents()))
		assert.Equal(t, 0, len(state.OldEvents()))

		// Attempting to purge again should fail with api.ErrInstanceNotFound
		if err := be.PurgeOrchestrationState(ctx, instanceID); !assert.ErrorIs(t, err, api.ErrInstanceNotFound) {
			return
		}
	}
}

func initTest(t *testing.T, be backend.Backend, testIteration int, createTaskHub bool) {
	t.Logf("(%d) Testing %s...", testIteration, reflect.TypeOf(be).String())
	err := be.DeleteTaskHub(ctx)
	if err != nil {
		assert.Equal(t, backend.ErrTaskHubNotFound, err)
	}
	if createTaskHub {
		err := be.CreateTaskHub(ctx)
		assert.NoError(t, err)
	}
}

func workItemProcessingTestLogic(
	t *testing.T,
	be backend.Backend,
	getOrchestratorActions func() []*protos.OrchestratorAction,
	validateMetadata func(metadata *api.OrchestrationMetadata),
) {
	expectedID := "myinstance"

	startTime := time.Now().UTC()
	if createOrchestrationInstance(t, be, expectedID) {
		if wi, ok := getOrchestrationWorkItem(t, be, expectedID); ok {
			if state, ok := getOrchestrationRuntimeState(t, be, wi); ok {
				// Update the state with new events. Normally the worker logic would do this.
				for _, e := range wi.NewEvents {
					state.AddEvent(e)
				}

				actions := getOrchestratorActions()
				_, err := state.ApplyActions(actions, nil)
				if assert.NoError(t, err) {
					wi.State = state
					err := be.CompleteOrchestrationWorkItem(ctx, wi)
					if assert.NoError(t, err) {
						// Validate runtime state
						if state, ok = getOrchestrationRuntimeState(t, be, wi); ok {
							createdTime, err := state.CreatedTime()
							if assert.NoError(t, err) {
								assert.GreaterOrEqual(t, createdTime, startTime)
							}

							// State should be initialized with only "old" events
							assert.Empty(t, state.NewEvents())
							assert.NotEmpty(t, state.OldEvents())
							// Validate orchestration metadata
							if metadata, ok := getOrchestrationMetadata(t, be, state.InstanceID()); ok {
								assert.Equal(t, defaultName, metadata.Name)
								assert.Equal(t, defaultInput, metadata.SerializedInput)
								assert.Less(t, createdTime.Sub(metadata.CreatedAt).Abs(), time.Microsecond) // Some database backends (like postgres) don't support sub-microsecond precision
								assert.Equal(t, state.RuntimeStatus(), metadata.RuntimeStatus)

								validateMetadata(metadata)
							}
						}
					}
				}
			}
		}
	}
}

func createOrchestrationInstance(t assert.TestingT, be backend.Backend, instanceID string) bool {
	e := &protos.HistoryEvent{
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{
				Name:                  defaultName,
				OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: instanceID},
				Input:                 wrapperspb.String(defaultInput),
			},
		},
	}
	policy := &protos.OrchestrationIdReusePolicy{}
	err := be.CreateOrchestrationInstance(ctx, e, backend.WithOrchestrationIdReusePolicy(policy))
	return assert.NoError(t, err)
}

func getOrchestrationWorkItem(t assert.TestingT, be backend.Backend, expectedInstanceID string) (*backend.OrchestrationWorkItem, bool) {
	wi, err := be.GetOrchestrationWorkItem(ctx)
	if assert.NoError(t, err) && assert.NotNil(t, wi) {
		assert.NotEmpty(t, wi.LockedBy)
		return wi, assert.Equal(t, expectedInstanceID, string(wi.InstanceID))
	}

	return nil, false
}

func getOrchestrationRuntimeState(t assert.TestingT, be backend.Backend, wi *backend.OrchestrationWorkItem) (*backend.OrchestrationRuntimeState, bool) {
	state, err := be.GetOrchestrationRuntimeState(ctx, wi)
	if assert.NoError(t, err) && assert.NotNil(t, state) {
		iid := state.InstanceID()
		return state, assert.Equal(t, wi.InstanceID, iid)
	}

	return nil, false
}

func getOrchestrationMetadata(t assert.TestingT, be backend.Backend, iid api.InstanceID) (*api.OrchestrationMetadata, bool) {
	metadata, err := be.GetOrchestrationMetadata(ctx, iid)
	if assert.NoError(t, err) && assert.NotNil(t, metadata) {
		return metadata, assert.Equal(t, iid, metadata.InstanceID)
	}

	return nil, false
}
