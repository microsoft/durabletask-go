package durabletaskscheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestRegisterScheduledTasksUsesUnversionedSystemNames(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, RegisterScheduledTasks(registry))
	snapshot := registry.Snapshot()
	require.Contains(t, snapshot.Entities, "schedule")
	require.Contains(t, snapshot.Orchestrators, task.TaskRegistration{
		Name: ExecuteScheduleOperationOrchestratorName,
	})
	require.Contains(t, snapshot.Orchestrators, task.TaskRegistration{
		Name: ExecuteScheduledTaskOrchestratorName,
	})
	require.NotContains(t, snapshot.Orchestrators, task.TaskRegistration{
		Name: ExecuteScheduleOperationOrchestratorName, Version: "v1",
	})
	require.NotNil(t, WithScheduledTasks())
}

func TestScheduleStateUsesDotNetCompatibleJSON(t *testing.T) {
	state := scheduleState{
		Status:         ScheduleStatusActive,
		ExecutionToken: "0123456789abcdef0123456789abcdef",
		ScheduleConfiguration: &scheduleConfiguration{
			ScheduleID:        "nightly",
			OrchestrationName: "Backup",
			Interval:          dotNetSpan(5 * time.Minute),
		},
	}
	payload, err := json.Marshal(state)
	require.NoError(t, err)
	require.JSONEq(t, `{"Status":1,"ExecutionToken":"0123456789abcdef0123456789abcdef","LastRunAt":null,"NextRunAt":null,"ScheduleCreatedAt":null,"ScheduleLastModifiedAt":null,"ScheduleConfiguration":{"OrchestrationName":"Backup","ScheduleId":"nightly","StartAt":null,"EndAt":null,"Interval":"00:05:00","StartImmediatelyIfLate":false}}`, string(payload))

	var decoded scheduleState
	require.NoError(t, json.Unmarshal([]byte(`{
		"Status": 2,
		"ExecutionToken": "token",
		"ScheduleConfiguration": {
			"ScheduleId": "from-dotnet",
			"OrchestrationName": "Run",
			"Interval": "1.02:03:04.5000000",
			"StartImmediatelyIfLate": true
		}
	}`), &decoded))
	require.Equal(t, ScheduleStatusPaused, decoded.Status)
	require.Equal(t, 26*time.Hour+3*time.Minute+4*time.Second+500*time.Millisecond,
		time.Duration(decoded.ScheduleConfiguration.Interval))
	span, err := json.Marshal(dotNetSpan(26*time.Hour + 3*time.Minute + 4*time.Second + 500*time.Millisecond))
	require.NoError(t, err)
	require.Equal(t, `"1.02:03:04.5000000"`, string(span))
}

func TestScheduleOptionsUseDotNetCompatibleWireJSON(t *testing.T) {
	payload, err := json.Marshal(ScheduleCreationOptions{
		ScheduleID:        "daily",
		OrchestrationName: "Backup",
		Interval:          5 * time.Minute,
	})
	require.NoError(t, err)
	require.JSONEq(t, `{
		"ScheduleId":"daily",
		"OrchestrationName":"Backup",
		"Interval":"00:05:00",
		"StartImmediatelyIfLate":false
	}`, string(payload))

	var decoded ScheduleCreationOptions
	require.NoError(t, json.Unmarshal([]byte(`{
		"ScheduleId":"dotnet",
		"OrchestrationName":"Run",
		"Interval":"1.02:03:04.5000000",
		"StartImmediatelyIfLate":true
	}`), &decoded))
	require.Equal(t, "dotnet", decoded.ScheduleID)
	require.Equal(t, 26*time.Hour+3*time.Minute+4*time.Second+500*time.Millisecond, decoded.Interval)
}

func TestDetermineNextRunHandlesLateStartWithoutOverflow(t *testing.T) {
	now := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	state := &scheduleState{ScheduleCreatedAt: now.Add(-100000 * time.Hour)}
	config := &scheduleConfiguration{
		Interval: dotNetSpan(time.Second),
	}
	next, err := determineNextRun(state, config, now)
	require.NoError(t, err)
	require.Equal(t, now.Add(time.Second), next)

	state.LastRunAt = now
	config.StartImmediatelyIfLate = true
	next, err = determineNextRun(state, config, now)
	require.NoError(t, err)
	require.Equal(t, now.Add(time.Second), next)
}

func TestScheduleErrorsSupportIsAndAs(t *testing.T) {
	err := invalidTransition("id", ScheduleStatusActive, ScheduleStatusPaused, pauseScheduleOperation)
	require.ErrorIs(t, err, ErrScheduleInvalidTransition)
	var transition *ScheduleInvalidTransitionError
	require.ErrorAs(t, err, &transition)
	require.Equal(t, "id", transition.ScheduleID)

	notFound := &ScheduleNotFoundError{ScheduleID: "missing"}
	require.ErrorIs(t, notFound, ErrScheduleNotFound)
	var typedNotFound *ScheduleNotFoundError
	require.ErrorAs(t, notFound, &typedNotFound)
}

func TestScheduleClientTargetsUnversionedOperationOrchestrator(t *testing.T) {
	backend := &scheduleClientBackend{}
	client := &ScheduledTaskClient{client: backend}
	handle, err := client.GetScheduleClient("daily")
	require.NoError(t, err)
	require.Equal(t, "daily", handle.ID())
	require.NoError(t, handle.Pause(context.Background()))
	require.Equal(t, ExecuteScheduleOperationOrchestratorName, backend.orchestrator)
	require.NotNil(t, backend.version)
	require.Empty(t, backend.version.GetValue())
}

func TestScheduleClientGetReturnsNilForMissingSchedule(t *testing.T) {
	backend := &scheduleClientBackend{}
	client := &ScheduledTaskClient{client: backend}
	description, err := client.Get(context.Background(), "missing")
	require.NoError(t, err)
	require.Nil(t, description)
}

func TestScheduleEntityCreatesPascalStateAndRunSignal(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, RegisterScheduledTasks(registry))
	executor := task.NewTaskExecutor(registry)
	input, err := api.DefaultDataConverter().Serialize(ScheduleCreationOptions{
		ScheduleID:             "daily",
		OrchestrationName:      "Backup",
		OrchestrationInput:     `{"database":"main"}`,
		Interval:               time.Hour,
		StartImmediatelyIfLate: true,
	})
	require.NoError(t, err)

	result, err := executor.(entityExecutor).ExecuteEntity(context.Background(), &protos.EntityBatchRequest{
		InstanceId: api.NewEntityID(ScheduleEntityName, "daily").String(),
		Operations: []*protos.OperationRequest{{
			Operation: createScheduleOperation,
			Input:     wrapperspb.String(input),
		}},
	})
	require.NoError(t, err)
	require.Len(t, result.Actions, 1)
	require.Equal(t, runScheduleOperation, result.Actions[0].GetSendSignal().GetName())
	require.Contains(t, result.EntityState.GetValue(), `"Status":1`)
	require.Contains(t, result.EntityState.GetValue(), `"Interval":"01:00:00"`)
}

func TestScheduleEntityOperationNamesAreCaseInsensitive(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, RegisterScheduledTasks(registry))
	executor := task.NewTaskExecutor(registry)
	input, err := api.DefaultDataConverter().Serialize(ScheduleCreationOptions{
		ScheduleID: "daily", OrchestrationName: "Backup", Interval: time.Hour,
	})
	require.NoError(t, err)
	result, err := executor.(entityExecutor).ExecuteEntity(context.Background(), &protos.EntityBatchRequest{
		InstanceId: api.NewEntityID(ScheduleEntityName, "daily").String(),
		Operations: []*protos.OperationRequest{{
			Operation: strings.ToLower(createScheduleOperation),
			Input:     wrapperspb.String(input),
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, result.EntityState)
}

func TestScheduleEntityRunsOnceAndCancelsStaleTokens(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, RegisterScheduledTasksWithDefaultVersion(registry, "v1"))
	executor := task.NewTaskExecutor(registry)
	input, err := api.DefaultDataConverter().Serialize(ScheduleCreationOptions{
		ScheduleID:             "daily",
		OrchestrationName:      "Backup",
		OrchestrationInput:     `{"database":"main"}`,
		Interval:               time.Hour,
		StartAt:                time.Now().Add(-time.Minute),
		StartImmediatelyIfLate: true,
	})
	require.NoError(t, err)
	created, err := executor.(entityExecutor).ExecuteEntity(context.Background(), &protos.EntityBatchRequest{
		InstanceId: api.NewEntityID(ScheduleEntityName, "daily").String(),
		Operations: []*protos.OperationRequest{{
			Operation: createScheduleOperation,
			Input:     wrapperspb.String(input),
		}},
	})
	require.NoError(t, err)
	var state scheduleState
	require.NoError(t, json.Unmarshal([]byte(created.EntityState.GetValue()), &state))

	token, err := api.DefaultDataConverter().Serialize(state.ExecutionToken)
	require.NoError(t, err)
	ran, err := executor.(entityExecutor).ExecuteEntity(context.Background(), &protos.EntityBatchRequest{
		InstanceId:  api.NewEntityID(ScheduleEntityName, "daily").String(),
		EntityState: created.EntityState,
		Operations: []*protos.OperationRequest{{
			Operation: runScheduleOperation,
			Input:     wrapperspb.String(token),
		}},
	})
	require.NoError(t, err)
	require.Len(t, ran.Actions, 2)
	require.Equal(t, `{"database":"main"}`, ran.Actions[0].GetStartNewOrchestration().Input.GetValue())
	require.Equal(t, "v1", ran.Actions[0].GetStartNewOrchestration().Version.GetValue())
	require.Contains(t, ran.Actions[0].GetStartNewOrchestration().InstanceId, "+00:00")
	require.Equal(t, runScheduleOperation, ran.Actions[1].GetSendSignal().GetName())

	stale, err := api.DefaultDataConverter().Serialize("not-current")
	require.NoError(t, err)
	staleResult, err := executor.(entityExecutor).ExecuteEntity(context.Background(), &protos.EntityBatchRequest{
		InstanceId:  api.NewEntityID(ScheduleEntityName, "daily").String(),
		EntityState: ran.EntityState,
		Operations: []*protos.OperationRequest{{
			Operation: runScheduleOperation,
			Input:     wrapperspb.String(stale),
		}},
	})
	require.NoError(t, err)
	require.Empty(t, staleResult.Actions)
}

func TestScheduleEntityUsesWrapperForTagsContextAndRetries(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, RegisterScheduledTasksWithDefaultVersion(registry, "v2"))
	executor := task.NewTaskExecutor(
		registry,
		task.WithUnversionedOrchestratorNames(
			ExecuteScheduleOperationOrchestratorName,
			ExecuteScheduledTaskOrchestratorName,
		),
	)
	input, err := api.DefaultDataConverter().Serialize(ScheduleCreationOptions{
		ScheduleID:             "daily",
		OrchestrationName:      "Backup",
		OrchestrationInput:     `{"database":"main"}`,
		Interval:               time.Hour,
		StartAt:                time.Now().Add(-time.Minute),
		StartImmediatelyIfLate: true,
		Tags:                   map[string]string{"schedule": "daily"},
		ContextFields:          api.ContextFields{"tenant": "north"},
		RetryPolicy: &ScheduleRetryPolicy{
			MaxAttempts:          3,
			InitialRetryInterval: time.Second,
			BackoffCoefficient:   2,
			MaxRetryInterval:     time.Minute,
			RetryTimeout:         time.Hour,
		},
	})
	require.NoError(t, err)
	created, err := executor.(entityExecutor).ExecuteEntity(context.Background(), &protos.EntityBatchRequest{
		InstanceId: api.NewEntityID(ScheduleEntityName, "daily").String(),
		Operations: []*protos.OperationRequest{{
			Operation: createScheduleOperation,
			Input:     wrapperspb.String(input),
		}},
	})
	require.NoError(t, err)
	var state scheduleState
	require.NoError(t, json.Unmarshal([]byte(created.EntityState.GetValue()), &state))
	token, err := api.DefaultDataConverter().Serialize(state.ExecutionToken)
	require.NoError(t, err)
	ran, err := executor.(entityExecutor).ExecuteEntity(context.Background(), &protos.EntityBatchRequest{
		InstanceId:  api.NewEntityID(ScheduleEntityName, "daily").String(),
		EntityState: created.EntityState,
		Operations: []*protos.OperationRequest{{
			Operation: runScheduleOperation,
			Input:     wrapperspb.String(token),
		}},
	})
	require.NoError(t, err)
	start := ran.Actions[0].GetStartNewOrchestration()
	require.NotNil(t, start)
	require.Equal(t, ExecuteScheduledTaskOrchestratorName, start.Name)
	require.NotNil(t, start.Version)
	require.Empty(t, start.Version.GetValue())
	var request scheduledTaskRequest
	require.NoError(t, api.DefaultDataConverter().Deserialize(start.Input.GetValue(), &request))
	require.Equal(t, "Backup", request.OrchestrationName)
	require.Equal(t, "v2", request.OrchestrationVersion)
	require.Equal(t, map[string]string{"schedule": "daily"}, request.Tags)
	require.Equal(t, api.ContextFields{"tenant": "north"}, request.ContextFields)
	require.Equal(t, 3, request.RetryPolicy.MaxAttempts)
}

func TestScheduleEntityStateIsJSONWithCustomInputConverter(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, RegisterScheduledTasks(registry))
	converter := prefixedConverter{}
	executor := task.NewTaskExecutor(registry, task.WithDataConverter(converter))
	input, err := converter.Serialize(ScheduleCreationOptions{
		ScheduleID: "daily", OrchestrationName: "Backup", Interval: time.Hour,
		TypedOrchestrationInput: map[string]string{
			"database": "main",
		},
	})
	require.NoError(t, err)
	result, err := executor.(entityExecutor).ExecuteEntity(context.Background(), &protos.EntityBatchRequest{
		InstanceId: api.NewEntityID(ScheduleEntityName, "daily").String(),
		Operations: []*protos.OperationRequest{{
			Operation: createScheduleOperation,
			Input:     wrapperspb.String(input),
		}},
	})
	require.NoError(t, err)
	require.False(t, strings.HasPrefix(result.EntityState.GetValue(), "converter:"))
	var state scheduleState
	require.NoError(t, json.Unmarshal([]byte(result.EntityState.GetValue()), &state))
	require.Equal(t, "daily", state.ScheduleConfiguration.ScheduleID)
	require.Equal(t, `converter:{"database":"main"}`, state.ScheduleConfiguration.OrchestrationInput)
}

func TestScheduleDescriptionReadsInputWithConverter(t *testing.T) {
	state, err := json.Marshal(scheduleState{
		Status: ScheduleStatusActive,
		ScheduleConfiguration: &scheduleConfiguration{
			ScheduleID:         "daily",
			OrchestrationName:  "Backup",
			OrchestrationInput: `converter:"main"`,
			Interval:           dotNetSpan(time.Hour),
		},
	})
	require.NoError(t, err)
	description, err := scheduleDescription(&api.EntityMetadata{
		InstanceID:      api.NewEntityID(ScheduleEntityName, "daily"),
		SerializedState: string(state),
	}, prefixedConverter{})
	require.NoError(t, err)
	var input string
	require.NoError(t, description.ReadInput(&input))
	require.Equal(t, "main", input)
}

func TestScheduleOperationFailureReturnsTypedErrors(t *testing.T) {
	err := scheduleOperationFailure("daily", pauseScheduleOperation, &api.OrchestrationMetadata{
		RuntimeStatus: api.RUNTIME_STATUS_FAILED,
		FailureDetails: &api.FailureDetails{
			ErrorType: scheduleInvalidTransitionType,
			Properties: map[string]any{
				"scheduleId": "daily",
				"from":       float64(ScheduleStatusPaused),
				"to":         float64(ScheduleStatusPaused),
				"operation":  pauseScheduleOperation,
			},
		},
	})
	require.ErrorIs(t, err, ErrScheduleInvalidTransition)
	var transition *ScheduleInvalidTransitionError
	require.ErrorAs(t, err, &transition)
	require.Equal(t, ScheduleStatusPaused, transition.From)

	err = scheduleOperationFailure("daily", "Run", &api.OrchestrationMetadata{
		RuntimeStatus:  api.RUNTIME_STATUS_FAILED,
		FailureDetails: &api.FailureDetails{ErrorMessage: "failed"},
	})
	require.ErrorIs(t, err, ErrScheduleOperationFailed)
	var operation *ScheduleOperationError
	require.ErrorAs(t, err, &operation)
	require.Equal(t, "failed", operation.FailureDetails.ErrorMessage)
}

func TestScheduleEntityDoesNotRecreateDeletedStateForStaleRun(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, RegisterScheduledTasks(registry))
	executor := task.NewTaskExecutor(registry)
	token, err := api.DefaultDataConverter().Serialize("stale")
	require.NoError(t, err)
	state, err := json.Marshal(scheduleState{Status: ScheduleStatusUninitialized})
	require.NoError(t, err)
	result, err := executor.(entityExecutor).ExecuteEntity(context.Background(), &protos.EntityBatchRequest{
		InstanceId:  api.NewEntityID(ScheduleEntityName, "daily").String(),
		EntityState: wrapperspb.String(string(state)),
		Operations: []*protos.OperationRequest{{
			Operation: runScheduleOperation,
			Input:     wrapperspb.String(token),
		}},
	})
	require.NoError(t, err)
	require.Nil(t, result.EntityState)
}

func TestScheduleEntityIgnoresStaleRetirementDelete(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, RegisterScheduledTasks(registry))
	executor := task.NewTaskExecutor(registry)
	state, err := json.Marshal(scheduleState{
		Status:         ScheduleStatusActive,
		ExecutionToken: "current",
		ScheduleConfiguration: &scheduleConfiguration{
			ScheduleID: "daily", OrchestrationName: "Backup", Interval: dotNetSpan(time.Hour),
		},
	})
	require.NoError(t, err)
	staleToken, err := api.DefaultDataConverter().Serialize("stale")
	require.NoError(t, err)
	result, err := executor.(entityExecutor).ExecuteEntity(context.Background(), &protos.EntityBatchRequest{
		InstanceId:  api.NewEntityID(ScheduleEntityName, "daily").String(),
		EntityState: wrapperspb.String(string(state)),
		Operations: []*protos.OperationRequest{{
			Operation: deleteScheduleOperation,
			Input:     wrapperspb.String(staleToken),
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, result.EntityState)

	result, err = executor.(entityExecutor).ExecuteEntity(context.Background(), &protos.EntityBatchRequest{
		InstanceId:  api.NewEntityID(ScheduleEntityName, "daily").String(),
		EntityState: result.EntityState,
		Operations: []*protos.OperationRequest{{
			Operation: deleteScheduleOperation,
		}},
	})
	require.NoError(t, err)
	require.Nil(t, result.EntityState)
}

func TestCreateReactivatesPausedSchedule(t *testing.T) {
	now := time.Now().UTC()
	state := scheduleState{
		Status:            ScheduleStatusPaused,
		ScheduleCreatedAt: now.Add(-time.Hour),
		ScheduleConfiguration: &scheduleConfiguration{
			ScheduleID: "daily",
		},
	}
	ctx := &task.EntityContext{ID: api.NewEntityID(ScheduleEntityName, "daily")}
	require.NoError(t, createSchedule(ctx, &state, ScheduleCreationOptions{
		ScheduleID: "daily", OrchestrationName: "Backup", Interval: time.Hour,
	}))
	require.Equal(t, ScheduleStatusActive, state.Status)
	require.Equal(t, now.Add(-time.Hour).Unix(), state.ScheduleCreatedAt.Unix())
}

func TestValidateCreation(t *testing.T) {
	err := validateCreation(ScheduleCreationOptions{
		ScheduleID:        "id",
		OrchestrationName: "run",
		Interval:          time.Millisecond,
	})
	require.ErrorIs(t, err, ErrScheduleValidation)
	require.False(t, errors.Is(err, ErrScheduleNotFound))

	err = validateCreation(ScheduleCreationOptions{
		ScheduleID:              "id",
		OrchestrationName:       "run",
		OrchestrationInstanceID: "fixed",
		Interval:                time.Second,
		RetryPolicy: &ScheduleRetryPolicy{
			MaxAttempts:          2,
			InitialRetryInterval: time.Second,
		},
	})
	require.ErrorIs(t, err, ErrScheduleValidation)
}

type scheduleClientBackend struct {
	orchestrator string
	version      *wrapperspb.StringValue
	fetchErr     error
}

func (b *scheduleClientBackend) ScheduleNewOrchestration(_ context.Context, orchestrator string, options ...api.NewOrchestrationOptions) (api.InstanceID, error) {
	request := &protos.CreateInstanceRequest{}
	for _, option := range options {
		if err := option(request, api.DefaultDataConverter()); err != nil {
			return "", err
		}
	}
	b.orchestrator = orchestrator
	b.version = request.Version
	return "operation", nil
}

func (b *scheduleClientBackend) WaitForOrchestrationCompletion(context.Context, api.InstanceID, ...api.FetchOrchestrationMetadataOptions) (*api.OrchestrationMetadata, error) {
	return &api.OrchestrationMetadata{RuntimeStatus: api.RUNTIME_STATUS_COMPLETED}, nil
}

func (b *scheduleClientBackend) GetEntity(context.Context, api.EntityID, ...api.GetEntityOptions) (*api.EntityMetadata, error) {
	return nil, b.fetchErr
}

func (b *scheduleClientBackend) QueryEntities(context.Context, api.EntityQuery) (*api.EntityQueryResults, error) {
	return nil, nil
}

type entityExecutor interface {
	ExecuteEntity(context.Context, *protos.EntityBatchRequest) (*protos.EntityBatchResult, error)
}

type prefixedConverter struct{}

func (prefixedConverter) Serialize(value any) (string, error) {
	payload, err := json.Marshal(value)
	return "converter:" + string(payload), err
}

func (prefixedConverter) Deserialize(payload string, target any) error {
	if !strings.HasPrefix(payload, "converter:") {
		return fmt.Errorf("missing converter prefix")
	}
	return json.Unmarshal([]byte(strings.TrimPrefix(payload, "converter:")), target)
}

var _ scheduledTaskBackend = (*scheduleClientBackend)(nil)
var _ durabletaskclient.TaskHubGrpcWorkerOption = WithScheduledTasks()

func TestScheduleRetryPolicyFromPublicAppliesDefaultsWithoutMutation(t *testing.T) {
	public := &ScheduleRetryPolicy{InitialRetryInterval: time.Second}
	normalized, err := scheduleRetryPolicyFromPublic(public)
	require.NoError(t, err)
	require.Equal(t, 1, normalized.MaxAttempts)
	require.Equal(t, 1.0, normalized.BackoffCoefficient)
	require.Equal(t, dotNetSpan(math.MaxInt64), normalized.MaxRetryInterval)
	require.Equal(t, dotNetSpan(math.MaxInt64), normalized.RetryTimeout)
	require.Zero(t, public.MaxAttempts)
	require.Zero(t, public.BackoffCoefficient)
	require.Zero(t, public.MaxRetryInterval)
	require.Zero(t, public.RetryTimeout)
}
