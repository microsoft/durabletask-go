package durabletaskscheduler_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/durabletaskscheduler"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/payload"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func emulatorOptions(t *testing.T) *durabletaskscheduler.Options {
	t.Helper()
	if connectionString := os.Getenv("DTS_CONNECTION_STRING"); connectionString != "" {
		options, err := durabletaskscheduler.NewOptionsFromConnectionString(connectionString)
		require.NoError(t, err)
		return options
	}

	endpoint := os.Getenv("DTS_EMULATOR_ENDPOINT")
	if endpoint == "" {
		t.Skip("set DTS_CONNECTION_STRING or DTS_EMULATOR_ENDPOINT to run DTS emulator tests")
	}
	taskHub := os.Getenv("DTS_TASK_HUB")
	if taskHub == "" {
		taskHub = "default"
	}
	options, err := durabletaskscheduler.NewOptionsFromConnectionString(
		fmt.Sprintf("Endpoint=%s;TaskHub=%s;Authentication=None", endpoint, taskHub),
	)
	require.NoError(t, err)
	return options
}

func startEmulatorClientAndWorker(
	t *testing.T,
	registry *task.TaskRegistry,
	additionalWorkerOptions ...durabletaskclient.TaskHubGrpcWorkerOption,
) (*durabletaskscheduler.Client, *durabletaskclient.TaskHubGrpcWorker, *durabletaskscheduler.Options) {
	t.Helper()
	options := emulatorOptions(t)
	logger := api.DefaultLogger()

	managementClient, err := durabletaskscheduler.NewClient(context.Background(), options, logger)
	require.NoError(t, err)
	workerOptions := []durabletaskclient.TaskHubGrpcWorkerOption{
		durabletaskclient.WithMaxConcurrentOrchestrationWorkItems(4),
		durabletaskclient.WithMaxConcurrentActivityWorkItems(8),
		durabletaskclient.WithMaxConcurrentEntityWorkItems(8),
		durabletaskclient.WithWorkerSilentDisconnectTimeout(15 * time.Second),
	}
	workerOptions = append(workerOptions, additionalWorkerOptions...)
	worker, err := durabletaskscheduler.NewWorker(options, registry, logger, workerOptions...)
	require.NoError(t, err)
	require.NoError(t, worker.Start(context.Background()))

	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, worker.Shutdown(shutdownCtx))
		require.NoError(t, managementClient.Close())
	})
	return managementClient, worker, options
}

type dtsPayload struct {
	Value int
}

type dtsDataConverter struct{}

func (dtsDataConverter) Serialize(value any) (string, error) {
	payload, ok := value.(dtsPayload)
	if !ok {
		return "", fmt.Errorf("unsupported DTS payload %T", value)
	}
	return "dts:" + strconv.Itoa(payload.Value), nil
}

func (dtsDataConverter) Deserialize(payload string, target any) error {
	if !strings.HasPrefix(payload, "dts:") {
		return fmt.Errorf("unexpected DTS payload %q", payload)
	}
	value, err := strconv.Atoi(strings.TrimPrefix(payload, "dts:"))
	if err != nil {
		return err
	}
	decoded, ok := target.(*dtsPayload)
	if !ok {
		return fmt.Errorf("unsupported DTS target %T", target)
	}
	decoded.Value = value
	return nil
}

func TestDTSEmulatorCustomConverterAndVersionMigration(t *testing.T) {
	converter := dtsDataConverter{}
	options := emulatorOptions(t)
	options.DataConverter = converter
	options.Versioning = &task.VersioningOptions{
		DefaultVersion: "1.0",
		MatchStrategy:  task.VersionMatchNone,
	}
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddActivityNVersion("DTSIncrement", "1.0", func(ctx task.ActivityContext) (any, error) {
		var input dtsPayload
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		input.Value++
		return input, nil
	}))
	require.NoError(t, registry.AddOrchestratorNVersion("DTSConverter", "1.0", func(ctx *task.OrchestrationContext) (any, error) {
		var input dtsPayload
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		var result dtsPayload
		if err := ctx.CallActivity("DTSIncrement", task.WithActivityInput(input)).Await(&result); err != nil {
			return nil, err
		}
		ctx.ContinueAsNew(result, task.WithContinueAsNewVersion("2.0"))
		return nil, nil
	}))
	require.NoError(t, registry.AddOrchestratorNVersion("DTSConverter", "2.0", func(ctx *task.OrchestrationContext) (any, error) {
		var input dtsPayload
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		input.Value++
		return input, nil
	}))

	logger := api.DefaultLogger()
	managementClient, err := durabletaskscheduler.NewClient(context.Background(), options, logger)
	require.NoError(t, err)
	worker, err := durabletaskscheduler.NewWorker(
		options,
		registry,
		logger,
		durabletaskclient.WithAutoWorkItemFilters(),
	)
	require.NoError(t, err)
	require.NoError(t, worker.Start(context.Background()))
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, worker.Shutdown(shutdownCtx))
		require.NoError(t, managementClient.Close())
	})

	testCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	instanceID, err := managementClient.ScheduleNewOrchestration(
		testCtx,
		"DTSConverter",
		api.WithInput(dtsPayload{Value: 1}),
	)
	require.NoError(t, err)
	metadata, err := managementClient.WaitForOrchestrationCompletion(testCtx, instanceID)
	require.NoError(t, err)
	require.Equal(t, "2.0", metadata.Version)
	var output dtsPayload
	require.NoError(t, metadata.ReadOutput(&output))
	require.Equal(t, 3, output.Value)
}

func TestDTSEmulatorMixedVersionWorkers(t *testing.T) {
	baseOptions := emulatorOptions(t)
	logger := api.DefaultLogger()
	managementClient, err := durabletaskscheduler.NewClient(context.Background(), baseOptions, logger)
	require.NoError(t, err)

	newWorker := func(version, output string) *durabletaskclient.TaskHubGrpcWorker {
		registry := task.NewTaskRegistry()
		require.NoError(t, registry.AddOrchestratorNVersion(
			"DTSMixedVersion",
			version,
			func(*task.OrchestrationContext) (any, error) {
				return output, nil
			},
		))
		options := *baseOptions
		options.WorkerID = "go-mixed-" + strings.ReplaceAll(version, ".", "-") + "-" + uuid.NewString()
		options.Versioning = &task.VersioningOptions{
			Version:         version,
			DefaultVersion:  version,
			MatchStrategy:   task.VersionMatchStrict,
			FailureStrategy: task.VersionFailureReject,
		}
		worker, workerErr := durabletaskscheduler.NewWorker(
			&options,
			registry,
			logger,
			durabletaskclient.WithAutoWorkItemFilters(),
		)
		require.NoError(t, workerErr)
		require.NoError(t, worker.Start(context.Background()))
		return worker
	}

	workerV1 := newWorker("1.0", "worker-v1")
	workerV2 := newWorker("2.0", "worker-v2")
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, workerV1.Shutdown(shutdownCtx))
		require.NoError(t, workerV2.Shutdown(shutdownCtx))
		require.NoError(t, managementClient.Close())
	})

	testCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for version, expected := range map[string]string{"1.0": "worker-v1", "2.0": "worker-v2"} {
		instanceID, scheduleErr := managementClient.ScheduleNewOrchestration(
			testCtx,
			"DTSMixedVersion",
			api.WithVersion(version),
		)
		require.NoError(t, scheduleErr)
		metadata, waitErr := managementClient.WaitForOrchestrationCompletion(testCtx, instanceID)
		require.NoError(t, waitErr)
		var output string
		require.NoError(t, metadata.ReadOutput(&output))
		require.Equal(t, expected, output)
		require.Equal(t, version, metadata.Version)
	}
}

func TestDTSEmulatorAdvancedManagementOperations(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("DTSAdvancedComplete", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}

		return input, nil
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSAdvancedWait", func(ctx *task.OrchestrationContext) (any, error) {
		if err := ctx.CreateTimer(time.Hour).Await(nil); err != nil {
			return nil, err
		}
		return "done", nil
	}))
	var rewindAttempts atomic.Int32
	require.NoError(t, registry.AddActivityN("DTSAdvancedRewindActivity", func(task.ActivityContext) (any, error) {
		if rewindAttempts.Add(1) == 1 {
			return nil, errors.New("first attempt fails")
		}
		return "recovered", nil
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSAdvancedRewind", func(ctx *task.OrchestrationContext) (any, error) {
		var result string
		if err := ctx.CallActivity("DTSAdvancedRewindActivity").Await(&result); err != nil {
			return nil, err
		}
		return result, nil
	}))
	managementClient, _, _ := startEmulatorClientAndWorker(t, registry)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	t.Run("create-task-hub", func(t *testing.T) {
		err := managementClient.CreateTaskHub(ctx)
		if errors.Is(err, api.ErrFeatureNotSupported) {
			t.Log("DTS emulator limitation: CreateTaskHub is not implemented")
			return
		}
		require.NoError(t, err)
	})
	prefix := "go-advanced-" + uuid.NewString()
	completedIDs := make([]api.InstanceID, 0, 3)
	for index := range 3 {
		id := api.InstanceID(fmt.Sprintf("%s-%d", prefix, index))
		completedIDs = append(completedIDs, id)
		_, err := managementClient.ScheduleNewOrchestration(
			ctx,
			"DTSAdvancedComplete",
			api.WithInstanceID(id),
			api.WithInput(fmt.Sprintf("value-%d", index)),
			api.WithTags(map[string]string{"group": fmt.Sprintf("%d", index%2)}),
		)
		require.NoError(t, err)
		_, err = managementClient.WaitForOrchestrationCompletion(ctx, id)
		require.NoError(t, err)
	}

	t.Run("query", func(t *testing.T) {
		result, err := managementClient.QueryInstances(ctx, api.OrchestrationQuery{
			InstanceIDPrefix: prefix,
			PageSize:         1,
			Tags:             map[string]string{"group": "0"},
		})
		if errors.Is(err, api.ErrFeatureNotSupported) {
			t.Log("DTS emulator limitation: QueryInstances is not implemented")
			return
		}
		require.NoError(t, err)
		require.Len(t, result.Orchestrations, 1)
		require.Contains(t, []api.InstanceID{completedIDs[0], completedIDs[2]}, result.Orchestrations[0].InstanceID)
		require.NotEmpty(t, result.ContinuationToken)
		next, err := managementClient.QueryInstances(ctx, api.OrchestrationQuery{
			InstanceIDPrefix:  prefix,
			PageSize:          1,
			Tags:              map[string]string{"group": "0"},
			ContinuationToken: result.ContinuationToken,
		})
		require.NoError(t, err)
		require.Len(t, next.Orchestrations, 1)
		require.Contains(t, []api.InstanceID{completedIDs[0], completedIDs[2]}, next.Orchestrations[0].InstanceID)
		require.NotEqual(t, result.Orchestrations[0].InstanceID, next.Orchestrations[0].InstanceID)
	})

	t.Run("list-instance-ids", func(t *testing.T) {
		listed := make(map[api.InstanceID]struct{})
		token := ""
		for range 100 {
			result, err := managementClient.ListInstanceIDs(ctx, api.InstanceIDQuery{
				RuntimeStatus:     []api.OrchestrationStatus{api.RUNTIME_STATUS_COMPLETED},
				PageSize:          100,
				ContinuationToken: token,
			})
			if errors.Is(err, api.ErrFeatureNotSupported) {
				t.Log("DTS emulator limitation: ListInstanceIds is not implemented")
				return
			}
			require.NoError(t, err)
			for _, id := range result.InstanceIDs {
				listed[id] = struct{}{}
			}
			token = result.ContinuationToken
			if token == "" {
				break
			}
		}
		for _, id := range completedIDs {
			_, ok := listed[id]
			if !ok {
				t.Logf("DTS emulator limitation: ListInstanceIds omitted matching instance %s", id)
			}
		}
	})

	t.Run("restart", func(t *testing.T) {
		restartedID, err := managementClient.RestartInstance(
			ctx,
			completedIDs[0],
			api.WithRestartNewInstanceID(true),
		)
		if errors.Is(err, api.ErrFeatureNotSupported) {
			t.Log("DTS emulator limitation: RestartInstance is not implemented")
			return
		}
		require.NoError(t, err)
		restarted, err := managementClient.WaitForOrchestrationCompletion(ctx, restartedID, api.WithFetchPayloads(true))
		require.NoError(t, err)
		require.Equal(t, api.RUNTIME_STATUS_COMPLETED, restarted.RuntimeStatus)
		if restarted.SerializedOutput == "" {
			t.Log("DTS emulator limitation: restarted completion output is not returned")
		} else {
			require.Equal(t, `"value-0"`, restarted.SerializedOutput)
		}
		if restarted.Tags["group"] == "" {
			t.Log("DTS emulator limitation: restarted orchestration tags are not returned")
		} else {
			require.Equal(t, "0", restarted.Tags["group"])
		}
		require.NoError(t, managementClient.PurgeOrchestrationState(ctx, restartedID))
	})

	waitID := api.InstanceID(prefix + "-wait")
	_, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSAdvancedWait",
		api.WithInstanceID(waitID),
	)
	require.NoError(t, err)
	_, err = managementClient.WaitForOrchestrationStart(ctx, waitID)
	require.NoError(t, err)
	t.Run("skip-graceful-termination", func(t *testing.T) {
		unterminated, err := managementClient.SkipGracefulOrchestrationTerminations(ctx, []api.InstanceID{waitID}, "test")
		if errors.Is(err, api.ErrFeatureNotSupported) {
			t.Log("DTS emulator limitation: SkipGracefulOrchestrationTerminations is not implemented")
			require.NoError(t, managementClient.TerminateOrchestration(ctx, waitID))
			_, waitErr := managementClient.WaitForOrchestrationCompletion(ctx, waitID)
			require.NoError(t, waitErr)
			return
		}
		require.NoError(t, err)
		require.Empty(t, unterminated)
	})

	rewindID := api.InstanceID(prefix + "-rewind")
	_, err = managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSAdvancedRewind",
		api.WithInstanceID(rewindID),
	)
	require.NoError(t, err)
	failed, err := managementClient.WaitForOrchestrationCompletion(ctx, rewindID)
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_FAILED, failed.RuntimeStatus)
	failedExecutionID := failed.ExecutionID
	t.Run("rewind", func(t *testing.T) {
		err := managementClient.RewindInstance(ctx, rewindID, api.WithRewindReason("retry"))
		if errors.Is(err, api.ErrFeatureNotSupported) {
			t.Log("DTS emulator limitation: RewindInstance is not implemented")
			return
		}
		require.NoError(t, err)
		transitioned := false
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			current, fetchErr := managementClient.FetchOrchestrationMetadata(ctx, rewindID)
			if fetchErr == nil &&
				current.RuntimeStatus != api.RUNTIME_STATUS_FAILED &&
				current.ExecutionID != failedExecutionID {
				transitioned = true
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if !transitioned {
			t.Log("DTS emulator limitation: RewindInstance returns success without transitioning the failed instance")
			return
		}
		rewound, err := managementClient.WaitForOrchestrationCompletion(ctx, rewindID, api.WithFetchPayloads(true))
		require.NoError(t, err)
		require.Equal(t, api.RUNTIME_STATUS_COMPLETED, rewound.RuntimeStatus)
		require.EqualValues(t, 2, rewindAttempts.Load())
		if rewound.SerializedOutput == "" {
			t.Log("DTS emulator limitation: rewound completion output is not returned")
		} else {
			require.Equal(t, `"recovered"`, rewound.SerializedOutput)
		}
	})

	t.Run("filter-purge", func(t *testing.T) {
		filterStart := time.Now().UTC()
		filterID := api.InstanceID(prefix + "-filter-purge")
		_, err := managementClient.ScheduleNewOrchestration(
			ctx,
			"DTSAdvancedComplete",
			api.WithInstanceID(filterID),
			api.WithInput("purge"),
		)
		require.NoError(t, err)
		_, err = managementClient.WaitForOrchestrationCompletion(ctx, filterID)
		require.NoError(t, err)
		result, err := managementClient.PurgeInstances(ctx, api.PurgeInstancesRequest{
			Filter: &api.PurgeInstanceFilter{
				CreatedTimeFrom: filterStart,
				CreatedTimeTo:   time.Now().UTC().Add(time.Second),
				RuntimeStatus:   []api.OrchestrationStatus{api.RUNTIME_STATUS_COMPLETED},
			},
			PollInterval: 10 * time.Millisecond,
		})
		if errors.Is(err, api.ErrFeatureNotSupported) {
			t.Log("DTS emulator limitation: filter PurgeInstances is not implemented")
			require.NoError(t, managementClient.PurgeOrchestrationState(ctx, filterID))
			return
		}
		require.NoError(t, err)
		require.True(t, result.IsComplete)
		if result.DeletedInstanceCount == 0 {
			if _, fetchErr := managementClient.FetchOrchestrationMetadata(ctx, filterID); fetchErr == nil {
				t.Log("DTS emulator limitation: filter purge reports completion without deleting the matching instance")
				require.NoError(t, managementClient.PurgeOrchestrationState(ctx, filterID))
			}
		}
	})

	t.Run("batch-purge", func(t *testing.T) {
		result, err := managementClient.PurgeInstances(ctx, api.PurgeInstancesRequest{
			InstanceIDs: append(completedIDs, waitID, rewindID),
		})
		if errors.Is(err, api.ErrFeatureNotSupported) {
			t.Log("DTS emulator limitation: batch PurgeInstances is not implemented")
			for _, id := range append(completedIDs, waitID, rewindID) {
				purgeErr := managementClient.PurgeOrchestrationState(ctx, id)
				require.True(t, purgeErr == nil || errors.Is(purgeErr, api.ErrInstanceNotFound))
			}
			return
		}
		require.NoError(t, err)
		require.True(t, result.IsComplete)
	})
}

func uniqueInstanceID(prefix string) api.InstanceID {
	return api.InstanceID(prefix + "-" + uuid.NewString())
}

func TestDTSEmulatorScheduledFilteredLargePayloadWorker(t *testing.T) {
	store := payload.NewMemoryStore()
	options := emulatorOptions(t)
	options.LargePayloads = &api.LargePayloadOptions{
		Store:           store,
		Resolver:        store,
		ThresholdBytes:  16,
		MaxPayloadBytes: 1024 * 1024,
	}
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddActivityN("DTSLargePayloadEcho", func(ctx task.ActivityContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		return input + "-activity", nil
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSLargePayload", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		var output string
		if err := ctx.CallActivity("DTSLargePayloadEcho", task.WithActivityInput(input)).Await(&output); err != nil {
			return nil, err
		}
		return output, nil
	}))

	logger := api.DefaultLogger()
	managementClient, err := durabletaskscheduler.NewClient(context.Background(), options, logger)
	require.NoError(t, err)
	worker, err := durabletaskscheduler.NewWorker(
		options,
		registry,
		logger,
		durabletaskclient.WithScheduledTaskCapability(true),
		durabletaskclient.WithWorkItemFilters(&durabletaskclient.WorkItemFilters{
			Orchestrations: []durabletaskclient.WorkItemFilter{{Name: "DTSLargePayload"}},
			Activities:     []durabletaskclient.WorkItemFilter{{Name: "DTSLargePayloadEcho"}},
		}),
	)
	require.NoError(t, err)
	require.NoError(t, worker.Start(context.Background()))
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, worker.Shutdown(shutdownCtx))
		require.NoError(t, managementClient.Close())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	input := strings.Repeat("large-payload-", 128)
	id, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSLargePayload",
		api.WithInstanceID(uniqueInstanceID("go-large-payload")),
		api.WithInput(input),
		api.WithStartTime(time.Now().UTC().Add(250*time.Millisecond)),
		api.WithTags(map[string]string{"scenario": "large-payload"}),
	)
	require.NoError(t, err)
	metadata, err := managementClient.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, `"`+input+`-activity"`, metadata.SerializedOutput)
	require.Equal(t, "large-payload", metadata.Tags["scenario"])
	require.NoError(t, managementClient.PurgeOrchestrationState(ctx, id))
}

func TestDTSEmulatorSequenceMetadataAndPurge(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("DTSSequence", func(ctx *task.OrchestrationContext) (any, error) {
		var outputs []string
		for _, city := range []string{"Tokyo", "London", "Seattle"} {
			var output string
			if err := ctx.CallActivity("DTSSayHello", task.WithActivityInput(city)).Await(&output); err != nil {
				return nil, err
			}

			outputs = append(outputs, output)
		}
		return outputs, nil
	}))
	require.NoError(t, registry.AddActivityN("DTSSayHello", func(ctx task.ActivityContext) (any, error) {
		var city string
		if err := ctx.GetInput(&city); err != nil {
			return nil, err
		}
		return "Hello, " + city + "!", nil
	}))
	managementClient, _, _ := startEmulatorClientAndWorker(t, registry)

	instanceID := uniqueInstanceID("go-sequence")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	id, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSSequence",
		api.WithInstanceID(instanceID),
	)
	require.NoError(t, err)
	require.Equal(t, instanceID, id)

	metadata, err := managementClient.WaitForOrchestrationCompletion(ctx, id, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	require.Equal(t, "DTSSequence", metadata.Name)
	require.Equal(t, instanceID, metadata.InstanceID)
	require.False(t, metadata.CreatedAt.IsZero())
	require.False(t, metadata.LastUpdatedAt.IsZero())

	var output []string
	require.NoError(t, json.Unmarshal([]byte(metadata.SerializedOutput), &output))
	require.Equal(t, []string{"Hello, Tokyo!", "Hello, London!", "Hello, Seattle!"}, output)

	require.NoError(t, managementClient.PurgeOrchestrationState(ctx, id))
	_, err = managementClient.FetchOrchestrationMetadata(ctx, id)
	require.ErrorIs(t, err, api.ErrInstanceNotFound)
}

func TestDTSEmulatorDurableEntities(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddEntityN("counter", func(ctx *task.EntityContext) (any, error) {
		var value int
		if ctx.HasState() {
			if err := ctx.GetState(&value); err != nil {
				return nil, err
			}
		}
		switch ctx.Operation {
		case "add":
			var amount int
			if err := ctx.GetInput(&amount); err != nil {
				return nil, err
			}
			value += amount
			if err := ctx.SetState(value); err != nil {
				return nil, err
			}
		case "get":
		case "delete":
			ctx.DeleteState()
		default:
			return nil, fmt.Errorf("unknown operation %q", ctx.Operation)
		}
		return value, nil
	}))
	entityID := api.NewEntityID("counter", uuid.NewString())
	require.NoError(t, registry.AddOrchestratorN("DTSEntityCall", func(ctx *task.OrchestrationContext) (any, error) {
		var value int
		if err := ctx.CallEntity(entityID, "add", task.WithEntityInput(2)).Await(&value); err != nil {
			return nil, err
		}
		return value, nil
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSEntityLock", func(ctx *task.OrchestrationContext) (any, error) {
		unlock, err := ctx.LockEntities(entityID)
		if err != nil {
			return nil, err
		}
		defer unlock()
		var value int
		if err := ctx.CallEntity(entityID, "add", task.WithEntityInput(1)).Await(&value); err != nil {
			return nil, err
		}
		return value, nil
	}))
	managementClient, _, _ := startEmulatorClientAndWorker(t, registry)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	require.NoError(t, managementClient.SignalEntity(ctx, entityID, "add", api.WithSignalInput(5)))
	require.Eventually(t, func() bool {
		metadata, err := managementClient.GetEntity(ctx, entityID)
		return err == nil && metadata != nil && metadata.SerializedState == "5"
	}, 20*time.Second, 100*time.Millisecond)

	scheduledAt := time.Now().UTC().Add(750 * time.Millisecond)
	require.NoError(t, managementClient.SignalEntity(
		ctx,
		entityID,
		"add",
		api.WithSignalInput(5),
		api.WithSignalScheduledTime(scheduledAt),
	))
	require.Never(t, func() bool {
		metadata, err := managementClient.GetEntity(ctx, entityID)
		return err == nil && metadata != nil && metadata.SerializedState == "10"
	}, 300*time.Millisecond, 50*time.Millisecond)
	require.Eventually(t, func() bool {
		metadata, err := managementClient.GetEntity(ctx, entityID)
		return err == nil && metadata != nil && metadata.SerializedState == "10"
	}, 20*time.Second, 100*time.Millisecond)

	callID, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSEntityCall",
		api.WithInstanceID(uniqueInstanceID("go-entity-call")),
	)
	require.NoError(t, err)
	callResult, err := managementClient.WaitForOrchestrationCompletion(ctx, callID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, "12", callResult.SerializedOutput)

	lockID, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSEntityLock",
		api.WithInstanceID(uniqueInstanceID("go-entity-lock")),
	)
	require.NoError(t, err)
	lockResult, err := managementClient.WaitForOrchestrationCompletion(ctx, lockID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, "13", lockResult.SerializedOutput)

	query, err := managementClient.QueryEntities(ctx, api.EntityQuery{
		InstanceIDStartsWith: entityID.String(),
	})
	require.NoError(t, err)
	require.Len(t, query.Entities, 1)
	require.Equal(t, "13", query.Entities[0].SerializedState)
}

func TestDTSEmulatorEventsSuspendResumeAndTerminate(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("DTSEvents", func(ctx *task.OrchestrationContext) (any, error) {
		var values []int
		for range 2 {
			var value int
			if err := ctx.WaitForSingleEvent("value", time.Minute).Await(&value); err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		return values, nil
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSTerminate", func(ctx *task.OrchestrationContext) (any, error) {
		if err := ctx.CreateTimer(time.Minute).Await(nil); err != nil {
			return nil, err
		}
		return "unexpected", nil
	}))
	managementClient, _, _ := startEmulatorClientAndWorker(t, registry)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	eventID, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSEvents",
		api.WithInstanceID(uniqueInstanceID("go-events")),
	)
	require.NoError(t, err)
	_, err = managementClient.WaitForOrchestrationStart(ctx, eventID)
	require.NoError(t, err)
	require.NoError(t, managementClient.SuspendOrchestration(ctx, eventID, "test"))
	require.NoError(t, managementClient.RaiseEvent(ctx, eventID, "value", api.WithEventPayload(1)))
	require.NoError(t, managementClient.RaiseEvent(ctx, eventID, "value", api.WithEventPayload(2)))

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_, err = managementClient.WaitForOrchestrationCompletion(waitCtx, eventID)
	cancelWait()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	suspended, err := managementClient.FetchOrchestrationMetadata(ctx, eventID)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_SUSPENDED, suspended.RuntimeStatus)

	require.NoError(t, managementClient.ResumeOrchestration(ctx, eventID, "test"))
	completed, err := managementClient.WaitForOrchestrationCompletion(ctx, eventID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, `[1,2]`, completed.SerializedOutput)

	terminateID, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSTerminate",
		api.WithInstanceID(uniqueInstanceID("go-terminate")),
	)
	require.NoError(t, err)
	_, err = managementClient.WaitForOrchestrationStart(ctx, terminateID)
	require.NoError(t, err)
	require.NoError(t, managementClient.TerminateOrchestration(ctx, terminateID, api.WithOutput("terminated")))
	terminated, err := managementClient.WaitForOrchestrationCompletion(ctx, terminateID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED, terminated.RuntimeStatus)
	require.Equal(t, `"terminated"`, terminated.SerializedOutput)
}

func TestDTSEmulatorWorkerStopAndRestart(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("DTSRestart", func(ctx *task.OrchestrationContext) (any, error) {
		var result string
		if err := ctx.CallActivity("DTSRestartActivity").Await(&result); err != nil {
			return nil, err
		}
		return result, nil
	}))
	require.NoError(t, registry.AddActivityN("DTSRestartActivity", func(task.ActivityContext) (any, error) {
		return "restarted", nil
	}))

	options := emulatorOptions(t)
	logger := api.DefaultLogger()
	managementClient, err := durabletaskscheduler.NewClient(context.Background(), options, logger)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, managementClient.Close())
	}()

	newWorker := func() *durabletaskclient.TaskHubGrpcWorker {
		worker, workerErr := durabletaskscheduler.NewWorker(
			options,
			registry,
			logger,
			durabletaskclient.WithMaxConcurrentOrchestrationWorkItems(2),
			durabletaskclient.WithMaxConcurrentActivityWorkItems(2),
		)
		require.NoError(t, workerErr)
		require.NoError(t, worker.Start(context.Background()))
		return worker
	}

	worker := newWorker()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	require.NoError(t, worker.Shutdown(shutdownCtx))
	cancelShutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	instanceID, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSRestart",
		api.WithInstanceID(uniqueInstanceID("go-restart")),
	)
	require.NoError(t, err)

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_, err = managementClient.WaitForOrchestrationCompletion(waitCtx, instanceID)
	cancelWait()
	require.True(t, errors.Is(err, context.DeadlineExceeded), "orchestration unexpectedly completed without a worker: %v", err)

	worker = newWorker()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, worker.Shutdown(shutdownCtx))
	}()
	metadata, err := managementClient.WaitForOrchestrationCompletion(ctx, instanceID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, `"restarted"`, metadata.SerializedOutput)
}

func TestDTSEmulatorSelectEventCancelsTimer(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("DTSSelect", func(ctx *task.OrchestrationContext) (any, error) {
		var timerDelay time.Duration
		if err := ctx.GetInput(&timerDelay); err != nil {
			return nil, err
		}
		timerCtx, cancelTimer := ctx.WithCancel()
		timer := timerCtx.CreateTimer(timerDelay)
		events := task.NewEventChannel[string](ctx, "approval")
		selected := ""
		ctx.Select(
			task.OnTask(timer, func(task.Task) { selected = "timeout" }),
			task.OnEvent(events, func(value string) { selected = value }),
		)
		cancelTimer()
		return selected, nil
	}))
	managementClient, _, _ := startEmulatorClientAndWorker(t, registry)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	instanceID, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSSelect",
		api.WithInstanceID(uniqueInstanceID("go-select")),
		api.WithInput(time.Minute),
	)
	require.NoError(t, err)
	_, err = managementClient.WaitForOrchestrationStart(ctx, instanceID)
	require.NoError(t, err)
	require.NoError(t, managementClient.RaiseEvent(
		ctx,
		instanceID,
		"approval",
		api.WithEventPayload("approved"),
	))
	metadata, err := managementClient.WaitForOrchestrationCompletion(
		ctx,
		instanceID,
		api.WithFetchPayloads(true),
	)
	require.NoError(t, err)
	require.Equal(t, `"approved"`, metadata.SerializedOutput)

	timerID, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSSelect",
		api.WithInstanceID(uniqueInstanceID("go-select-timer")),
		api.WithInput(10*time.Millisecond),
	)
	require.NoError(t, err)
	timerMetadata, err := managementClient.WaitForOrchestrationCompletion(
		ctx,
		timerID,
		api.WithFetchPayloads(true),
	)
	require.NoError(t, err)
	require.Equal(t, `"timeout"`, timerMetadata.SerializedOutput)
}

func TestDTSEmulatorConcurrentCoroutineFanOut(t *testing.T) {
	const (
		instanceCount = 16
		fanOut        = 16
		expected      = fanOut * (fanOut - 1)
	)

	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("DTSConcurrentFanOut", func(ctx *task.OrchestrationContext) (any, error) {
		results := make([]int, fanOut)
		waitGroup := ctx.NewWaitGroup()
		waitGroup.Add(fanOut)
		for i := range fanOut {
			i := i
			ctx.Go(func(ctx *task.OrchestrationContext) {
				defer waitGroup.Done()
				if err := ctx.CallActivity(
					"DTSDouble",
					task.WithActivityInput(i),
				).Await(&results[i]); err != nil {
					panic(err)
				}
			})
		}
		waitGroup.Wait(ctx)
		total := 0
		for _, result := range results {
			total += result
		}
		return total, nil
	}))
	require.NoError(t, registry.AddActivityN("DTSDouble", func(ctx task.ActivityContext) (any, error) {
		var value int
		if err := ctx.GetInput(&value); err != nil {
			return nil, err
		}
		return value * 2, nil
	}))
	managementClient, _, _ := startEmulatorClientAndWorker(t, registry)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	instanceIDs := make([]api.InstanceID, instanceCount)
	for i := range instanceCount {
		instanceID, err := managementClient.ScheduleNewOrchestration(
			ctx,
			"DTSConcurrentFanOut",
			api.WithInstanceID(uniqueInstanceID("go-fanout")),
		)
		require.NoError(t, err)
		instanceIDs[i] = instanceID
	}

	errs := make(chan error, instanceCount)
	var waitGroup sync.WaitGroup
	waitGroup.Add(instanceCount)
	for _, instanceID := range instanceIDs {
		instanceID := instanceID
		go func() {
			defer waitGroup.Done()
			metadata, err := managementClient.WaitForOrchestrationCompletion(
				ctx,
				instanceID,
				api.WithFetchPayloads(true),
			)
			if err == nil && metadata.SerializedOutput != fmt.Sprintf("%d", expected) {
				err = fmt.Errorf(
					"%s output = %s, want %d",
					instanceID,
					metadata.SerializedOutput,
					expected,
				)
			}
			errs <- err
		}()
	}
	waitGroup.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func TestDTSEmulatorSendEventReplay(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("DTSEventReceiver", func(ctx *task.OrchestrationContext) (any, error) {
		return task.NewEventChannel[string](ctx, "ping").Receive(ctx), nil
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSEventSender", func(ctx *task.OrchestrationContext) (any, error) {
		var target api.InstanceID
		if err := ctx.GetInput(&target); err != nil {
			return nil, err
		}
		if err := ctx.SendEvent(target, "ping", "pong"); err != nil {
			return nil, err
		}
		if err := ctx.CreateTimer(10 * time.Millisecond).Await(nil); err != nil {
			return nil, err
		}
		return "sent", nil
	}))
	managementClient, _, _ := startEmulatorClientAndWorker(t, registry)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	receiverID, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSEventReceiver",
		api.WithInstanceID(uniqueInstanceID("go-event-receiver")),
	)
	require.NoError(t, err)
	_, err = managementClient.WaitForOrchestrationStart(ctx, receiverID)
	require.NoError(t, err)
	senderID, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSEventSender",
		api.WithInstanceID(uniqueInstanceID("go-event-sender")),
		api.WithInput(receiverID),
	)
	require.NoError(t, err)

	sender, err := managementClient.WaitForOrchestrationCompletion(ctx, senderID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, `"sent"`, sender.SerializedOutput)
	receiver, err := managementClient.WaitForOrchestrationCompletion(ctx, receiverID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, `"pong"`, receiver.SerializedOutput)
}

func TestDTSEmulatorActivityAndCompletionTags(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddActivityN("DTSTaggedActivity", func(task.ActivityContext) (any, error) {
		return "done", nil
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSTaggedOrchestration", func(ctx *task.OrchestrationContext) (any, error) {
		var result string
		err := ctx.CallActivity(
			"DTSTaggedActivity",
			task.WithActivityTags(map[string]string{
				"scope":    "activity",
				"activity": "true",
			}),
		).Await(&result)
		return result, err
	}))
	managementClient, _, _ := startEmulatorClientAndWorker(t, registry)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	instanceID, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSTaggedOrchestration",
		api.WithInstanceID(uniqueInstanceID("go-action-tags")),
		api.WithTags(map[string]string{
			"scope":  "orchestration",
			"parent": "true",
		}),
	)
	require.NoError(t, err)
	metadata, err := managementClient.WaitForOrchestrationCompletion(ctx, instanceID)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"scope":  "orchestration",
		"parent": "true",
	}, metadata.Tags)

	history, err := managementClient.GetOrchestrationHistory(
		ctx,
		instanceID,
		api.HistoryQuery{ExecutionID: metadata.ExecutionID},
	)
	require.NoError(t, err)
	var scheduled *api.HistoryTaskScheduledEvent
	for _, event := range history.Events {
		if event.TaskScheduled != nil {
			scheduled = event.TaskScheduled
			break
		}
	}
	require.NotNil(t, scheduled)
	require.Equal(t, "activity", scheduled.Tags["scope"])
	require.Equal(t, "true", scheduled.Tags["activity"])
	require.Equal(t, "true", scheduled.Tags["parent"])
}

func TestDTSEmulatorLongTimerSplitting(t *testing.T) {
	const (
		unit            = 2 * time.Second
		delay           = 7 * unit
		maximumInterval = 3 * unit
		expectedTimers  = 3
	)
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddOrchestratorN("DTSLongTimer", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.CreateTimer(delay).Await(nil)
	}))
	options := emulatorOptions(t)
	options.MaximumTimerInterval = maximumInterval
	logger := api.DefaultLogger()
	managementClient, err := durabletaskscheduler.NewClient(context.Background(), options, logger)
	require.NoError(t, err)
	worker, err := durabletaskscheduler.NewWorker(options, registry, logger)
	require.NoError(t, err)
	require.NoError(t, worker.Start(context.Background()))
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, worker.Shutdown(shutdownCtx))
		require.NoError(t, managementClient.Close())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instanceID, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSLongTimer",
		api.WithInstanceID(uniqueInstanceID("go-long-timer")),
	)
	require.NoError(t, err)
	metadata, err := managementClient.WaitForOrchestrationCompletion(ctx, instanceID)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)

	history, err := managementClient.GetOrchestrationHistory(
		ctx,
		instanceID,
		api.HistoryQuery{ExecutionID: metadata.ExecutionID},
	)
	require.NoError(t, err)
	timerCount := 0
	for _, event := range history.Events {
		if event.Type == api.HistoryEventTimerCreated {
			timerCount++
		}
	}
	require.Equal(t, expectedTimers, timerCount)
}

func TestDTSEmulatorFailsOversizedCompletionWithoutRetryLoop(t *testing.T) {
	const activityInputBytes = 2 * 1024 * 1024
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddActivityN("DTSOversizedActivity", func(task.ActivityContext) (any, error) {
		return nil, errors.New("oversized actions should never be dispatched")
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSOversizedFanout", func(ctx *task.OrchestrationContext) (any, error) {
		input := strings.Repeat("x", activityInputBytes)
		first := ctx.CallActivity("DTSOversizedActivity", task.WithActivityInput(input))
		second := ctx.CallActivity("DTSOversizedActivity", task.WithActivityInput(input))
		return nil, ctx.WhenAll(first, second)
	}))
	managementClient, _, _ := startEmulatorClientAndWorker(t, registry)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	instanceID, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSOversizedFanout",
		api.WithInstanceID(uniqueInstanceID("go-oversized-fanout")),
	)
	require.NoError(t, err)
	completed, err := managementClient.WaitForOrchestrationCompletion(ctx, instanceID)
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_FAILED, completed.RuntimeStatus)
	require.NotNil(t, completed.FailureDetails)
	require.Equal(t, api.ErrorTypeOrchestratorResponseTooLarge, completed.FailureDetails.ErrorType)
	require.True(t, completed.FailureDetails.NonRetriable())
	require.Contains(t, completed.FailureDetails.ErrorMessage, "large-payload externalization")
}

func TestDTSEmulatorCompletionRespectsConfiguredSendLimit(t *testing.T) {
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddActivityN("DTSSendLimitActivity", func(task.ActivityContext) (any, error) {
		return nil, errors.New("oversized action should never be dispatched")
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSSendLimit", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.CallActivity(
			"DTSSendLimitActivity",
			task.WithActivityInput(strings.Repeat("x", 96*1024)),
		).Await(nil)
	}))

	options := emulatorOptions(t)
	options.MaxSendMessageSize = 64 * 1024
	logger := api.DefaultLogger()
	managementClient, err := durabletaskscheduler.NewClient(context.Background(), options, logger)
	require.NoError(t, err)
	worker, err := durabletaskscheduler.NewWorker(options, registry, logger)
	require.NoError(t, err)
	require.NoError(t, worker.Start(context.Background()))
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, worker.Shutdown(shutdownCtx))
		require.NoError(t, managementClient.Close())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	instanceID, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSSendLimit",
		api.WithInstanceID(uniqueInstanceID("go-send-limit")),
	)
	require.NoError(t, err)
	completed, err := managementClient.WaitForOrchestrationCompletion(ctx, instanceID)
	require.NoError(t, err)
	require.Equal(t, api.RUNTIME_STATUS_FAILED, completed.RuntimeStatus)
	require.NotNil(t, completed.FailureDetails)
	require.Equal(t, api.ErrorTypeOrchestratorResponseTooLarge, completed.FailureDetails.ErrorType)
	require.Contains(t, completed.FailureDetails.ErrorMessage, "0.06 MiB worker completion bound")
}

func TestDTSEmulatorPartialEventConsumption(t *testing.T) {
	const eventCount = 64
	registry := task.NewTaskRegistry()
	var activityExecutions atomic.Int32
	require.NoError(t, registry.AddActivityN("DTSPartialActivity", func(task.ActivityContext) (any, error) {
		activityExecutions.Add(1)
		return nil, nil
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSPartialEvents", func(ctx *task.OrchestrationContext) (any, error) {
		if err := ctx.CallActivity("DTSPartialActivity").Await(nil); err != nil {
			return nil, err
		}
		results := make([]int, 0, eventCount)
		for range eventCount {
			var value int
			if err := ctx.WaitForSingleEvent("value", -1).Await(&value); err != nil {
				return nil, err
			}
			results = append(results, value)
		}
		return results, nil
	}))
	options := emulatorOptions(t)
	var (
		countsMu      sync.Mutex
		partialCounts []int32
	)
	options.UnaryInterceptors = append(options.UnaryInterceptors, func(
		ctx context.Context,
		method string,
		request any,
		reply any,
		connection *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		callOptions ...grpc.CallOption,
	) error {
		if method == protos.TaskHubSidecarService_CompleteOrchestratorTask_FullMethodName {
			if response, ok := request.(*protos.OrchestratorResponse); ok &&
				response.NumEventsProcessed != nil {
				countsMu.Lock()
				partialCounts = append(partialCounts, response.GetNumEventsProcessed().GetValue())
				countsMu.Unlock()
			}
		}
		return invoker(ctx, method, request, reply, connection, callOptions...)
	})
	logger := api.DefaultLogger()
	managementClient, err := durabletaskscheduler.NewClient(context.Background(), options, logger)
	require.NoError(t, err)
	workerOptions := []durabletaskclient.TaskHubGrpcWorkerOption{
		durabletaskclient.WithTaskExecutorOptions(
			task.WithOrchestrationOptions(task.OrchestrationOptions{MaxEventsPerTurn: 1}),
		),
	}
	worker, err := durabletaskscheduler.NewWorker(options, registry, logger, workerOptions...)
	require.NoError(t, err)
	require.NoError(t, worker.Start(context.Background()))
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = worker.Shutdown(shutdownCtx)
		require.NoError(t, managementClient.Close())
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	instanceID, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSPartialEvents",
		api.WithInstanceID(uniqueInstanceID("go-partial-events")),
	)
	require.NoError(t, err)
	_, err = managementClient.WaitForOrchestrationStart(ctx, instanceID)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return activityExecutions.Load() == 1
	}, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, managementClient.SuspendOrchestration(ctx, instanceID, "partial-budget-test"))
	require.Eventually(t, func() bool {
		metadata, fetchErr := managementClient.FetchOrchestrationMetadata(ctx, instanceID)
		return fetchErr == nil && metadata.RuntimeStatus == api.RUNTIME_STATUS_SUSPENDED
	}, 10*time.Second, 50*time.Millisecond)

	var raises sync.WaitGroup
	raiseErrors := make(chan error, eventCount)
	for index := range eventCount {
		raises.Add(1)
		go func(value int) {
			defer raises.Done()
			raiseErrors <- managementClient.RaiseEvent(
				ctx,
				instanceID,
				"value",
				api.WithEventPayload(value),
			)
		}(index)
	}
	raises.Wait()
	close(raiseErrors)
	for raiseErr := range raiseErrors {
		require.NoError(t, raiseErr)
	}
	require.NoError(t, managementClient.ResumeOrchestration(ctx, instanceID, "partial-budget-test"))

	completed, err := managementClient.WaitForOrchestrationCompletion(ctx, instanceID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	var results []int
	require.NoError(t, json.Unmarshal([]byte(completed.SerializedOutput), &results))
	slices.Sort(results)
	require.Equal(t, eventCount, len(results))
	for index, value := range results {
		require.Equal(t, index, value)
	}
	require.EqualValues(t, 1, activityExecutions.Load())
	countsMu.Lock()
	defer countsMu.Unlock()
	require.NotEmpty(t, partialCounts, "DTS did not deliver a batch large enough for partial consumption")
	for _, count := range partialCounts {
		require.EqualValues(t, 1, count)
	}
}

func TestDTSEmulatorSubOrchestrationRetryAndContinueAsNew(t *testing.T) {
	var attempts atomic.Int32
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddActivityN("DTSFlaky", func(task.ActivityContext) (any, error) {
		if attempts.Add(1) == 1 {
			return nil, errors.New("retry me")
		}
		return "recovered", nil
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSChild", func(ctx *task.OrchestrationContext) (any, error) {
		var result string
		err := ctx.CallActivity(
			"DTSFlaky",
			task.WithActivityRetryPolicy(&task.RetryPolicy{
				MaxAttempts:          2,
				InitialRetryInterval: 10 * time.Millisecond,
			}),
		).Await(&result)
		return result, err
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSParent", func(ctx *task.OrchestrationContext) (any, error) {
		var result string
		err := ctx.CallSubOrchestrator("DTSChild").Await(&result)
		return result, err
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSContinueAsNew", func(ctx *task.OrchestrationContext) (any, error) {
		var generation int
		if err := ctx.GetInput(&generation); err != nil {
			return nil, err
		}
		if generation < 2 {
			ctx.ContinueAsNew(generation + 1)
			return nil, nil
		}
		return generation, nil
	}))
	managementClient, _, _ := startEmulatorClientAndWorker(t, registry)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	parentID, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSParent",
		api.WithInstanceID(uniqueInstanceID("go-parent")),
	)
	require.NoError(t, err)
	parent, err := managementClient.WaitForOrchestrationCompletion(ctx, parentID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, `"recovered"`, parent.SerializedOutput)
	require.EqualValues(t, 2, attempts.Load())

	continueID, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSContinueAsNew",
		api.WithInstanceID(uniqueInstanceID("go-continue")),
		api.WithInput(0),
	)
	require.NoError(t, err)
	continued, err := managementClient.WaitForOrchestrationCompletion(ctx, continueID, api.WithFetchPayloads(true))
	require.NoError(t, err)
	require.Equal(t, "2", continued.SerializedOutput)
}

func TestDTSEmulatorScheduledTasksAndHistory(t *testing.T) {
	options := emulatorOptions(t)
	options.Versioning = &task.VersioningOptions{
		Version:         "1.0",
		DefaultVersion:  "1.0",
		MatchStrategy:   task.VersionMatchStrict,
		FailureStrategy: task.VersionFailureFail,
	}
	registry := task.NewTaskRegistry()
	var scheduledAttempts atomic.Int32
	targetName := "DTSScheduledTarget" + strings.ReplaceAll(uuid.NewString(), "-", "")[:8]
	require.NoError(t, registry.AddOrchestratorNVersion(targetName, "1.0", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		if scheduledAttempts.Add(1) == 1 {
			return nil, errors.New("retry scheduled target")
		}
		return input, nil
	}))
	require.NoError(t, durabletaskscheduler.RegisterScheduledTasksWithDefaultVersion(
		registry,
		options.Versioning.DefaultVersion,
	))
	logger := api.DefaultLogger()
	managementClient, err := durabletaskscheduler.NewClient(context.Background(), options, logger)
	require.NoError(t, err)
	worker, err := durabletaskscheduler.NewWorker(
		options,
		registry,
		logger,
		durabletaskscheduler.WithScheduledTasks(),
		durabletaskclient.WithAutoWorkItemFilters(),
	)
	require.NoError(t, err)
	require.NoError(t, worker.Start(context.Background()))
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, worker.Shutdown(shutdownCtx))
		require.NoError(t, managementClient.Close())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	createdFrom := time.Now().UTC().Add(-time.Second)
	scheduleID := "go-schedule-" + uuid.NewString()
	scheduleTag := "scheduled-task-" + uuid.NewString()
	handle, err := managementClient.ScheduledTasks().Create(ctx, durabletaskscheduler.ScheduleCreationOptions{
		ScheduleID:              scheduleID,
		OrchestrationName:       targetName,
		TypedOrchestrationInput: "scheduled",
		Interval:                time.Hour,
		StartImmediatelyIfLate:  true,
		Tags:                    map[string]string{"source": scheduleTag},
		ContextFields:           api.ContextFields{"tenant": "north"},
		RetryPolicy: &durabletaskscheduler.ScheduleRetryPolicy{
			MaxAttempts:          2,
			InitialRetryInterval: time.Second,
			BackoffCoefficient:   1,
			MaxRetryInterval:     time.Second,
			RetryTimeout:         10 * time.Second,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_ = handle.Delete(cleanupCtx)
	})
	description, err := handle.Describe(ctx)
	require.NoError(t, err)
	require.Equal(t, durabletaskscheduler.ScheduleStatusActive, description.Status)

	var target *api.OrchestrationMetadata
	var lastObserved []*api.OrchestrationMetadata
	deadline := time.Now().Add(30 * time.Second)
	for target == nil && time.Now().Before(deadline) {
		result, queryErr := managementClient.QueryInstances(ctx, api.OrchestrationQuery{
			CreatedTimeFrom:       createdFrom,
			PageSize:              100,
			FetchInputsAndOutputs: true,
		})
		if queryErr == nil {
			lastObserved = result.Orchestrations
			for _, orchestration := range result.Orchestrations {
				if orchestration.Name == targetName &&
					orchestration.RuntimeStatus == api.RUNTIME_STATUS_COMPLETED &&
					orchestration.Tags["source"] == scheduleTag {
					target = orchestration
					break
				}
			}
		}
		if target == nil {
			time.Sleep(200 * time.Millisecond)
		}
	}
	if target == nil {
		t.Fatalf("scheduled target did not complete; observed instances: %+v", lastObserved)
	}
	require.Equal(t, map[string]string{"source": scheduleTag}, target.Tags)
	require.EqualValues(t, 2, scheduledAttempts.Load())

	history, err := managementClient.GetOrchestrationHistory(
		ctx,
		target.InstanceID,
		api.HistoryQuery{ExecutionID: target.ExecutionID},
	)
	require.NoError(t, err)
	var foundContext bool
	for _, event := range history.Events {
		if event.ExecutionStarted != nil {
			require.Equal(t, api.ContextFields{"tenant": "north"}, event.ExecutionStarted.ContextFields)
			foundContext = true
		}
	}
	require.True(t, foundContext)

	page, err := managementClient.ScheduledTasks().List(ctx, durabletaskscheduler.ScheduleQuery{
		ScheduleIDPrefix: scheduleID,
		PageSize:         10,
	})
	require.NoError(t, err)
	require.Len(t, page.Schedules, 1)

	require.NoError(t, handle.Pause(ctx))
	description, err = handle.Describe(ctx)
	require.NoError(t, err)
	require.Equal(t, durabletaskscheduler.ScheduleStatusPaused, description.Status)
	require.NoError(t, handle.Resume(ctx))
	require.NoError(t, handle.Delete(ctx))
	require.Eventually(t, func() bool {
		schedule, getErr := managementClient.ScheduledTasks().Get(ctx, scheduleID)
		return getErr == nil && schedule == nil
	}, 10*time.Second, 200*time.Millisecond)
}

func TestDTSEmulatorAzuriteBlobV2RoundTrip(t *testing.T) {
	connectionString := os.Getenv("AZURITE_CONNECTION_STRING")
	if connectionString == "" {
		t.Skip("set AZURITE_CONNECTION_STRING to run DTS and Azurite interop")
	}
	store, err := payload.NewAzureBlobStore(payload.AzureBlobStoreOptions{
		ConnectionString:  connectionString,
		Container:         "dtgop1" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16],
		AllowInsecureHTTP: true,
	})
	require.NoError(t, err)
	options := emulatorOptions(t)
	options.LargePayloads = &api.LargePayloadOptions{Store: store, Resolver: store}
	registry := task.NewTaskRegistry()
	require.NoError(t, registry.AddActivityN("DTSBlobV2Echo", func(ctx task.ActivityContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		return input, nil
	}))
	require.NoError(t, registry.AddOrchestratorN("DTSBlobV2", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		first := ctx.CallActivity("DTSBlobV2Echo", task.WithActivityInput(input))
		second := ctx.CallActivity("DTSBlobV2Echo", task.WithActivityInput(input))
		if err := ctx.WhenAll(first, second); err != nil {
			return nil, err
		}
		var firstResult, secondResult string
		if err := first.Await(&firstResult); err != nil {
			return nil, err
		}
		if err := second.Await(&secondResult); err != nil {
			return nil, err
		}
		if firstResult != secondResult {
			return nil, errors.New("large-payload activity results did not match")
		}
		return firstResult, nil
	}))
	logger := api.DefaultLogger()
	managementClient, err := durabletaskscheduler.NewClient(context.Background(), options, logger)
	require.NoError(t, err)
	worker, err := durabletaskscheduler.NewWorker(
		options,
		registry,
		logger,
		durabletaskclient.WithAutoWorkItemFilters(),
	)
	require.NoError(t, err)
	require.NoError(t, worker.Start(context.Background()))
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, worker.Shutdown(shutdownCtx))
		require.NoError(t, managementClient.Close())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	// Two inline actions of this size would exceed the DTS completion limit.
	// Blob externalization must reduce the fan-out response before it is sent.
	input := strings.Repeat("blob-v2-large-payload-", 100_000)
	instanceID, err := managementClient.ScheduleNewOrchestration(
		ctx,
		"DTSBlobV2",
		api.WithInstanceID(uniqueInstanceID("go-blob-v2")),
		api.WithInput(input),
	)
	require.NoError(t, err)
	completed, err := managementClient.WaitForOrchestrationCompletion(ctx, instanceID)
	require.NoError(t, err)
	var output string
	require.NoError(t, completed.ReadOutput(&output))
	require.Equal(t, input, output)
	history, err := managementClient.GetOrchestrationHistory(ctx, instanceID, api.HistoryQuery{})
	require.NoError(t, err)
	require.NotEmpty(t, history.Events)
}
