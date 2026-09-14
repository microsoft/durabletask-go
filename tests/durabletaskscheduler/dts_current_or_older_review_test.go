package durabletaskscheduler_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestDTSCurrentOrOlderFiltersDoNotClaimNewerWork(t *testing.T) {
	options := emulatorOptions(t)
	mixed := "DTSReviewOlder-" + uuid.NewString()
	legacy := mixed + "-legacy"
	oldRegistry := task.NewTaskRegistry()
	for _, version := range []string{"", "1.0", "2.0", "10.0"} {
		require.NoError(t, oldRegistry.AddOrchestratorNVersion(mixed, version, func(*task.OrchestrationContext) (any, error) {
			return "old:" + version, nil
		}))
	}
	require.NoError(t, oldRegistry.AddOrchestratorN(legacy, func(*task.OrchestrationContext) (any, error) {
		return "legacy", nil
	}))
	var abandoned atomic.Int32
	oldOptions := *options
	oldOptions.Versioning = &task.VersioningOptions{
		Version: "2.0", DefaultVersion: "1.0", MatchStrategy: task.VersionMatchCurrentOrOlder,
	}
	oldOptions.UnaryInterceptors = []grpc.UnaryClientInterceptor{
		func(ctx context.Context, method string, request, reply any, connection *grpc.ClientConn, invoke grpc.UnaryInvoker, callOptions ...grpc.CallOption) error {
			if method == protos.TaskHubSidecarService_AbandonTaskOrchestratorWorkItem_FullMethodName {
				abandoned.Add(1)
			}
			return invoke(ctx, method, request, reply, connection, callOptions...)
		},
	}
	client, _ := startEmulatorWithOptions(t, &oldOptions, oldRegistry, durabletaskclient.WithAutoWorkItemFilters())
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	run := func(name, version, expected string) {
		id := uniqueInstanceID("go-current-or-older")
		t.Cleanup(func() { cleanupRewindInstances(t, client, []api.InstanceID{id}) })
		_, err := client.ScheduleNewOrchestration(ctx, name, api.WithInstanceID(id), api.WithVersion(version))
		require.NoError(t, err)
		result, err := client.WaitForOrchestrationCompletion(ctx, id)
		require.NoError(t, err)
		var output string
		require.NoError(t, result.ReadOutput(&output))
		require.Equal(t, expected, output)
	}
	for _, version := range []string{"", "1.0", "2.0"} {
		run(mixed, version, "old:"+version)
	}
	for _, version := range []string{"", "1.0", "2.0"} {
		run(legacy, version, "legacy")
	}
	defaultID := uniqueInstanceID("go-default-older")
	t.Cleanup(func() { cleanupRewindInstances(t, client, []api.InstanceID{defaultID}) })
	_, err := client.ScheduleNewOrchestration(ctx, legacy, api.WithInstanceID(defaultID))
	require.NoError(t, err)
	defaulted, err := client.WaitForOrchestrationCompletion(ctx, defaultID)
	require.NoError(t, err)
	require.Equal(t, "1.0", defaulted.Version)
	var defaultOutput string
	require.NoError(t, defaulted.ReadOutput(&defaultOutput))
	require.Equal(t, "legacy", defaultOutput)
	var pending []api.InstanceID
	for _, name := range []string{mixed, legacy} {
		id := uniqueInstanceID("go-newer-worker")
		t.Cleanup(func() { cleanupRewindInstances(t, client, []api.InstanceID{id}) })
		_, err := client.ScheduleNewOrchestration(ctx, name, api.WithInstanceID(id), api.WithVersion("10.0"))
		require.NoError(t, err)
		pending = append(pending, id)
	}
	require.Never(t, func() bool { return abandoned.Load() != 0 }, 500*time.Millisecond, 10*time.Millisecond)

	newOptions := *options
	newOptions.Versioning = &task.VersioningOptions{Version: "10.0", MatchStrategy: task.VersionMatchStrict}
	newRegistry := task.NewTaskRegistry()
	for _, name := range []string{mixed, legacy} {
		require.NoError(t, newRegistry.AddOrchestratorNVersion(name, "10.0", func(*task.OrchestrationContext) (any, error) {
			return "new:10.0", nil
		}))
	}
	startEmulatorWithOptions(t, &newOptions, newRegistry, durabletaskclient.WithAutoWorkItemFilters())
	for _, id := range pending {
		result, err := client.WaitForOrchestrationCompletion(ctx, id)
		require.NoError(t, err)
		var output string
		require.NoError(t, result.ReadOutput(&output))
		require.Equal(t, "new:10.0", output)
	}
	require.Zero(t, abandoned.Load())
}
