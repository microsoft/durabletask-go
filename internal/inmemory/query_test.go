package inmemory

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/microsoft/durabletask-go/internal/protos"
)

func TestQueryInstancesContinuesAfterLastResultChanges(t *testing.T) {
	for _, change := range []string{"purge", "restart"} {
		t.Run(change, func(t *testing.T) {
			s, createdAt := queryTestServer()
			defer s.Close()

			first, err := s.QueryInstances(t.Context(), &protos.QueryInstancesRequest{
				Query: &protos.InstanceQuery{MaxInstanceCount: 1},
			})
			require.NoError(t, err)
			require.Len(t, first.GetOrchestrationState(), 1)
			require.Equal(t, "a", first.GetOrchestrationState()[0].GetInstanceId())

			switch change {
			case "purge":
				_, err = s.PurgeInstances(t.Context(), &protos.PurgeInstancesRequest{
					Request: &protos.PurgeInstancesRequest_InstanceId{InstanceId: "a"},
				})
				require.NoError(t, err)
			case "restart":
				s.mu.Lock()
				s.instances["a"].createdAt = createdAt.Add(4 * time.Second)
				s.mu.Unlock()
			}

			second, err := s.QueryInstances(t.Context(), &protos.QueryInstancesRequest{
				Query: &protos.InstanceQuery{
					MaxInstanceCount:  1,
					ContinuationToken: first.GetContinuationToken(),
				},
			})
			require.NoError(t, err)
			require.Len(t, second.GetOrchestrationState(), 1)
			require.Equal(t, "b", second.GetOrchestrationState()[0].GetInstanceId())
		})
	}
}

func TestListInstanceIDsContinuesAfterLastResultChanges(t *testing.T) {
	for _, change := range []string{"purge", "restart"} {
		t.Run(change, func(t *testing.T) {
			s, createdAt := queryTestServer()
			defer s.Close()

			first, err := s.ListInstanceIds(t.Context(), &protos.ListInstanceIdsRequest{PageSize: 1})
			require.NoError(t, err)
			require.Equal(t, []string{"a"}, first.GetInstanceIds())

			switch change {
			case "purge":
				_, err = s.PurgeInstances(t.Context(), &protos.PurgeInstancesRequest{
					Request: &protos.PurgeInstancesRequest_InstanceId{InstanceId: "a"},
				})
				require.NoError(t, err)
			case "restart":
				s.mu.Lock()
				s.instances["a"].completedAt = createdAt.Add(4 * time.Second)
				s.mu.Unlock()
			}

			second, err := s.ListInstanceIds(t.Context(), &protos.ListInstanceIdsRequest{
				PageSize:        1,
				LastInstanceKey: first.GetLastInstanceKey(),
			})
			require.NoError(t, err)
			require.Equal(t, []string{"b"}, second.GetInstanceIds())
		})
	}
}

func queryTestServer() (*Server, time.Time) {
	s := NewServer()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i, id := range []string{"a", "b", "c"} {
		when := base.Add(time.Duration(i) * time.Second)
		s.instances[id] = &instance{
			id:          id,
			status:      protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
			createdAt:   when,
			completedAt: when,
		}
	}
	return s, base
}
