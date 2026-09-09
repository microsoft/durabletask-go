package client

import (
	"context"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type entityClientTestService struct {
	protos.TaskHubSidecarServiceClient
	getRequest       *protos.GetEntityRequest
	getResponse      *protos.GetEntityResponse
	queryRequest     *protos.QueryEntitiesRequest
	cleanupRequests  []*protos.CleanEntityStorageRequest
	cleanupResponses []*protos.CleanEntityStorageResponse
}

func (service *entityClientTestService) GetEntity(
	_ context.Context,
	request *protos.GetEntityRequest,
	_ ...grpc.CallOption,
) (*protos.GetEntityResponse, error) {
	service.getRequest = request
	if service.getResponse == nil {
		return &protos.GetEntityResponse{}, nil
	}
	return service.getResponse, nil
}

func (service *entityClientTestService) QueryEntities(
	_ context.Context,
	request *protos.QueryEntitiesRequest,
	_ ...grpc.CallOption,
) (*protos.QueryEntitiesResponse, error) {
	service.queryRequest = request
	return &protos.QueryEntitiesResponse{}, nil
}

func (service *entityClientTestService) CleanEntityStorage(
	_ context.Context,
	request *protos.CleanEntityStorageRequest,
	_ ...grpc.CallOption,
) (*protos.CleanEntityStorageResponse, error) {
	service.cleanupRequests = append(service.cleanupRequests, request)
	response := service.cleanupResponses[0]
	service.cleanupResponses = service.cleanupResponses[1:]
	return response, nil
}

func Test_EntityClient_GetEntityDefaultsAndMissing(t *testing.T) {
	service := new(entityClientTestService)
	client := &TaskHubGrpcClient{client: service, converter: api.DefaultDataConverter()}

	metadata, err := client.GetEntity(context.Background(), api.NewEntityID("counter", "key"))
	require.NoError(t, err)
	require.Nil(t, metadata)
	require.True(t, service.getRequest.IncludeState)

	service.getResponse = &protos.GetEntityResponse{
		Exists: true,
		Entity: &protos.EntityMetadata{
			InstanceId:      "@counter@key",
			SerializedState: wrapperspb.String("42"),
		},
	}
	metadata, err = client.GetEntity(context.Background(), api.NewEntityID("counter", "key"))
	require.NoError(t, err)
	require.True(t, metadata.StateIncluded)
	require.True(t, metadata.HasState)
	var state int
	require.NoError(t, metadata.ReadState(&state))
	require.Equal(t, 42, state)

	metadata, err = client.GetEntity(
		context.Background(),
		api.NewEntityID("counter", "key"),
		api.GetEntityOptions{ExcludeState: true},
	)
	require.NoError(t, err)
	require.False(t, service.getRequest.IncludeState)
	require.False(t, metadata.StateIncluded)
	require.ErrorIs(t, metadata.ReadState(&state), api.ErrEntityStateNotIncluded)
}

func Test_EntityClient_QueryPrefixNormalizationAndStateDefault(t *testing.T) {
	service := new(entityClientTestService)
	client := &TaskHubGrpcClient{client: service, converter: api.DefaultDataConverter()}
	tests := map[string]string{
		"Counter":      "@counter",
		"@Counter":     "@counter",
		"Counter@Key":  "@counter@Key",
		"@Counter@Key": "@counter@Key",
	}
	for input, expected := range tests {
		t.Run(input, func(t *testing.T) {
			_, err := client.QueryEntities(context.Background(), api.EntityQuery{
				InstanceIDStartsWith: input,
			})
			require.NoError(t, err)
			require.Equal(t, expected, service.queryRequest.Query.InstanceIdStartsWith.GetValue())
			require.True(t, service.queryRequest.Query.IncludeState)
		})
	}

	_, err := client.QueryEntities(context.Background(), api.EntityQuery{ExcludeState: true})
	require.NoError(t, err)
	require.False(t, service.queryRequest.Query.IncludeState)
}

func Test_EntityClient_QueryValidation(t *testing.T) {
	client := &TaskHubGrpcClient{client: new(entityClientTestService)}
	_, err := client.QueryEntities(context.Background(), api.EntityQuery{PageSize: -1})
	require.ErrorContains(t, err, "page size")

	now := time.Now().UTC()
	_, err = client.QueryEntities(context.Background(), api.EntityQuery{
		LastModifiedFrom: now,
		LastModifiedTo:   now.Add(-time.Second),
	})
	require.ErrorContains(t, err, "start time must not be after end time")
}

func Test_EntityClient_CleanupDefaultsAndPagination(t *testing.T) {
	service := &entityClientTestService{
		cleanupResponses: []*protos.CleanEntityStorageResponse{
			{
				EmptyEntitiesRemoved:  2,
				OrphanedLocksReleased: 3,
				ContinuationToken:     wrapperspb.String("next"),
			},
			{
				EmptyEntitiesRemoved:  5,
				OrphanedLocksReleased: 7,
			},
		},
	}
	client := &TaskHubGrpcClient{client: service}
	result, err := client.CleanEntityStorage(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 7, result.EmptyEntitiesRemoved)
	require.EqualValues(t, 10, result.OrphanedLocksReleased)
	require.Empty(t, result.ContinuationToken)
	require.Len(t, service.cleanupRequests, 2)
	require.True(t, service.cleanupRequests[0].RemoveEmptyEntities)
	require.True(t, service.cleanupRequests[0].ReleaseOrphanedLocks)
	require.Equal(t, "next", service.cleanupRequests[1].ContinuationToken.GetValue())
}

func Test_EntityClient_CleanupSinglePageAndRepeatedToken(t *testing.T) {
	singlePageService := &entityClientTestService{
		cleanupResponses: []*protos.CleanEntityStorageResponse{{
			ContinuationToken: wrapperspb.String("next"),
		}},
	}
	client := &TaskHubGrpcClient{client: singlePageService}
	result, err := client.CleanEntityStorage(
		context.Background(),
		api.CleanEntityStorageOptions{SinglePage: true},
	)
	require.NoError(t, err)
	require.Equal(t, "next", result.ContinuationToken)
	require.Len(t, singlePageService.cleanupRequests, 1)

	repeatedService := &entityClientTestService{
		cleanupResponses: []*protos.CleanEntityStorageResponse{{
			ContinuationToken: wrapperspb.String("same"),
		}},
	}
	client = &TaskHubGrpcClient{client: repeatedService}
	_, err = client.CleanEntityStorage(
		context.Background(),
		api.CleanEntityStorageOptions{ContinuationToken: "same"},
	)
	require.ErrorContains(t, err, "repeated continuation token")
}
