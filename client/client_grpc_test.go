package client

import (
	"context"
	"fmt"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type fakeClientConn struct {
	invoke func(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error
}

func (f fakeClientConn) Invoke(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {
	return f.invoke(ctx, method, args, reply, opts...)
}

func (fakeClientConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, fmt.Errorf("unexpected stream call")
}

func Test_TaskHubGrpcClient_QueryEntities_ErrorsOnInvalidEntityID(t *testing.T) {
	conn := fakeClientConn{
		invoke: func(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {
			if method != protos.TaskHubSidecarService_QueryEntities_FullMethodName {
				return fmt.Errorf("unexpected method: %s", method)
			}

			resp := reply.(*protos.QueryEntitiesResponse)
			resp.Entities = []*protos.EntityMetadata{
				{InstanceId: "not-an-entity-id"},
			}
			return nil
		},
	}

	client := NewTaskHubGrpcClient(conn, backend.DefaultLogger())

	_, err := client.QueryEntities(context.Background(), api.EntityQuery{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `failed to parse entity ID "not-an-entity-id"`)
}
