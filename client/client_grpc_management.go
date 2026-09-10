package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/historyconv"
	"github.com/microsoft/durabletask-go/internal/largepayload"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/internal/tagcodec"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func (c *TaskHubGrpcClient) GetOrchestrationHistory(
	ctx context.Context,
	id api.InstanceID,
	query api.HistoryQuery,
) (*api.OrchestrationHistory, error) {
	return historyconv.Collect(id, query, func(handler api.HistoryEventHandler) error {
		return c.StreamOrchestrationHistory(ctx, id, query, handler)
	})
}

func (c *TaskHubGrpcClient) StreamOrchestrationHistory(
	ctx context.Context,
	id api.InstanceID,
	query api.HistoryQuery,
	handler api.HistoryEventHandler,
) error {
	normalized, err := historyconv.NormalizeStreamRequest(id, query, handler)
	if err != nil {
		return err
	}
	request := &protos.StreamInstanceHistoryRequest{
		InstanceId:            string(id),
		ForWorkItemProcessing: false,
	}
	if normalized.ExecutionID != "" {
		request.ExecutionId = wrapperspb.String(normalized.ExecutionID)
	}

	streamContext, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := c.client.StreamInstanceHistory(streamContext, request)
	if err != nil {
		return clientRPCError(ctx, "failed to stream orchestration history", err)
	}

	converter := historyconv.New(c.converter)
	eventCount := 0
	for {
		chunk, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return nil
		}
		if recvErr != nil {
			return clientRPCError(ctx, "failed to stream orchestration history", recvErr)
		}
		if chunk == nil {
			return errors.New("failed to stream orchestration history: received a nil history chunk")
		}
		for _, event := range chunk.GetEvents() {
			if err := largepayload.TransformHistoryEvent(streamContext, c.largePayloads, event, false); err != nil {
				return fmt.Errorf("failed to hydrate orchestration history event %d: %w", eventCount, err)
			}
			converted, err := converter.Convert(event)
			if err != nil {
				return fmt.Errorf("failed to convert orchestration history event %d: %w", eventCount, err)
			}
			if err := handler(converted); err != nil {
				return err
			}
			eventCount++
		}
	}
}

func (c *TaskHubGrpcClient) QueryInstances(ctx context.Context, query api.OrchestrationQuery) (*api.OrchestrationQueryResult, error) {
	pageSize, err := api.NormalizeInstanceQueryPageSize(query.PageSize)
	if err != nil {
		return nil, err
	}
	if err := api.ValidateTimeRange(query.CreatedTimeFrom, query.CreatedTimeTo); err != nil {
		return nil, fmt.Errorf("invalid orchestration query: %w", err)
	}

	result := &api.OrchestrationQueryResult{
		Orchestrations: make([]*api.OrchestrationMetadata, 0, pageSize),
	}
	continuationToken := query.ContinuationToken
	scannedPages := 0
	for len(result.Orchestrations) < pageSize {
		remaining := pageSize - len(result.Orchestrations)
		wireQuery := &protos.InstanceQuery{
			RuntimeStatus:         slices.Clone(query.RuntimeStatus),
			MaxInstanceCount:      int32(remaining),
			InstanceIdPrefix:      stringValue(query.InstanceIDPrefix),
			ContinuationToken:     stringValue(continuationToken),
			FetchInputsAndOutputs: query.FetchInputsAndOutputs,
		}
		if !query.CreatedTimeFrom.IsZero() {
			wireQuery.CreatedTimeFrom = timestamppb.New(query.CreatedTimeFrom)
		}
		if !query.CreatedTimeTo.IsZero() {
			wireQuery.CreatedTimeTo = timestamppb.New(query.CreatedTimeTo)
		}
		if len(query.TaskHubNames) > 0 {
			wireQuery.TaskHubNames = make([]*wrapperspb.StringValue, 0, len(query.TaskHubNames))
			for _, taskHubName := range query.TaskHubNames {
				wireQuery.TaskHubNames = append(wireQuery.TaskHubNames, wrapperspb.String(taskHubName))
			}
		}

		resp, err := c.client.QueryInstances(ctx, &protos.QueryInstancesRequest{Query: wireQuery})
		if err != nil {
			return nil, clientRPCError(ctx, "failed to query orchestration instances", err)
		}
		if resp == nil {
			return nil, errors.New("query service returned a nil response")
		}
		scannedPages++

		for _, state := range resp.GetOrchestrationState() {
			if state == nil {
				return nil, errors.New("orchestration state is nil")
			}
			if !matchesTags(tagcodec.DecodeUserTagsOrPlain(state.Tags), query.Tags) {
				continue
			}
			if err := largepayload.TransformOrchestrationState(ctx, c.largePayloads, state); err != nil {
				return nil, fmt.Errorf("failed to hydrate orchestration query result: %w", err)
			}
			metadata, err := orchestrationMetadataFromState(state, c.converter)
			if err != nil {
				return nil, err
			}
			result.Orchestrations = append(result.Orchestrations, metadata)
		}

		nextToken := resp.GetContinuationToken().GetValue()
		if nextToken == "" {
			return result, nil
		}
		if nextToken == continuationToken {
			return nil, errors.New("query service returned a non-advancing continuation token")
		}
		continuationToken = nextToken
		if len(query.Tags) > 0 && scannedPages >= api.MaxRemoteTagFilterScanPages {
			result.ContinuationToken = continuationToken
			return result, nil
		}
	}
	result.ContinuationToken = continuationToken
	return result, nil
}

func (c *TaskHubGrpcClient) ListInstanceIDs(ctx context.Context, query api.InstanceIDQuery) (*api.InstanceIDQueryResult, error) {
	pageSize, err := api.NormalizeInstanceQueryPageSize(query.PageSize)
	if err != nil {
		return nil, err
	}
	if err := api.ValidateTimeRange(query.CompletedTimeFrom, query.CompletedTimeTo); err != nil {
		return nil, fmt.Errorf("invalid instance ID query: %w", err)
	}
	req := &protos.ListInstanceIdsRequest{
		RuntimeStatus:   slices.Clone(query.RuntimeStatus),
		PageSize:        int32(pageSize),
		LastInstanceKey: stringValue(query.ContinuationToken),
	}
	if !query.CompletedTimeFrom.IsZero() {
		req.CompletedTimeFrom = timestamppb.New(query.CompletedTimeFrom)
	}
	if !query.CompletedTimeTo.IsZero() {
		req.CompletedTimeTo = timestamppb.New(query.CompletedTimeTo)
	}

	resp, err := c.client.ListInstanceIds(ctx, req)
	if err != nil {
		return nil, clientRPCError(ctx, "failed to list orchestration instance IDs", err)
	}
	result := &api.InstanceIDQueryResult{
		InstanceIDs:       make([]api.InstanceID, 0, len(resp.GetInstanceIds())),
		ContinuationToken: resp.GetLastInstanceKey().GetValue(),
	}
	for _, id := range resp.GetInstanceIds() {
		result.InstanceIDs = append(result.InstanceIDs, api.InstanceID(id))
	}
	return result, nil
}

func (c *TaskHubGrpcClient) RestartInstance(ctx context.Context, id api.InstanceID, opts ...api.RestartOptions) (api.InstanceID, error) {
	req := &protos.RestartInstanceRequest{InstanceId: string(id)}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return api.EmptyInstanceID, fmt.Errorf("failed to configure restart request: %w", api.WrapInvalidArgument(err))
		}
	}
	resp, err := c.client.RestartInstance(ctx, req)
	if err != nil {
		return api.EmptyInstanceID, clientRPCError(ctx, "failed to restart orchestration instance", err)
	}
	return api.InstanceID(resp.GetInstanceId()), nil
}

func (c *TaskHubGrpcClient) PurgeInstances(ctx context.Context, request api.PurgeInstancesRequest) (*api.PurgeInstancesResult, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	if request.Filter != nil && request.Filter.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, request.Filter.Timeout)
		defer cancel()
	}
	pollInterval := request.PollInterval
	if pollInterval <= 0 {
		pollInterval = api.DefaultPurgePollInterval
	}
	result := &api.PurgeInstancesResult{IsComplete: true}
	if request.Filter != nil {
		req, err := makePurgeFilterRequest(request)
		if err != nil {
			return nil, err
		}
		return c.pollPurgeInstances(ctx, req, pollInterval)
	}

	for start := 0; start < len(request.InstanceIDs); start += api.MaxInstanceBatchSize {
		end := min(start+api.MaxInstanceBatchSize, len(request.InstanceIDs))
		ids := request.InstanceIDs[start:end]
		instanceIDs := make([]string, len(ids))
		for i, id := range ids {
			if id == api.EmptyInstanceID {
				return nil, api.WrapInvalidArgument(errors.New("purge instance ID cannot be empty"))
			}
			instanceIDs[i] = string(id)
		}
		req := &protos.PurgeInstancesRequest{
			Request: &protos.PurgeInstancesRequest_InstanceBatch{
				InstanceBatch: &protos.InstanceBatch{InstanceIds: instanceIDs},
			},
			Recursive:       request.Recursive,
			IsOrchestration: true,
		}
		batchResult, err := c.pollPurgeInstances(ctx, req, pollInterval)
		if batchResult != nil {
			result.DeletedInstanceCount += batchResult.DeletedInstanceCount
			result.IsComplete = result.IsComplete && batchResult.IsComplete
		}
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

func (c *TaskHubGrpcClient) pollPurgeInstances(ctx context.Context, req *protos.PurgeInstancesRequest, pollInterval time.Duration) (*api.PurgeInstancesResult, error) {
	result := &api.PurgeInstancesResult{}
	for {
		resp, err := c.client.PurgeInstances(ctx, req)
		if err != nil {
			return result, clientRPCError(ctx, "failed to purge orchestration instances", err)
		}
		result.DeletedInstanceCount += int(resp.GetDeletedInstanceCount())
		if resp.GetIsComplete() == nil || resp.GetIsComplete().GetValue() {
			result.IsComplete = true
			return result, nil
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return result, ctx.Err()
		case <-timer.C:
		}
	}
}

func makePurgeFilterRequest(request api.PurgeInstancesRequest) (*protos.PurgeInstancesRequest, error) {
	filter := request.Filter
	wireFilter := &protos.PurgeInstanceFilter{
		RuntimeStatus: slices.Clone(filter.RuntimeStatus),
	}
	if !filter.CreatedTimeFrom.IsZero() {
		wireFilter.CreatedTimeFrom = timestamppb.New(filter.CreatedTimeFrom)
	}
	if !filter.CreatedTimeTo.IsZero() {
		wireFilter.CreatedTimeTo = timestamppb.New(filter.CreatedTimeTo)
	}
	if filter.Timeout > 0 {
		wireFilter.Timeout = durationpb.New(filter.Timeout)
	}
	return &protos.PurgeInstancesRequest{
		Request:         &protos.PurgeInstancesRequest_PurgeInstanceFilter{PurgeInstanceFilter: wireFilter},
		Recursive:       request.Recursive,
		IsOrchestration: true,
	}, nil
}

func matchesTags(actual, expected map[string]string) bool {
	for key, value := range expected {
		actualValue, exists := actual[key]
		if !exists || actualValue != value {
			return false
		}
	}
	return true
}

func stringValue(value string) *wrapperspb.StringValue {
	if value == "" {
		return nil
	}
	return wrapperspb.String(value)
}
