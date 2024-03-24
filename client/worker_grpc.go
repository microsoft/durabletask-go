package client

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func (c *TaskHubGrpcClient) StartWorkItemListener(ctx context.Context, r *task.TaskRegistry) error {
	executor := task.NewTaskExecutor(r)

	if _, err := c.client.Hello(ctx, &emptypb.Empty{}); err != nil {
		return fmt.Errorf("failed to connect to task hub service: %w", err)
	}

	req := protos.GetWorkItemsRequest{}
	stream, err := c.client.GetWorkItems(ctx, &req)
	if err != nil {
		return fmt.Errorf("failed to get work item stream: %w", err)
	}

	go func() {
		c.logger.Info("starting background processor")
		for {
			// TODO: Manage concurrency
			workItem, err := stream.Recv()
			if err == io.EOF || stream.Context().Err() != nil {
				// shutdown
				break
			} else if err != nil {
				c.logger.Warnf("failed to establish work item stream: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}

			if orchReq := workItem.GetOrchestratorRequest(); orchReq != nil {
				go c.processOrchestrationWorkItem(stream.Context(), executor, orchReq)
			} else if actReq := workItem.GetActivityRequest(); actReq != nil {
				go c.processActivityWorkItem(stream.Context(), executor, actReq)
			} else {
				c.logger.Warnf("received unknown work item type: %v", workItem)
			}
		}
	}()
	return nil
}

func (c *TaskHubGrpcClient) processOrchestrationWorkItem(
	ctx context.Context,
	executor backend.Executor,
	workItem *protos.OrchestratorRequest,
) {
	results, err := executor.ExecuteOrchestrator(ctx, api.InstanceID(workItem.InstanceId), workItem.PastEvents, workItem.NewEvents)

	resp := protos.OrchestratorResponse{InstanceId: workItem.InstanceId}
	if err != nil {
		// NOTE: At the time of writing, there's no known case where this error is returned.
		//       We add error handling here anyways, just in case.
		failureAction := helpers.NewCompleteOrchestrationAction(
			-1,
			protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
			wrapperspb.String("An internal error occured while executing the orchestration."),
			nil,
			&protos.TaskFailureDetails{
				ErrorType:    fmt.Sprintf("%T", err),
				ErrorMessage: err.Error(),
			})
		resp.Actions = []*protos.OrchestratorAction{failureAction}
	} else {
		resp.Actions = results.Response.Actions
		resp.CustomStatus = results.Response.GetCustomStatus()
	}

	if _, err = c.client.CompleteOrchestratorTask(ctx, &resp); err != nil {
		if ctx.Err() != nil {
			c.logger.Warn("failed to complete orchestration task: context canceled")
		} else {
			c.logger.Errorf("failed to complete orchestration task: %v", err)
		}
	}
}

func (c *TaskHubGrpcClient) processActivityWorkItem(
	ctx context.Context,
	executor backend.Executor,
	req *protos.ActivityRequest,
) {
	var ptc *protos.TraceContext = req.ParentTraceContext
	ctx, err := helpers.ContextFromTraceContext(ctx, ptc)
	if err != nil {
		fmt.Printf("%v: failed to parse trace context: %v", req.Name, err)
	}
	event := helpers.NewTaskScheduledEvent(req.TaskId, req.Name, req.Version, req.Input, ptc)
	result, err := executor.ExecuteActivity(ctx, api.InstanceID(req.OrchestrationInstance.InstanceId), event)

	resp := protos.ActivityResponse{InstanceId: req.OrchestrationInstance.InstanceId, TaskId: req.TaskId}
	if err != nil {
		// NOTE: At the time of writing, there's no known case where this error is returned.
		//       We add error handling here anyways, just in case.
		resp.FailureDetails = &protos.TaskFailureDetails{
			ErrorType:    fmt.Sprintf("%T", err),
			ErrorMessage: err.Error(),
		}
	} else if tc := result.GetTaskCompleted(); tc != nil {
		resp.Result = tc.Result
	} else if tf := result.GetTaskFailed(); tf != nil {
		resp.FailureDetails = tf.FailureDetails
	} else {
		resp.FailureDetails = &protos.TaskFailureDetails{
			ErrorType:    "UnknownTaskResult",
			ErrorMessage: "Unknown task result",
		}
	}

	if _, err = c.client.CompleteActivityTask(ctx, &resp); err != nil {
		if ctx.Err() != nil {
			c.logger.Warn("failed to complete activity task: context canceled")
		} else {
			c.logger.Errorf("failed to complete activity task: %v", err)
		}
	}
}
