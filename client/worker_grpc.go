package client

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type workItemsStream interface {
	Recv() (*protos.WorkItem, error)
}

func (c *TaskHubGrpcClient) StartWorkItemListener(ctx context.Context, r *task.TaskRegistry) error {
	executor := task.NewTaskExecutor(r)

	var stream workItemsStream

	initStream := func() error {
		_, err := c.client.Hello(ctx, &emptypb.Empty{})
		if err != nil {
			return fmt.Errorf("failed to connect to task hub service: %w", err)
		}

		req := protos.GetWorkItemsRequest{}
		stream, err = c.client.GetWorkItems(ctx, &req)
		if err != nil {
			return fmt.Errorf("failed to get work item stream: %w", err)
		}
		return nil
	}

	c.logger.Infof("connecting work item listener stream")
	err := initStream()
	if err != nil {
		return err
	}

	go func() {
		c.logger.Info("starting background processor")
		for {
			// TODO: Manage concurrency
			workItem, err := stream.Recv()

			if err != nil {
				// user wants to stop the listener
				if ctx.Err() != nil {
					c.logger.Infof("stopping background processor: %v", err)
					return
				}

				retriable := false

				c.logger.Errorf("background processor received stream error: %v", err)

				if err == io.EOF {
					retriable = true
				} else if grpcStatus, ok := status.FromError(err); ok {
					c.logger.Warnf("received grpc error code %v", grpcStatus.Code().String())
					switch grpcStatus.Code() {
					case codes.Unavailable:
						fallthrough
					case codes.Canceled:
						fallthrough
					default:
						retriable = true
					}
				}

				if !retriable {
					c.logger.Infof("stopping background processor, non retriable error: %v", err)
					return
				}

				err = backoff.Retry(
					func() error {
						// user wants to stop the listener
						if ctx.Err() != nil {
							return backoff.Permanent(ctx.Err())
						}

						c.logger.Infof("reconnecting work item listener stream")
						streamErr := initStream()
						if streamErr != nil {
							c.logger.Errorf("error initializing work item listener stream %v", streamErr)
							return streamErr
						}
						return nil
					},
					// retry forever since we don't have a way of asynchronously return errors to the user
					newInfiniteRetries(),
				)
				if err != nil {
					c.logger.Infof("stopping background processor, unable to reconnect stream: %v", err)
					return
				}
				c.logger.Infof("successfully reconnected work item listener stream...")
				// continue iterating
				continue
			}

			if orchReq := workItem.GetOrchestratorRequest(); orchReq != nil {
				go c.processOrchestrationWorkItem(ctx, executor, orchReq)
			} else if actReq := workItem.GetActivityRequest(); actReq != nil {
				go c.processActivityWorkItem(ctx, executor, actReq)
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
	var tc *protos.TraceContext = nil // TODO: How to populate trace context?
	event := helpers.NewTaskScheduledEvent(req.TaskId, req.Name, req.Version, req.Input, tc)
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

func newInfiniteRetries() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	// max wait of 15 seconds between retries
	b.MaxInterval = 15 * time.Second
	// retry forever
	b.MaxElapsedTime = 0
	b.Reset()
	return b
}
