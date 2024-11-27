package task

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type callActivityOption func(*callActivityOptions) error

type callActivityOptions struct {
	rawInput    *wrapperspb.StringValue
	retryPolicy *RetryPolicy
}

type RetryPolicy struct {
	// Max number of attempts to try the activity call, first execution inclusive
	MaxAttempts int
	// Timespan to wait for the first retry
	InitialRetryInterval time.Duration
	// Used to determine rate of increase of back-off
	BackoffCoefficient float64
	// Max timespan to wait for a retry
	MaxRetryInterval time.Duration
	// Total timeout across all the retries performed
	RetryTimeout time.Duration
	// Optional function to control if retries should proceed
	Handle func(error) bool
}

func (policy *RetryPolicy) Validate() error {
	if policy.InitialRetryInterval <= 0 {
		return fmt.Errorf("InitialRetryInterval must be greater than 0")
	}
	if policy.MaxAttempts <= 0 {
		// setting 1 max attempt is equivalent to not retrying
		policy.MaxAttempts = 1
	}
	if policy.BackoffCoefficient <= 0 {
		policy.BackoffCoefficient = 1
	}
	if policy.MaxRetryInterval <= 0 {
		policy.MaxRetryInterval = math.MaxInt64
	}
	if policy.RetryTimeout <= 0 {
		policy.RetryTimeout = math.MaxInt64
	}
	if policy.Handle == nil {
		policy.Handle = func(err error) bool {
			return true
		}
	}
	return nil
}

// WithActivityInput configures an input for an activity invocation.
// The specified input must be JSON serializable.
func WithActivityInput(input any) callActivityOption {
	return func(opt *callActivityOptions) error {
		data, err := marshalData(input)
		if err != nil {
			return err
		}
		opt.rawInput = wrapperspb.String(string(data))
		return nil
	}
}

// WithRawActivityInput configures a raw input for an activity invocation.
func WithRawActivityInput(input string) callActivityOption {
	return func(opt *callActivityOptions) error {
		opt.rawInput = wrapperspb.String(input)
		return nil
	}
}

func WithActivityRetryPolicy(policy *RetryPolicy) callActivityOption {
	return func(opt *callActivityOptions) error {
		if policy == nil {
			return nil
		}
		err := policy.Validate()
		if err != nil {
			return err
		}
		opt.retryPolicy = policy
		return nil
	}
}

// ActivityContext is the context parameter type for activity implementations.
type ActivityContext interface {
	GetInput(resultPtr any) error
	Context() context.Context
}

type activityContext struct {
	TaskID int32
	Name   string

	rawInput []byte
	ctx      context.Context
}

// Activity is the functional interface for activity implementations.
type Activity func(ctx ActivityContext) (any, error)

func newTaskActivityContext(ctx context.Context, taskID int32, ts *protos.TaskScheduledEvent) *activityContext {
	return &activityContext{
		TaskID:   taskID,
		Name:     ts.Name,
		rawInput: []byte(ts.Input.GetValue()),
		ctx:      ctx,
	}
}

// GetInput unmarshals the serialized activity input and saves the result into [v].
func (actx *activityContext) GetInput(v any) error {
	return unmarshalData(actx.rawInput, v)
}

func (actx *activityContext) Context() context.Context {
	return actx.ctx
}
