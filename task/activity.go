package task

import (
	"context"
	"fmt"
	"maps"
	"math"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// CallActivityOption configures an activity invocation.
type CallActivityOption func(*callActivityOptions, api.DataConverter) error

type callActivityOptions struct {
	rawInput    *wrapperspb.StringValue
	version     *wrapperspb.StringValue
	retryPolicy *RetryPolicy
	tags        map[string]string
}

func (options *callActivityOptions) versionOrInherited(inheritedVersion string) *wrapperspb.StringValue {
	if options.version != nil {
		return options.version
	}
	// Preserve nil for implicit unversioned calls; an explicit empty version remains non-nil.
	if inheritedVersion == "" {
		return nil
	}
	return wrapperspb.String(inheritedVersion)
}

// WithActivityVersion configures the activity version.
func WithActivityVersion(version string) CallActivityOption {
	return func(opt *callActivityOptions, _ api.DataConverter) error {
		opt.version = wrapperspb.String(version)
		return nil
	}
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
	// Optional deterministic function that controls whether retries should proceed.
	Handle func(RetryContext) bool
}

// RetryContext contains durable inputs for a retry decision.
// Retry handlers execute during replay and must not perform I/O or depend on wall-clock time.
type RetryContext struct {
	LastAttemptNumber int
	// LastFailure is never nil while Handle is running and must be treated as read-only.
	LastFailure    *api.FailureDetails
	TotalRetryTime time.Duration
}

// Normalized validates the retry policy and returns an independent copy with
// default values applied. The receiver is not modified.
func (policy *RetryPolicy) Normalized() (RetryPolicy, error) {
	if policy == nil {
		return RetryPolicy{}, fmt.Errorf("%w: retry policy cannot be nil", api.ErrInvalidArgument)
	}
	normalized := *policy
	if normalized.InitialRetryInterval <= 0 {
		return RetryPolicy{}, fmt.Errorf("%w: InitialRetryInterval must be greater than 0", api.ErrInvalidArgument)
	}
	if normalized.MaxAttempts <= 0 {
		// setting 1 max attempt is equivalent to not retrying
		normalized.MaxAttempts = 1
	}
	if normalized.BackoffCoefficient <= 0 {
		normalized.BackoffCoefficient = 1
	}
	if normalized.MaxRetryInterval <= 0 {
		normalized.MaxRetryInterval = math.MaxInt64
	}
	if normalized.RetryTimeout <= 0 {
		normalized.RetryTimeout = math.MaxInt64
	}
	if normalized.Handle == nil {
		normalized.Handle = func(RetryContext) bool { return true }
	}
	return normalized, nil
}

// Validate reports whether the retry policy is valid without modifying it.
func (policy *RetryPolicy) Validate() error {
	_, err := policy.Normalized()
	return err
}

// WithActivityInput configures an input for an activity invocation.
// The configured data converter must be able to serialize the input.
func WithActivityInput(input any) CallActivityOption {
	return func(opt *callActivityOptions, converter api.DataConverter) error {
		data, err := marshalData(converter, input)
		if err != nil {
			return err
		}
		opt.rawInput = wrapperspb.String(string(data))
		return nil
	}
}

// WithRawActivityInput configures a raw input for an activity invocation.
func WithRawActivityInput(input string) CallActivityOption {
	return func(opt *callActivityOptions, _ api.DataConverter) error {
		opt.rawInput = wrapperspb.String(input)
		return nil
	}
}

// WithActivityTags adds user tags to an activity invocation. Explicit activity
// tags override orchestration tags with the same key.
func WithActivityTags(tags map[string]string) CallActivityOption {
	return func(opt *callActivityOptions, _ api.DataConverter) error {
		if err := validateUnreservedKeys("activity tag", tags); err != nil {
			return err
		}
		opt.tags = maps.Clone(tags)
		return nil
	}
}

// WithActivityRetryPolicy snapshots policy when this option is created. Later
// caller mutations do not affect activity retries.
func WithActivityRetryPolicy(policy *RetryPolicy) CallActivityOption {
	if policy == nil {
		return func(*callActivityOptions, api.DataConverter) error { return nil }
	}
	snapshot := *policy
	return func(opt *callActivityOptions, _ api.DataConverter) error {
		normalized, err := snapshot.Normalized()
		if err != nil {
			return err
		}
		opt.retryPolicy = &normalized
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

	rawInput  []byte
	ctx       context.Context
	converter api.DataConverter
}

// Activity is the functional interface for activity implementations.
type Activity func(ctx ActivityContext) (any, error)

func newTaskActivityContext(
	ctx context.Context,
	taskID int32,
	ts *protos.TaskScheduledEvent,
	converter api.DataConverter,
) *activityContext {
	return &activityContext{
		TaskID:    taskID,
		Name:      ts.Name,
		rawInput:  []byte(ts.Input.GetValue()),
		ctx:       ctx,
		converter: converter,
	}
}

// GetInput unmarshals the serialized activity input and saves the result into [v].
func (actx *activityContext) GetInput(v any) error {
	return unmarshalData(actx.converter, actx.rawInput, v)
}

func (actx *activityContext) Context() context.Context {
	return actx.ctx
}
