package api

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/dapr/durabletask-go/api/protos"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var (
	ErrInstanceNotFound  = errors.New("no such instance exists")
	ErrNotStarted        = errors.New("orchestration has not started")
	ErrNotCompleted      = errors.New("orchestration has not yet completed")
	ErrNoFailures        = errors.New("orchestration did not report failure details")
	ErrDuplicateInstance = errors.New("orchestration instance already exists")
	ErrIgnoreInstance    = errors.New("ignore creating orchestration instance")

	EmptyInstanceID = InstanceID("")
)

type CreateOrchestrationAction = protos.CreateOrchestrationAction

const (
	REUSE_ID_ACTION_ERROR     CreateOrchestrationAction = protos.CreateOrchestrationAction_ERROR
	REUSE_ID_ACTION_IGNORE    CreateOrchestrationAction = protos.CreateOrchestrationAction_IGNORE
	REUSE_ID_ACTION_TERMINATE CreateOrchestrationAction = protos.CreateOrchestrationAction_TERMINATE
)

type OrchestrationStatus = protos.OrchestrationStatus

const (
	RUNTIME_STATUS_RUNNING          OrchestrationStatus = protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING
	RUNTIME_STATUS_COMPLETED        OrchestrationStatus = protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED
	RUNTIME_STATUS_CONTINUED_AS_NEW OrchestrationStatus = protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW
	RUNTIME_STATUS_FAILED           OrchestrationStatus = protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED
	RUNTIME_STATUS_CANCELED         OrchestrationStatus = protos.OrchestrationStatus_ORCHESTRATION_STATUS_CANCELED
	RUNTIME_STATUS_TERMINATED       OrchestrationStatus = protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED
	RUNTIME_STATUS_PENDING          OrchestrationStatus = protos.OrchestrationStatus_ORCHESTRATION_STATUS_PENDING
	RUNTIME_STATUS_SUSPENDED        OrchestrationStatus = protos.OrchestrationStatus_ORCHESTRATION_STATUS_SUSPENDED
)

type OrchestrationIdReusePolicy = protos.OrchestrationIdReusePolicy

// InstanceID is a unique identifier for an orchestration instance.
type InstanceID string

// NewOrchestrationOptions configures options for starting a new orchestration.
type NewOrchestrationOptions func(*protos.CreateInstanceRequest) error

// GetOrchestrationMetadataOptions is a set of options for fetching orchestration metadata.
type FetchOrchestrationMetadataOptions func(*protos.GetInstanceRequest)

// RaiseEventOptions is a set of options for raising an orchestration event.
type RaiseEventOptions func(*protos.RaiseEventRequest) error

// TerminateOptions is a set of options for terminating an orchestration.
type TerminateOptions func(*protos.TerminateRequest) error

// PurgeOptions is a set of options for purging an orchestration.
type PurgeOptions func(*protos.PurgeInstancesRequest) error

// WithInstanceID configures an explicit orchestration instance ID. If not specified,
// a random UUID value will be used for the orchestration instance ID.
func WithInstanceID(id InstanceID) NewOrchestrationOptions {
	return func(req *protos.CreateInstanceRequest) error {
		req.InstanceId = string(id)
		return nil
	}
}

// WithOrchestrationIdReusePolicy configures Orchestration ID reuse policy.
func WithOrchestrationIdReusePolicy(policy *protos.OrchestrationIdReusePolicy) NewOrchestrationOptions {
	return func(req *protos.CreateInstanceRequest) error {
		// initialize CreateInstanceOption
		req.OrchestrationIdReusePolicy = &protos.OrchestrationIdReusePolicy{
			OperationStatus: policy.OperationStatus,
			Action:          policy.Action,
		}
		return nil
	}
}

// WithInput configures an input for the orchestration. The specified input must be serializable.
func WithInput(input any) NewOrchestrationOptions {
	return func(req *protos.CreateInstanceRequest) error {
		bytes, err := json.Marshal(input)
		if err != nil {
			return err
		}
		req.Input = wrapperspb.String(string(bytes))
		return nil
	}
}

// WithRawInput configures an input for the orchestration. The specified input must be a string.
func WithRawInput(rawInput *wrapperspb.StringValue) NewOrchestrationOptions {
	return func(req *protos.CreateInstanceRequest) error {
		req.Input = rawInput
		return nil
	}
}

// WithStartTime configures a start time at which the orchestration should start running.
// Note that the actual start time could be later than the specified start time if the
// task hub is under load or if the app is not running at the specified start time.
func WithStartTime(startTime time.Time) NewOrchestrationOptions {
	return func(req *protos.CreateInstanceRequest) error {
		req.ScheduledStartTimestamp = timestamppb.New(startTime)
		return nil
	}
}

// WithFetchPayloads configures whether to load orchestration inputs, outputs, and custom status values, which could be large.
func WithFetchPayloads(fetchPayloads bool) FetchOrchestrationMetadataOptions {
	return func(req *protos.GetInstanceRequest) {
		req.GetInputsAndOutputs = fetchPayloads
	}
}

// WithEventPayload configures an event payload. The specified payload must be serializable.
func WithEventPayload(data any) RaiseEventOptions {
	return func(req *protos.RaiseEventRequest) error {
		bytes, err := json.Marshal(data)
		if err != nil {
			return err
		}
		req.Input = wrapperspb.String(string(bytes))
		return nil
	}
}

// WithRawEventData configures an event payload that is a raw, unprocessed string (e.g. JSON data).
func WithRawEventData(data *wrapperspb.StringValue) RaiseEventOptions {
	return func(req *protos.RaiseEventRequest) error {
		req.Input = data
		return nil
	}
}

// WithOutput configures an output for the terminated orchestration. The specified output must be serializable.
func WithOutput(data any) TerminateOptions {
	return func(req *protos.TerminateRequest) error {
		bytes, err := json.Marshal(data)
		if err != nil {
			return err
		}
		req.Output = wrapperspb.String(string(bytes))
		return nil
	}
}

// WithRawOutput configures a raw, unprocessed output (i.e. pre-serialized) for the terminated orchestration.
func WithRawOutput(data *wrapperspb.StringValue) TerminateOptions {
	return func(req *protos.TerminateRequest) error {
		req.Output = data
		return nil
	}
}

// WithRecursiveTerminate configures whether to terminate all sub-orchestrations created by the target orchestration.
func WithRecursiveTerminate(recursive bool) TerminateOptions {
	return func(req *protos.TerminateRequest) error {
		req.Recursive = recursive
		return nil
	}
}

// WithRecursivePurge configures whether to purge all sub-orchestrations created by the target orchestration.
func WithRecursivePurge(recursive bool) PurgeOptions {
	return func(req *protos.PurgeInstancesRequest) error {
		req.Recursive = recursive
		return nil
	}
}

func OrchestrationMetadataIsRunning(o *protos.OrchestrationMetadata) bool {
	return !OrchestrationMetadataIsComplete(o)
}

func OrchestrationMetadataIsComplete(o *protos.OrchestrationMetadata) bool {
	return o.RuntimeStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED ||
		o.RuntimeStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED ||
		o.RuntimeStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED ||
		o.RuntimeStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_CANCELED
}
