package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var (
	ErrInstanceNotFound = errors.New("no such instance exists")
	ErrNotStarted       = errors.New("orchestration has not started")
	ErrNotCompleted     = errors.New("orchestration has not yet completed")
	ErrNoFailures       = errors.New("orchestration did not report failure details")

	EmptyInstanceID = InstanceID("")
)

// InstanceID is a unique identifier for an orchestration instance.
type InstanceID string

type OrchestrationMetadata struct {
	InstanceID             InstanceID
	Name                   string
	RuntimeStatus          protos.OrchestrationStatus
	CreatedAt              time.Time
	LastUpdatedAt          time.Time
	SerializedInput        string
	SerializedOutput       string
	SerializedCustomStatus string
	FailureDetails         *protos.TaskFailureDetails
}

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
func WithRawInput(rawInput string) NewOrchestrationOptions {
	return func(req *protos.CreateInstanceRequest) error {
		req.Input = wrapperspb.String(rawInput)
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
func WithRawEventData(data string) RaiseEventOptions {
	return func(req *protos.RaiseEventRequest) error {
		req.Input = wrapperspb.String(data)
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
func WithRawOutput(data string) TerminateOptions {
	return func(req *protos.TerminateRequest) error {
		req.Output = wrapperspb.String(data)
		return nil
	}
}

// WithRecursive configures whether to terminate all sub-orchestrations created by the target orchestration.
func WithRecursive(recursive bool) TerminateOptions {
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

func NewOrchestrationMetadata(
	iid InstanceID,
	name string,
	status protos.OrchestrationStatus,
	createdAt time.Time,
	lastUpdatedAt time.Time,
	serializedInput string,
	serializedOutput string,
	serializedCustomStatus string,
	failureDetails *protos.TaskFailureDetails,
) *OrchestrationMetadata {
	return &OrchestrationMetadata{
		InstanceID:             iid,
		Name:                   name,
		RuntimeStatus:          status,
		CreatedAt:              createdAt,
		LastUpdatedAt:          lastUpdatedAt,
		SerializedInput:        serializedInput,
		SerializedOutput:       serializedOutput,
		SerializedCustomStatus: serializedCustomStatus,
		FailureDetails:         failureDetails,
	}
}

func (m *OrchestrationMetadata) MarshalJSON() ([]byte, error) {
	obj := make(map[string]any, 16)

	// Required values
	obj["id"] = m.InstanceID
	obj["name"] = m.Name
	obj["status"] = helpers.ToRuntimeStatusString(m.RuntimeStatus)
	obj["createdAt"] = m.CreatedAt
	obj["lastUpdatedAt"] = m.LastUpdatedAt

	// Optional values
	if m.SerializedInput != "" {
		obj["serializedInput"] = m.SerializedInput
	}
	if m.SerializedOutput != "" {
		obj["serializedOutput"] = m.SerializedOutput
	}
	if m.SerializedCustomStatus != "" {
		obj["serializedCustomStatus"] = m.SerializedCustomStatus
	}

	// Optional failure details (recursive)
	if m.FailureDetails != nil {
		const fieldCount = 4
		root := make(map[string]any, fieldCount)
		current := root
		f := m.FailureDetails
		for {
			current["type"] = f.ErrorType
			current["message"] = f.ErrorMessage
			if f.StackTrace != nil {
				current["stackTrace"] = f.StackTrace.GetValue()
			}
			if f.InnerFailure == nil {
				// base case
				break
			}
			// recursive case
			f = f.InnerFailure
			inner := make(map[string]any, fieldCount)
			current["innerFailure"] = inner
			current = inner
		}
		obj["failureDetails"] = root
	}
	return json.Marshal(obj)
}

func (m *OrchestrationMetadata) UnmarshalJSON(data []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if rerr, ok := r.(error); ok {
				err = fmt.Errorf("failed to unmarshal the JSON payload: %w", rerr)
			} else {
				err = errors.New("failed to unmarshal the JSON payload")
			}
		}
	}()

	var obj map[string]any
	if err := json.Unmarshal(data, &obj); err != nil {
		return fmt.Errorf("failed to unmarshal orchestration metadata json: %w", err)
	}

	if id, ok := obj["id"]; ok {
		m.InstanceID = InstanceID(id.(string))
	} else {
		return errors.New("missing 'id' field")
	}
	if name, ok := obj["name"]; ok {
		m.Name = name.(string)
	} else {
		return errors.New("missing 'name' field")
	}
	if status, ok := obj["status"]; ok {
		m.RuntimeStatus = helpers.FromRuntimeStatusString(status.(string))
	} else {
		return errors.New("missing 'name' field")
	}
	if createdAt, ok := obj["createdAt"]; ok {
		if time, err := time.Parse(time.RFC3339, createdAt.(string)); err == nil {
			m.CreatedAt = time
		} else {
			return errors.New("invalid 'createdAt' field: must be RFC3339 format")
		}
	} else {
		return errors.New("missing 'createdAt' field")
	}
	if lastUpdatedAt, ok := obj["lastUpdatedAt"]; ok {
		if time, err := time.Parse(time.RFC3339, lastUpdatedAt.(string)); err == nil {
			m.LastUpdatedAt = time
		} else {
			return errors.New("invalid 'lastUpdatedAt' field: must be RFC3339 format")
		}
	} else {
		return errors.New("missing 'lastUpdatedAt' field")
	}
	if input, ok := obj["serializedInput"]; ok {
		m.SerializedInput = input.(string)
	}
	if output, ok := obj["serializedOutput"]; ok {
		m.SerializedOutput = output.(string)
	}
	if output, ok := obj["serializedCustomStatus"]; ok {
		m.SerializedCustomStatus = output.(string)
	}

	failureDetails, ok := obj["failureDetails"]
	if ok {
		m.FailureDetails = &protos.TaskFailureDetails{}
		current := m.FailureDetails
		obj = failureDetails.(map[string]any)
		for {
			current.ErrorType = obj["type"].(string)
			current.ErrorMessage = obj["message"].(string)
			if stackTrace, ok := obj["stackTrace"]; ok {
				current.StackTrace = wrapperspb.String(stackTrace.(string))
			}
			if innerFailure, ok := obj["innerFailure"]; ok {
				// recursive case
				next := &protos.TaskFailureDetails{}
				current.InnerFailure = next
				current = next
				obj = innerFailure.(map[string]any)
			} else {
				// base case
				break
			}
		}
	}
	return nil
}

func (o *OrchestrationMetadata) IsRunning() bool {
	return !o.IsComplete()
}

func (o *OrchestrationMetadata) IsComplete() bool {
	return o.RuntimeStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED ||
		o.RuntimeStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED ||
		o.RuntimeStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED ||
		o.RuntimeStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_CANCELED
}
