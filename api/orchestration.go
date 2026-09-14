package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/internal/tagcodec"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var (
	ErrInstanceNotFound  = errors.New("no such instance exists")
	ErrNotCompleted      = errors.New("orchestration has not yet completed")
	ErrDuplicateInstance = errors.New("orchestration instance already exists")
	ErrInvalidState      = errors.New("orchestration is not in a valid state for this operation")

	EmptyInstanceID = InstanceID("")
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

// OrchestrationIDReusePolicy controls deduplication when an orchestration with
// the requested instance ID already exists.
//
// A nil DedupeStatuses slice uses the service default. An empty, non-nil slice
// allows every configurable status to be replaced. Otherwise, an existing
// instance whose status appears in DedupeStatuses causes scheduling to fail.
// ContinuedAsNew is not configurable because it is an execution transition
// rather than a stable instance status.
type OrchestrationIDReusePolicy struct {
	DedupeStatuses []OrchestrationStatus
}

// InstanceID is a unique identifier for an orchestration instance.
type InstanceID string

type OrchestrationMetadata struct {
	InstanceID             InstanceID
	Name                   string
	Version                string
	ExecutionID            string
	ParentInstanceID       InstanceID
	RuntimeStatus          protos.OrchestrationStatus
	ScheduledStartAt       time.Time
	CreatedAt              time.Time
	LastUpdatedAt          time.Time
	CompletedAt            time.Time
	SerializedInput        string
	SerializedOutput       string
	SerializedCustomStatus string
	FailureDetails         *FailureDetails
	Tags                   map[string]string
	Converter              DataConverter `json:"-"`
}

// NewOrchestrationOptions configures options for starting a new orchestration.
type NewOrchestrationOptions func(*protos.CreateInstanceRequest, DataConverter) error

// FetchOrchestrationMetadataOptions configures orchestration metadata retrieval.
type FetchOrchestrationMetadataOptions func(*protos.GetInstanceRequest)

// RaiseEventOptions is a set of options for raising an orchestration event.
type RaiseEventOptions func(*protos.RaiseEventRequest, DataConverter) error

// TerminateOptions is a set of options for terminating an orchestration.
type TerminateOptions func(*protos.TerminateRequest, DataConverter) error

// PurgeOptions is a set of options for purging an orchestration.
type PurgeOptions func(*protos.PurgeInstancesRequest) error

// WithInstanceID configures an explicit orchestration instance ID. If not specified,
// a random UUID value will be used for the orchestration instance ID.
func WithInstanceID(id InstanceID) NewOrchestrationOptions {
	return func(req *protos.CreateInstanceRequest, _ DataConverter) error {
		if err := helpers.ValidateOrchestrationInstanceID(string(id)); err != nil {
			return err
		}
		req.InstanceId = string(id)
		return nil
	}
}

// WithOrchestrationIDReusePolicy configures orchestration instance-ID reuse.
func WithOrchestrationIDReusePolicy(policy *OrchestrationIDReusePolicy) NewOrchestrationOptions {
	return func(req *protos.CreateInstanceRequest, _ DataConverter) error {
		if policy == nil || policy.DedupeStatuses == nil {
			req.OrchestrationIdReusePolicy = nil
			return nil
		}
		for _, status := range policy.DedupeStatuses {
			if !isReusableOrchestrationStatus(status) {
				return invalidArgument(fmt.Sprintf("invalid orchestration dedupe status: %s", status))
			}
		}
		replaceable := make([]protos.OrchestrationStatus, 0, len(reusableOrchestrationStatuses))
		for _, status := range reusableOrchestrationStatuses {
			if !slices.Contains(policy.DedupeStatuses, status) {
				replaceable = append(replaceable, status)
			}
		}
		req.OrchestrationIdReusePolicy = &protos.OrchestrationIdReusePolicy{
			ReplaceableStatus: replaceable,
		}
		return nil
	}
}

var reusableOrchestrationStatuses = [...]OrchestrationStatus{
	RUNTIME_STATUS_RUNNING,
	RUNTIME_STATUS_COMPLETED,
	RUNTIME_STATUS_FAILED,
	RUNTIME_STATUS_CANCELED,
	RUNTIME_STATUS_TERMINATED,
	RUNTIME_STATUS_PENDING,
	RUNTIME_STATUS_SUSPENDED,
}

func isReusableOrchestrationStatus(status OrchestrationStatus) bool {
	return slices.Contains(reusableOrchestrationStatuses[:], status)
}

// WithInput configures an input for the orchestration. The specified input must be serializable.
func WithInput(input any) NewOrchestrationOptions {
	return func(req *protos.CreateInstanceRequest, converter DataConverter) error {
		payload, err := SerializeData(converter, input)
		if err != nil {
			return err
		}
		req.Input = wrapperspb.String(payload)
		return nil
	}
}

// WithRawInput configures an input for the orchestration. The specified input must be a string.
func WithRawInput(rawInput string) NewOrchestrationOptions {
	return func(req *protos.CreateInstanceRequest, _ DataConverter) error {
		req.Input = wrapperspb.String(rawInput)
		return nil
	}
}

// WithStartTime configures a start time at which the orchestration should start running.
// Note that the actual start time could be later than the specified start time if the
// task hub is under load or if the app is not running at the specified start time.
func WithStartTime(startTime time.Time) NewOrchestrationOptions {
	return func(req *protos.CreateInstanceRequest, _ DataConverter) error {
		req.ScheduledStartTimestamp = timestamppb.New(startTime)
		return nil
	}
}

// WithVersion configures the orchestration version.
func WithVersion(version string) NewOrchestrationOptions {
	return func(req *protos.CreateInstanceRequest, _ DataConverter) error {
		req.Version = wrapperspb.String(version)
		return nil
	}
}

// WithContextFields configures immutable fields propagated into orchestration
// and activity contexts. The fields are persisted with orchestration history.
func WithContextFields(fields ContextFields) NewOrchestrationOptions {
	return func(req *protos.CreateInstanceRequest, _ DataConverter) error {
		for key := range fields {
			if strings.HasPrefix(key, ReservedContextFieldPrefix) {
				return invalidArgument(fmt.Sprintf("context field %q uses reserved prefix %q", key, ReservedContextFieldPrefix))
			}
			if strings.HasPrefix(key, tagcodec.UserTagPrefix) {
				return invalidArgument(fmt.Sprintf("context field %q uses reserved prefix %q", key, tagcodec.UserTagPrefix))
			}
		}
		req.Tags = tagcodec.Merge(req.Tags, tagcodec.EncodeContextFields(fields))
		return nil
	}
}

// WithTags configures orchestration tags that are persisted and returned by metadata queries.
func WithTags(tags map[string]string) NewOrchestrationOptions {
	return func(req *protos.CreateInstanceRequest, _ DataConverter) error {
		for key := range tags {
			if key == "" {
				return invalidArgument("tag key cannot be empty")
			}
			if strings.HasPrefix(key, ReservedContextFieldPrefix) {
				return invalidArgument(fmt.Sprintf("tag %q uses reserved prefix %q", key, ReservedContextFieldPrefix))
			}
			if strings.HasPrefix(key, tagcodec.UserTagPrefix) {
				return invalidArgument(fmt.Sprintf("tag %q uses reserved prefix %q", key, tagcodec.UserTagPrefix))
			}
		}
		req.Tags = tagcodec.Merge(req.Tags, tagcodec.EncodeUserTags(tags))
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
	return func(req *protos.RaiseEventRequest, converter DataConverter) error {
		payload, err := SerializeData(converter, data)
		if err != nil {
			return err
		}
		req.Input = wrapperspb.String(payload)
		return nil
	}
}

// WithRawEventData configures an event payload that is a raw, unprocessed string (e.g. JSON data).
func WithRawEventData(data string) RaiseEventOptions {
	return func(req *protos.RaiseEventRequest, _ DataConverter) error {
		req.Input = wrapperspb.String(data)
		return nil
	}
}

// WithOutput configures an output for the terminated orchestration. The specified output must be serializable.
func WithOutput(data any) TerminateOptions {
	return func(req *protos.TerminateRequest, converter DataConverter) error {
		payload, err := SerializeData(converter, data)
		if err != nil {
			return err
		}
		req.Output = wrapperspb.String(payload)
		return nil
	}
}

// WithRawOutput configures a raw, unprocessed output (i.e. pre-serialized) for the terminated orchestration.
func WithRawOutput(data string) TerminateOptions {
	return func(req *protos.TerminateRequest, _ DataConverter) error {
		req.Output = wrapperspb.String(data)
		return nil
	}
}

// WithRecursiveTerminate configures whether to terminate all sub-orchestrations created by the target orchestration.
func WithRecursiveTerminate(recursive bool) TerminateOptions {
	return func(req *protos.TerminateRequest, _ DataConverter) error {
		req.Recursive = recursive
		return nil
	}
}

// ReadInput deserializes the orchestration input with the metadata converter.
func (m *OrchestrationMetadata) ReadInput(target any) error {
	return deserializePayload(m.Converter, m.SerializedInput, target)
}

// ReadOutput deserializes the orchestration output with the metadata converter.
func (m *OrchestrationMetadata) ReadOutput(target any) error {
	return deserializePayload(m.Converter, m.SerializedOutput, target)
}

// ReadCustomStatus deserializes custom status with the metadata converter.
func (m *OrchestrationMetadata) ReadCustomStatus(target any) error {
	return deserializePayload(m.Converter, m.SerializedCustomStatus, target)
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
	failureDetails *FailureDetails,
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

type orchestrationMetadataJSON struct {
	InstanceID             *InstanceID       `json:"id"`
	Name                   *string           `json:"name"`
	Status                 *string           `json:"status"`
	CreatedAt              *time.Time        `json:"createdAt"`
	LastUpdatedAt          *time.Time        `json:"lastUpdatedAt"`
	Version                string            `json:"version,omitempty"`
	ExecutionID            string            `json:"executionId,omitempty"`
	ParentInstanceID       InstanceID        `json:"parentInstanceId,omitempty"`
	ScheduledStartAt       *time.Time        `json:"scheduledStartAt,omitempty"`
	CompletedAt            *time.Time        `json:"completedAt,omitempty"`
	SerializedInput        string            `json:"serializedInput,omitempty"`
	SerializedOutput       string            `json:"serializedOutput,omitempty"`
	SerializedCustomStatus string            `json:"serializedCustomStatus,omitempty"`
	FailureDetails         *FailureDetails   `json:"failureDetails,omitempty"`
	Tags                   map[string]string `json:"tags,omitempty"`
}

func (m *OrchestrationMetadata) MarshalJSON() ([]byte, error) {
	status := helpers.ToRuntimeStatusString(m.RuntimeStatus)
	payload := orchestrationMetadataJSON{
		InstanceID:             &m.InstanceID,
		Name:                   &m.Name,
		Status:                 &status,
		CreatedAt:              &m.CreatedAt,
		LastUpdatedAt:          &m.LastUpdatedAt,
		Version:                m.Version,
		ExecutionID:            m.ExecutionID,
		ParentInstanceID:       m.ParentInstanceID,
		SerializedInput:        m.SerializedInput,
		SerializedOutput:       m.SerializedOutput,
		SerializedCustomStatus: m.SerializedCustomStatus,
		FailureDetails:         m.FailureDetails,
		Tags:                   maps.Clone(m.Tags),
	}
	if !m.ScheduledStartAt.IsZero() {
		payload.ScheduledStartAt = &m.ScheduledStartAt
	}
	if !m.CompletedAt.IsZero() {
		payload.CompletedAt = &m.CompletedAt
	}
	return json.Marshal(payload)
}

func (m *OrchestrationMetadata) UnmarshalJSON(data []byte) error {
	var payload orchestrationMetadataJSON
	if err := json.Unmarshal(data, &payload); err != nil {
		return fmt.Errorf("failed to unmarshal orchestration metadata json: %w", err)
	}
	if payload.InstanceID == nil {
		return errors.New("missing 'id' field")
	}
	if payload.Name == nil {
		return errors.New("missing 'name' field")
	}
	if payload.Status == nil {
		return errors.New("missing 'status' field")
	}
	if payload.CreatedAt == nil {
		return errors.New("missing 'createdAt' field")
	}
	if payload.LastUpdatedAt == nil {
		return errors.New("missing 'lastUpdatedAt' field")
	}
	m.InstanceID = *payload.InstanceID
	m.Name = *payload.Name
	m.RuntimeStatus = helpers.FromRuntimeStatusString(*payload.Status)
	m.CreatedAt = *payload.CreatedAt
	m.LastUpdatedAt = *payload.LastUpdatedAt
	m.Version = payload.Version
	m.ExecutionID = payload.ExecutionID
	m.ParentInstanceID = payload.ParentInstanceID
	m.SerializedInput = payload.SerializedInput
	m.SerializedOutput = payload.SerializedOutput
	m.SerializedCustomStatus = payload.SerializedCustomStatus
	m.FailureDetails = payload.FailureDetails
	m.Tags = maps.Clone(payload.Tags)
	if payload.ScheduledStartAt != nil {
		m.ScheduledStartAt = *payload.ScheduledStartAt
	}
	if payload.CompletedAt != nil {
		m.CompletedAt = *payload.CompletedAt
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
