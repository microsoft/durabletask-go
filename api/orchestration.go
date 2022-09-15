package api

import (
	"errors"
	"time"

	"github.com/cgillum/durabletask-go/internal/protos"
)

var (
	ErrInstanceNotFound = errors.New("No such instance exists")
	ErrNotStarted       = errors.New("orchestration has not started")
	ErrNotCompleted     = errors.New("orchestration has not yet completed")
	ErrNoFailures       = errors.New("orchestration did not report failure details")
)

// InstanceID is a unique identifier for an orchestration instance.
type InstanceID string

type OrchestrationMetadata struct {
	instanceID             InstanceID
	name                   string
	status                 protos.OrchestrationStatus
	createdAt              time.Time
	lastUpdatedAt          time.Time
	serializedInput        string
	serializedOutput       string
	serializedCustomStatus string
	failureDetails         *protos.TaskFailureDetails
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
	failureDetails *protos.TaskFailureDetails) OrchestrationMetadata {
	return OrchestrationMetadata{
		instanceID:             iid,
		name:                   name,
		status:                 status,
		createdAt:              createdAt,
		lastUpdatedAt:          lastUpdatedAt,
		serializedInput:        serializedInput,
		serializedOutput:       serializedOutput,
		serializedCustomStatus: serializedCustomStatus,
		failureDetails:         failureDetails,
	}
}

func (o OrchestrationMetadata) InstanceID() InstanceID {
	return o.instanceID
}

func (o OrchestrationMetadata) Name() string {
	return o.name
}

func (o OrchestrationMetadata) RuntimeStatus() protos.OrchestrationStatus {
	return o.status
}

func (o OrchestrationMetadata) CreatedAt() time.Time {
	return o.createdAt
}

func (o OrchestrationMetadata) LastUpdatedAt() time.Time {
	return o.lastUpdatedAt
}

func (o OrchestrationMetadata) SerializedInput() string {
	return o.serializedInput
}

func (o OrchestrationMetadata) SerializedOutput() (string, error) {
	if o.IsComplete() {
		return o.serializedOutput, nil
	}

	return "", ErrNotCompleted
}

func (o OrchestrationMetadata) SerializedCustomStatus() string {
	return o.serializedCustomStatus
}

func (o OrchestrationMetadata) IsRunning() bool {
	return !o.IsComplete()
}

func (o OrchestrationMetadata) IsComplete() bool {
	return o.status == protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED ||
		o.status == protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED ||
		o.status == protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED ||
		o.status == protos.OrchestrationStatus_ORCHESTRATION_STATUS_CANCELED
}

func (o OrchestrationMetadata) FailureDetails() (*protos.TaskFailureDetails, error) {
	if !o.IsComplete() {
		return nil, ErrNotCompleted
	}

	if o.failureDetails == nil {
		return nil, ErrNoFailures
	}

	return o.failureDetails, nil
}
