package exporthistory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
)

// ClientOptions configures [Client]. ContainerName and Prefix supply the
// destination for jobs created without an explicit [ExportDestination].
type ClientOptions struct {
	// ContainerName is the default destination container.
	ContainerName string
	// Prefix is the default destination path prefix. When empty each job gets
	// the mode-and-ID prefix "<mode>-<jobId>/", matching .NET.
	Prefix string
}

// taskHubClient is the narrow task hub surface the export client uses.
type taskHubClient interface {
	ScheduleNewOrchestration(
		ctx context.Context,
		orchestrator string,
		opts ...api.NewOrchestrationOptions,
	) (api.InstanceID, error)
	WaitForOrchestrationCompletion(
		ctx context.Context,
		id api.InstanceID,
		opts ...api.FetchOrchestrationMetadataOptions,
	) (*api.OrchestrationMetadata, error)
	TerminateOrchestration(ctx context.Context, id api.InstanceID, opts ...api.TerminateOptions) error
	PurgeOrchestrationState(ctx context.Context, id api.InstanceID, opts ...api.PurgeOptions) error
	GetEntity(ctx context.Context, entityID api.EntityID, options ...api.GetEntityOptions) (*api.EntityMetadata, error)
	QueryEntities(ctx context.Context, query api.EntityQuery) (*api.EntityQueryResults, error)
}

// Client creates and inspects export jobs in a task hub.
type Client struct {
	hub       taskHubClient
	container string
	prefix    string
}

// NewClient returns an export history client backed by client.
func NewClient(client *durabletaskclient.TaskHubGrpcClient, options ClientOptions) (*Client, error) {
	if client == nil {
		return nil, &ValidationError{Message: "task hub client is required"}
	}
	return newClient(client, options)
}

// newClient builds a client over the narrow task hub surface so tests can
// supply their own implementation.
func newClient(hub taskHubClient, options ClientOptions) (*Client, error) {
	if hub == nil {
		return nil, &ValidationError{Message: "task hub client is required"}
	}
	container := strings.TrimSpace(options.ContainerName)
	if container != "" && !validBlobContainerName(container) {
		return nil, &ValidationError{
			Message: fmt.Sprintf("default export container %q is not a valid Azure Blob container name", container),
		}
	}
	if err := validateBlobPrefix(options.Prefix); err != nil {
		return nil, err
	}
	return &Client{
		hub:       hub,
		container: container,
		prefix:    options.Prefix,
	}, nil
}

// JobClient returns a handle for jobID without contacting the service.
func (c *Client) JobClient(jobID string) (*JobClient, error) {
	if c == nil || c.hub == nil {
		return nil, &ValidationError{Message: "export history client is required"}
	}
	if err := validateJobID(jobID); err != nil {
		return nil, err
	}
	return &JobClient{
		hub:       c.hub,
		container: c.container,
		prefix:    c.prefix,
		jobID:     jobID,
	}, nil
}

// CreateJob normalizes options, resolves the destination, creates the job, and
// returns a handle for it.
func (c *Client) CreateJob(ctx context.Context, options JobCreationOptions) (*JobClient, error) {
	if c == nil || c.hub == nil {
		return nil, &ValidationError{Message: "export history client is required"}
	}
	normalized, err := options.Normalize()
	if err != nil {
		return nil, err
	}
	job, err := c.JobClient(normalized.JobID)
	if err != nil {
		return nil, err
	}
	if err := job.Create(ctx, normalized); err != nil {
		return nil, err
	}
	return job, nil
}

// GetJob returns the current description of jobID, or a [NotFoundError] when
// the job does not exist.
func (c *Client) GetJob(ctx context.Context, jobID string) (*ExportJobDescription, error) {
	job, err := c.JobClient(jobID)
	if err != nil {
		return nil, err
	}
	return job.Describe(ctx)
}

// ListJobs returns one page of export jobs matching query. Status and
// creation-time filters are applied after the service returns the entity page,
// so a page can contain fewer than PageSize items and an empty page can still
// carry a continuation token.
func (c *Client) ListJobs(ctx context.Context, query ExportJobQuery) (*ExportJobQueryResult, error) {
	if c == nil || c.hub == nil {
		return nil, &ValidationError{Message: "export history client is required"}
	}
	pageSize := query.PageSize
	switch {
	case pageSize < 0:
		return nil, &ValidationError{Message: "page size cannot be negative"}
	case pageSize == 0:
		pageSize = DefaultJobQueryPageSize
	case pageSize > api.MaxInstanceQueryPageSize:
		return nil, &ValidationError{
			Message: fmt.Sprintf("page size cannot exceed %d", api.MaxInstanceQueryPageSize),
		}
	}
	if strings.Contains(query.JobIDPrefix, "@") {
		return nil, &ValidationError{Message: "export job ID prefix must not contain '@'"}
	}
	if query.Status != nil && !query.Status.IsValid() {
		return nil, &ValidationError{
			Message: fmt.Sprintf("invalid export job status %d", int(*query.Status)),
		}
	}
	entities, err := c.hub.QueryEntities(ctx, api.EntityQuery{
		InstanceIDStartsWith: strings.ToLower("@"+ExportJobEntityName+"@") + query.JobIDPrefix,
		PageSize:             pageSize,
		ContinuationToken:    query.ContinuationToken,
	})
	if err != nil {
		return nil, err
	}
	if entities == nil {
		return nil, errors.New("export job query returned no result")
	}
	result := &ExportJobQueryResult{ContinuationToken: entities.ContinuationToken}
	for _, entity := range entities.Entities {
		description, err := jobDescription(entity)
		if err != nil {
			return nil, err
		}
		if description == nil || !query.matches(description) {
			continue
		}
		result.Jobs = append(result.Jobs, description)
	}
	return result, nil
}

// JobClient manages one export job.
type JobClient struct {
	hub       taskHubClient
	container string
	prefix    string
	jobID     string
}

// ID returns the job identifier managed by this handle.
func (c *JobClient) ID() string {
	if c == nil {
		return ""
	}
	return c.jobID
}

// Create creates the job, or recreates it in place when it previously failed or
// completed. Creating a job that is already Active fails with an
// [InvalidTransitionError].
//
// Each successful Create reserves a new generation-specific orchestration ID.
// Previous generations' orchestration histories are not terminated or purged.
func (c *JobClient) Create(ctx context.Context, options JobCreationOptions) error {
	if c == nil || c.hub == nil {
		return &ValidationError{Message: "export job client is required"}
	}
	if options.JobID != "" && options.JobID != c.jobID {
		return &ValidationError{
			JobID:   c.jobID,
			Message: fmt.Sprintf("creation job ID %q does not match export job client %q", options.JobID, c.jobID),
		}
	}
	options.JobID = c.jobID
	normalized, err := options.Normalize()
	if err != nil {
		return err
	}
	destination, err := c.resolveDestination(normalized)
	if err != nil {
		return err
	}
	normalized.Destination = destination
	return c.operate(ctx, createOperation, normalized, nil)
}

// resolveDestination fills in the client's configured container and prefix when
// the caller did not supply a destination.
func (c *JobClient) resolveDestination(options JobCreationOptions) (*ExportDestination, error) {
	destination := ExportDestination{}
	if options.Destination != nil {
		destination = *options.Destination
	}
	if destination.Container == "" {
		destination.Container = c.container
	}
	if destination.Prefix == "" {
		destination.Prefix = c.prefix
	}
	if destination.Prefix == "" {
		destination.Prefix = defaultPrefix(options.Mode, c.jobID)
	}
	if destination.Container == "" {
		return nil, &ValidationError{
			JobID: c.jobID,
			Message: "export destination container is required because the client has no default container; " +
				"set ClientOptions.ContainerName or JobCreationOptions.Destination",
		}
	}
	if err := destination.Validate(); err != nil {
		return nil, withJobID(err, c.jobID)
	}
	return &destination, nil
}

// Describe returns the job's current description, or a [NotFoundError].
func (c *JobClient) Describe(ctx context.Context) (*ExportJobDescription, error) {
	if c == nil || c.hub == nil {
		return nil, &ValidationError{Message: "export job client is required"}
	}
	entity, err := c.hub.GetEntity(ctx, EntityID(c.jobID))
	if err != nil {
		return nil, err
	}
	if entity == nil {
		return nil, &NotFoundError{JobID: c.jobID}
	}
	description, err := jobDescription(entity)
	if err != nil {
		return nil, err
	}
	if description == nil {
		return nil, &NotFoundError{JobID: c.jobID}
	}
	description.JobID = c.jobID
	return description, nil
}

// Delete removes the job's entity state and stops the generation it removed.
//
// The entity delete and the orchestration cleanup are not atomic: the entity is
// deleted first and returns its captured orchestration ID. Cleanup targets only
// that ID, so a concurrent Create cannot have its new generation terminated or
// purged by this Delete. A missing job or orchestration is not an error.
func (c *JobClient) Delete(ctx context.Context) error {
	if c == nil || c.hub == nil {
		return &ValidationError{Message: "export job client is required"}
	}
	var instanceID string
	if err := c.operate(ctx, deleteOperation, nil, &instanceID); err != nil {
		return err
	}
	return c.terminateAndPurgeOrchestration(ctx, api.InstanceID(instanceID))
}

func (c *JobClient) terminateAndPurgeOrchestration(ctx context.Context, instanceID api.InstanceID) error {
	if instanceID == "" {
		return nil
	}
	err := c.hub.TerminateOrchestration(ctx, instanceID, api.WithOutput("Export job deleted"))
	if err != nil {
		if isInstanceMissing(err) {
			return nil
		}
		return fmt.Errorf("failed to terminate export orchestration %s: %w", instanceID, err)
	}
	if _, err := c.hub.WaitForOrchestrationCompletion(ctx, instanceID); err != nil {
		if isInstanceMissing(err) {
			return nil
		}
		return fmt.Errorf("failed to wait for export orchestration %s to terminate: %w", instanceID, err)
	}
	if err := c.hub.PurgeOrchestrationState(ctx, instanceID); err != nil {
		if isInstanceMissing(err) {
			return nil
		}
		return fmt.Errorf("failed to purge export orchestration %s: %w", instanceID, err)
	}
	return nil
}

// operate runs one entity operation through the system operation orchestrator
// and translates a failed orchestration back into a typed error. Results are
// decoded through the metadata's data converter into the caller's target.
func (c *JobClient) operate(ctx context.Context, operation string, input, output any) error {
	instanceID, err := c.hub.ScheduleNewOrchestration(
		ctx,
		ExecuteExportJobOperationOrchestratorName,
		api.WithInput(ExportJobOperationRequest{
			EntityID:      EntityID(c.jobID),
			OperationName: operation,
			Input:         input,
		}),
		// System operations must not inherit an application default version.
		api.WithVersion(""),
	)
	if err != nil {
		return err
	}
	metadata, err := c.hub.WaitForOrchestrationCompletion(ctx, instanceID)
	if err != nil {
		return err
	}
	if metadata == nil || metadata.RuntimeStatus != api.RUNTIME_STATUS_COMPLETED {
		return operationFailure(c.jobID, operation, metadata)
	}
	if output != nil {
		if metadata.SerializedOutput == "" {
			return fmt.Errorf("export job %q %s returned no output", c.jobID, operation)
		}
		if err := metadata.ReadOutput(output); err != nil {
			return fmt.Errorf("failed to decode %s result for export job %q: %w", operation, c.jobID, err)
		}
	}
	return nil
}

func defaultPrefix(mode ExportMode, jobID string) string {
	return strings.ToLower(mode.String()) + "-" + jobID + "/"
}

func isInstanceMissing(err error) bool {
	return errors.Is(err, api.ErrInstanceNotFound)
}

func jobDescription(metadata *api.EntityMetadata) (*ExportJobDescription, error) {
	if metadata == nil || metadata.SerializedState == "" {
		return nil, nil
	}
	var state ExportJobState
	if err := json.Unmarshal([]byte(metadata.SerializedState), &state); err != nil {
		return nil, fmt.Errorf("failed to deserialize export job state: %w", err)
	}
	return state.description(metadata.InstanceID.Key), nil
}

// operationFailure reconstructs the typed error the entity raised, so a caller
// can match an invalid transition or a validation failure with errors.Is and
// errors.As across the orchestration boundary.
func operationFailure(
	jobID string,
	operation string,
	metadata *api.OrchestrationMetadata,
) error {
	if metadata == nil {
		return &OperationError{JobID: jobID, Operation: operation}
	}
	if details := findFailure(metadata.FailureDetails, invalidTransitionErrorType); details != nil {
		return &InvalidTransitionError{
			JobID:     stringProperty(details.Properties, "jobId", jobID),
			From:      ExportJobStatus(intProperty(details.Properties, "from")),
			To:        ExportJobStatus(intProperty(details.Properties, "to")),
			Operation: stringProperty(details.Properties, "operation", operation),
		}
	}
	if details := findFailure(metadata.FailureDetails, notFoundErrorType); details != nil {
		return &NotFoundError{JobID: stringProperty(details.Properties, "jobId", jobID)}
	}
	if details := findFailure(metadata.FailureDetails, validationErrorType); details != nil {
		return &ValidationError{
			JobID:   stringProperty(details.Properties, "jobId", jobID),
			Message: stringProperty(details.Properties, "message", details.ErrorMessage),
		}
	}
	return &OperationError{
		JobID:          jobID,
		Operation:      operation,
		RuntimeStatus:  metadata.RuntimeStatus,
		FailureDetails: metadata.FailureDetails,
	}
}

func findFailure(details *api.FailureDetails, errorType api.ErrorType) *api.FailureDetails {
	for current := details; current != nil; current = current.InnerFailure {
		if current.ErrorType == errorType ||
			strings.HasSuffix(string(current.ErrorType), "."+string(errorType)) {
			return current
		}
	}
	return nil
}

func stringProperty(properties map[string]any, name, fallback string) string {
	if value, ok := properties[name].(string); ok && value != "" {
		return value
	}
	return fallback
}

func intProperty(properties map[string]any, name string) int {
	switch value := properties[name].(type) {
	case int:
		return value
	case int32:
		return int(value)
	case int64:
		return int(value)
	case float64:
		return int(value)
	default:
		return 0
	}
}
