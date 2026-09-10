package durabletaskscheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/bits"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/tagcodec"
	"github.com/microsoft/durabletask-go/task"
)

const (
	// ScheduleEntityName is the system entity name used to persist schedules.
	ScheduleEntityName = "Schedule"
	// ExecuteScheduleOperationOrchestratorName is the system orchestrator used
	// for schedule mutations.
	ExecuteScheduleOperationOrchestratorName = "ExecuteScheduleOperationOrchestrator"
	// ExecuteScheduledTaskOrchestratorName applies orchestration-only options
	// such as tags, context fields, and retries to a scheduled target.
	ExecuteScheduledTaskOrchestratorName = "ExecuteScheduledTaskOrchestrator"

	createScheduleOperation = "CreateSchedule"
	updateScheduleOperation = "UpdateSchedule"
	pauseScheduleOperation  = "PauseSchedule"
	resumeScheduleOperation = "ResumeSchedule"
	runScheduleOperation    = "RunSchedule"
	deleteScheduleOperation = "delete"
)

var (
	// ErrScheduleNotFound identifies attempts to access a missing schedule.
	ErrScheduleNotFound = errors.New("schedule not found")
	// ErrScheduleInvalidTransition identifies invalid schedule state changes.
	ErrScheduleInvalidTransition = errors.New("invalid schedule state transition")
	// ErrScheduleValidation identifies invalid schedule configuration.
	ErrScheduleValidation = errors.New("invalid schedule configuration")
	// ErrScheduleOperationFailed identifies a failed schedule mutation.
	ErrScheduleOperationFailed = errors.New("schedule operation failed")
)

const (
	scheduleValidationErrorType      api.ErrorType = "ScheduleClientValidationException"
	scheduleInvalidTransitionType    api.ErrorType = "ScheduleInvalidTransitionException"
	scheduleOperationFailedErrorType api.ErrorType = "ScheduleOperationFailed"
)

// ScheduleStatus is the persisted status of a schedule.
type ScheduleStatus int

const (
	ScheduleStatusUninitialized ScheduleStatus = iota
	ScheduleStatusActive
	ScheduleStatusPaused
)

// ScheduleNotFoundError is returned when a schedule does not exist.
type ScheduleNotFoundError struct{ ScheduleID string }

func (e *ScheduleNotFoundError) Error() string {
	return fmt.Sprintf("schedule with ID %q was not found", e.ScheduleID)
}
func (e *ScheduleNotFoundError) Unwrap() error { return ErrScheduleNotFound }

// ScheduleInvalidTransitionError is returned for an invalid state transition.
type ScheduleInvalidTransitionError struct {
	ScheduleID string
	From       ScheduleStatus
	To         ScheduleStatus
	Operation  string
}

func (e *ScheduleInvalidTransitionError) Error() string {
	return fmt.Sprintf("invalid state transition attempted for schedule %q: cannot transition from %s to %s during %s operation",
		e.ScheduleID, e.From, e.To, e.Operation)
}
func (e *ScheduleInvalidTransitionError) Unwrap() error { return ErrScheduleInvalidTransition }

// ScheduleValidationError is returned when schedule options are invalid.
type ScheduleValidationError struct{ Message string }

func (e *ScheduleValidationError) Error() string {
	return "schedule validation failed: " + e.Message
}
func (e *ScheduleValidationError) Unwrap() error { return ErrScheduleValidation }

func (*ScheduleValidationError) DurableTaskErrorType() api.ErrorType {
	return scheduleValidationErrorType
}

func (e *ScheduleValidationError) DurableTaskErrorProperties() map[string]any {
	return map[string]any{"message": e.Message}
}

func (*ScheduleInvalidTransitionError) DurableTaskErrorType() api.ErrorType {
	return scheduleInvalidTransitionType
}

func (e *ScheduleInvalidTransitionError) DurableTaskErrorProperties() map[string]any {
	return map[string]any{
		"scheduleId": e.ScheduleID,
		"from":       int(e.From),
		"to":         int(e.To),
		"operation":  e.Operation,
	}
}

// ScheduleOperationError describes a schedule mutation orchestration that did
// not complete successfully.
type ScheduleOperationError struct {
	ScheduleID     string
	Operation      string
	RuntimeStatus  api.OrchestrationStatus
	FailureDetails *api.FailureDetails
}

func (e *ScheduleOperationError) Error() string {
	message := ""
	if e.FailureDetails != nil {
		message = e.FailureDetails.ErrorMessage
	}
	if message == "" {
		return fmt.Sprintf("schedule %q operation %s failed with status %v", e.ScheduleID, e.Operation, e.RuntimeStatus)
	}
	return fmt.Sprintf("schedule %q operation %s failed: %s", e.ScheduleID, e.Operation, message)
}

func (e *ScheduleOperationError) Unwrap() error { return ErrScheduleOperationFailed }

func (*ScheduleOperationError) DurableTaskErrorType() api.ErrorType {
	return scheduleOperationFailedErrorType
}

func (s ScheduleStatus) String() string {
	switch s {
	case ScheduleStatusUninitialized:
		return "Uninitialized"
	case ScheduleStatusActive:
		return "Active"
	case ScheduleStatusPaused:
		return "Paused"
	default:
		return fmt.Sprintf("ScheduleStatus(%d)", s)
	}
}

// ScheduleCreationOptions configures a recurring interval schedule.
//
// OrchestrationInput is already serialized input. TypedOrchestrationInput is
// serialized by the configured DataConverter and takes precedence when non-nil.
// Effective orchestration instance IDs must not use the reserved entity format.
// Retry-backed schedules generate separate instance IDs for each attempt.
// Cron schedules are not supported.
type ScheduleCreationOptions struct {
	ScheduleID              string
	OrchestrationName       string
	Interval                time.Duration
	OrchestrationInput      string
	TypedOrchestrationInput any
	OrchestrationInstanceID string
	OrchestrationVersion    string
	StartAt                 time.Time
	EndAt                   time.Time
	StartImmediatelyIfLate  bool
	Tags                    map[string]string
	ContextFields           api.ContextFields
	RetryPolicy             *ScheduleRetryPolicy
}

// ScheduleUpdateOptions applies a sparse update to a schedule. Nil pointers
// mean the corresponding field is unchanged. Empty string values are ignored.
// Nil maps leave stored values unchanged; non-nil empty maps clear them.
type ScheduleUpdateOptions struct {
	OrchestrationName       *string
	OrchestrationInput      *string
	TypedOrchestrationInput any
	OrchestrationInstanceID *string
	OrchestrationVersion    *string
	StartAt                 *time.Time
	EndAt                   *time.Time
	Interval                *time.Duration
	StartImmediatelyIfLate  *bool
	Tags                    map[string]string
	ContextFields           api.ContextFields
	RetryPolicy             *ScheduleRetryPolicy
}

// ScheduleRetryPolicy is the serializable subset of [task.RetryPolicy] used
// when a scheduled target orchestration fails.
type ScheduleRetryPolicy struct {
	MaxAttempts          int
	InitialRetryInterval time.Duration
	BackoffCoefficient   float64
	MaxRetryInterval     time.Duration
	RetryTimeout         time.Duration
}

// scheduleCreationWire is the .NET-compatible JSON shape of
// ScheduleCreationOptions. `omitempty` only affects marshaling, so the same
// shape decodes payloads written by other DTS SDKs.
type scheduleCreationWire struct {
	ScheduleID              string               `json:"ScheduleId"`
	OrchestrationName       string               `json:"OrchestrationName"`
	Interval                dotNetSpan           `json:"Interval"`
	OrchestrationInput      string               `json:"OrchestrationInput,omitempty"`
	TypedOrchestrationInput any                  `json:"TypedOrchestrationInput,omitempty"`
	OrchestrationInstanceID string               `json:"OrchestrationInstanceId,omitempty"`
	OrchestrationVersion    string               `json:"OrchestrationVersion,omitempty"`
	StartAt                 *time.Time           `json:"StartAt,omitempty"`
	EndAt                   *time.Time           `json:"EndAt,omitempty"`
	StartImmediatelyIfLate  bool                 `json:"StartImmediatelyIfLate"`
	Tags                    map[string]string    `json:"Tags,omitempty"`
	ContextFields           api.ContextFields    `json:"ContextFields,omitempty"`
	RetryPolicy             *scheduleRetryPolicy `json:"RetryPolicy,omitempty"`
}

// scheduleUpdateWire is the .NET-compatible JSON shape of ScheduleUpdateOptions.
type scheduleUpdateWire struct {
	OrchestrationName       *string              `json:"OrchestrationName,omitempty"`
	OrchestrationInput      *string              `json:"OrchestrationInput,omitempty"`
	TypedOrchestrationInput any                  `json:"TypedOrchestrationInput,omitempty"`
	OrchestrationInstanceID *string              `json:"OrchestrationInstanceId,omitempty"`
	OrchestrationVersion    *string              `json:"OrchestrationVersion,omitempty"`
	StartAt                 *time.Time           `json:"StartAt,omitempty"`
	EndAt                   *time.Time           `json:"EndAt,omitempty"`
	Interval                *dotNetSpan          `json:"Interval,omitempty"`
	StartImmediatelyIfLate  *bool                `json:"StartImmediatelyIfLate,omitempty"`
	Tags                    map[string]string    `json:"Tags,omitzero"`
	ContextFields           api.ContextFields    `json:"ContextFields,omitzero"`
	RetryPolicy             *scheduleRetryPolicy `json:"RetryPolicy,omitempty"`
}

func (o ScheduleCreationOptions) MarshalJSON() ([]byte, error) {
	retry, err := scheduleRetryPolicyFromPublic(o.RetryPolicy)
	if err != nil {
		return nil, err
	}
	return json.Marshal(scheduleCreationWire{
		ScheduleID:              o.ScheduleID,
		OrchestrationName:       o.OrchestrationName,
		Interval:                dotNetSpan(o.Interval),
		OrchestrationInput:      o.OrchestrationInput,
		TypedOrchestrationInput: o.TypedOrchestrationInput,
		OrchestrationInstanceID: o.OrchestrationInstanceID,
		OrchestrationVersion:    o.OrchestrationVersion,
		StartAt:                 optionalTime(o.StartAt),
		EndAt:                   optionalTime(o.EndAt),
		StartImmediatelyIfLate:  o.StartImmediatelyIfLate,
		Tags:                    o.Tags,
		ContextFields:           o.ContextFields,
		RetryPolicy:             retry,
	})
}

func (o *ScheduleCreationOptions) UnmarshalJSON(data []byte) error {
	var decoded scheduleCreationWire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*o = ScheduleCreationOptions{
		ScheduleID:              decoded.ScheduleID,
		OrchestrationName:       decoded.OrchestrationName,
		Interval:                time.Duration(decoded.Interval),
		OrchestrationInput:      decoded.OrchestrationInput,
		TypedOrchestrationInput: decoded.TypedOrchestrationInput,
		OrchestrationInstanceID: decoded.OrchestrationInstanceID,
		OrchestrationVersion:    decoded.OrchestrationVersion,
		StartImmediatelyIfLate:  decoded.StartImmediatelyIfLate,
		Tags:                    decoded.Tags,
		ContextFields:           decoded.ContextFields,
		RetryPolicy:             decoded.RetryPolicy.public(),
	}
	if decoded.StartAt != nil {
		o.StartAt = *decoded.StartAt
	}
	if decoded.EndAt != nil {
		o.EndAt = *decoded.EndAt
	}
	return nil
}

func (o ScheduleUpdateOptions) MarshalJSON() ([]byte, error) {
	var interval *dotNetSpan
	if o.Interval != nil {
		value := dotNetSpan(*o.Interval)
		interval = &value
	}
	retry, err := scheduleRetryPolicyFromPublic(o.RetryPolicy)
	if err != nil {
		return nil, err
	}
	return json.Marshal(scheduleUpdateWire{
		OrchestrationName:       o.OrchestrationName,
		OrchestrationInput:      o.OrchestrationInput,
		TypedOrchestrationInput: o.TypedOrchestrationInput,
		OrchestrationInstanceID: o.OrchestrationInstanceID,
		OrchestrationVersion:    o.OrchestrationVersion,
		StartAt:                 o.StartAt,
		EndAt:                   o.EndAt,
		Interval:                interval,
		StartImmediatelyIfLate:  o.StartImmediatelyIfLate,
		Tags:                    o.Tags,
		ContextFields:           o.ContextFields,
		RetryPolicy:             retry,
	})
}

func (o *ScheduleUpdateOptions) UnmarshalJSON(data []byte) error {
	var decoded scheduleUpdateWire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*o = ScheduleUpdateOptions{
		OrchestrationName:       decoded.OrchestrationName,
		OrchestrationInput:      decoded.OrchestrationInput,
		TypedOrchestrationInput: decoded.TypedOrchestrationInput,
		OrchestrationInstanceID: decoded.OrchestrationInstanceID,
		OrchestrationVersion:    decoded.OrchestrationVersion,
		StartAt:                 decoded.StartAt,
		EndAt:                   decoded.EndAt,
		StartImmediatelyIfLate:  decoded.StartImmediatelyIfLate,
		Tags:                    decoded.Tags,
		ContextFields:           decoded.ContextFields,
		RetryPolicy:             decoded.RetryPolicy.public(),
	}
	if decoded.Interval != nil {
		value := time.Duration(*decoded.Interval)
		o.Interval = &value
	}
	return nil
}

// ScheduleDescription describes a schedule's current configuration and state.
type ScheduleDescription struct {
	ScheduleID              string
	OrchestrationName       string
	OrchestrationInput      string
	OrchestrationInstanceID string
	OrchestrationVersion    string
	StartAt                 time.Time
	EndAt                   time.Time
	Interval                time.Duration
	StartImmediatelyIfLate  bool
	Status                  ScheduleStatus
	ExecutionToken          string
	LastRunAt               time.Time
	NextRunAt               time.Time
	CreatedAt               time.Time
	LastModifiedAt          time.Time
	Tags                    map[string]string
	ContextFields           api.ContextFields
	RetryPolicy             *ScheduleRetryPolicy
	Converter               api.DataConverter `json:"-"`
}

// ReadInput deserializes the scheduled orchestration input.
func (d *ScheduleDescription) ReadInput(target any) error {
	if d == nil || target == nil || d.OrchestrationInput == "" {
		return nil
	}
	return api.NormalizeDataConverter(d.Converter).Deserialize(d.OrchestrationInput, target)
}

// ScheduleQuery filters a single page of schedules. Filters are applied after
// DTS returns the entity page, so a page can contain fewer than PageSize items.
type ScheduleQuery struct {
	Status           *ScheduleStatus
	ScheduleIDPrefix string
	// CreatedFrom is an exclusive lower creation-time bound, matching .NET.
	CreatedFrom time.Time
	// CreatedTo is an exclusive upper creation-time bound, matching .NET.
	CreatedTo         time.Time
	PageSize          int32
	ContinuationToken string
}

// ScheduleQueryResult is one page of schedules.
type ScheduleQueryResult struct {
	Schedules         []*ScheduleDescription
	ContinuationToken string
}

// ScheduleOperationRequest is the input consumed by
// ExecuteScheduleOperationOrchestrator.
type ScheduleOperationRequest struct {
	EntityID      api.EntityID `json:"EntityId"`
	OperationName string       `json:"OperationName"`
	Input         any          `json:"Input,omitempty"`
}

// RegisterScheduledTasks registers DTS's system schedule entity and operation
// orchestrator. The operation orchestrator is deliberately unversioned so it
// remains reachable when application default versioning is enabled.
func RegisterScheduledTasks(registry *task.TaskRegistry) error {
	return registerScheduledTasks(registry, "")
}

// RegisterScheduledTasksWithDefaultVersion registers scheduled-task system
// handlers and applies defaultVersion to scheduled target orchestrations that
// do not specify ScheduleCreationOptions.OrchestrationVersion.
func RegisterScheduledTasksWithDefaultVersion(registry *task.TaskRegistry, defaultVersion string) error {
	return registerScheduledTasks(registry, defaultVersion)
}

func registerScheduledTasks(registry *task.TaskRegistry, defaultVersion string) error {
	if registry == nil {
		return fmt.Errorf("task registry is required")
	}
	if err := registry.AddEntityN(ScheduleEntityName, func(ctx *task.EntityContext) (any, error) {
		return scheduleEntityWithDefaultVersion(ctx, defaultVersion)
	}); err != nil {
		return err
	}
	if err := registry.AddOrchestratorN(
		ExecuteScheduleOperationOrchestratorName,
		ExecuteScheduleOperationOrchestrator,
	); err != nil {
		return err
	}
	return registry.AddOrchestratorN(ExecuteScheduledTaskOrchestratorName, executeScheduledTaskOrchestrator)
}

// WithScheduledTasks advertises the DTS scheduled-task capability. Use it with
// RegisterScheduledTasks; registrations are then included by auto work-item
// filters derived from the registry.
func WithScheduledTasks() durabletaskclient.TaskHubGrpcWorkerOption {
	return durabletaskclient.CombineTaskHubGrpcWorkerOptions(
		durabletaskclient.WithScheduledTaskCapability(true),
		durabletaskclient.WithUnversionedOrchestratorNames(
			ExecuteScheduleOperationOrchestratorName,
			ExecuteScheduledTaskOrchestratorName,
		),
	)
}

// ExecuteScheduleOperationOrchestrator invokes one schedule entity operation.
// It must remain unversioned because schedule clients explicitly target the
// unversioned system orchestrator.
func ExecuteScheduleOperationOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var request ScheduleOperationRequest
	if err := ctx.GetInput(&request); err != nil {
		return nil, err
	}
	if request.OperationName == "" {
		return nil, &ScheduleValidationError{Message: "operation name is required"}
	}
	var result any
	if err := ctx.CallEntity(request.EntityID, request.OperationName, task.WithEntityInput(request.Input)).Await(&result); err != nil {
		return nil, err
	}
	return result, nil
}

type scheduledTaskRequest struct {
	OrchestrationName       string               `json:"OrchestrationName"`
	OrchestrationInput      string               `json:"OrchestrationInput,omitempty"`
	OrchestrationInstanceID string               `json:"OrchestrationInstanceId"`
	OrchestrationVersion    string               `json:"OrchestrationVersion,omitempty"`
	Tags                    map[string]string    `json:"Tags,omitempty"`
	ContextFields           api.ContextFields    `json:"ContextFields,omitempty"`
	RetryPolicy             *scheduleRetryPolicy `json:"RetryPolicy,omitempty"`
}

func executeScheduledTaskOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var request scheduledTaskRequest
	if err := ctx.GetInput(&request); err != nil {
		return nil, err
	}
	options := []task.SubOrchestratorOption{}
	if request.RetryPolicy == nil {
		options = append(options, task.WithSubOrchestrationInstanceID(request.OrchestrationInstanceID))
	}
	if request.OrchestrationInput != "" {
		options = append(options, task.WithRawSubOrchestratorInput(request.OrchestrationInput))
	}
	if request.OrchestrationVersion != "" {
		options = append(options, task.WithSubOrchestrationVersion(request.OrchestrationVersion))
	}
	if len(request.Tags) > 0 {
		options = append(options, task.WithSubOrchestrationTags(request.Tags))
	}
	if len(request.ContextFields) > 0 {
		options = append(options, task.WithSubOrchestrationContextFields(request.ContextFields))
	}
	if retry := request.RetryPolicy.taskPolicy(); retry != nil {
		options = append(options, task.WithSubOrchestrationRetryPolicy(retry))
	}
	if err := ctx.CallSubOrchestrator(request.OrchestrationName, options...).Await(nil); err != nil {
		return nil, err
	}
	return nil, nil
}

type scheduleState struct {
	Status                 ScheduleStatus         `json:"Status"`
	ExecutionToken         string                 `json:"ExecutionToken"`
	LastRunAt              time.Time              `json:"LastRunAt,omitempty"`
	NextRunAt              time.Time              `json:"NextRunAt,omitempty"`
	ScheduleCreatedAt      time.Time              `json:"ScheduleCreatedAt,omitempty"`
	ScheduleLastModifiedAt time.Time              `json:"ScheduleLastModifiedAt,omitempty"`
	ScheduleConfiguration  *scheduleConfiguration `json:"ScheduleConfiguration,omitempty"`
}

func (s scheduleState) MarshalJSON() ([]byte, error) {
	type stateJSON struct {
		Status                 ScheduleStatus         `json:"Status"`
		ExecutionToken         string                 `json:"ExecutionToken"`
		LastRunAt              *time.Time             `json:"LastRunAt"`
		NextRunAt              *time.Time             `json:"NextRunAt"`
		ScheduleCreatedAt      *time.Time             `json:"ScheduleCreatedAt"`
		ScheduleLastModifiedAt *time.Time             `json:"ScheduleLastModifiedAt"`
		ScheduleConfiguration  *scheduleConfiguration `json:"ScheduleConfiguration"`
	}
	return json.Marshal(stateJSON{
		Status:                 s.Status,
		ExecutionToken:         s.ExecutionToken,
		LastRunAt:              optionalTime(s.LastRunAt),
		NextRunAt:              optionalTime(s.NextRunAt),
		ScheduleCreatedAt:      optionalTime(s.ScheduleCreatedAt),
		ScheduleLastModifiedAt: optionalTime(s.ScheduleLastModifiedAt),
		ScheduleConfiguration:  s.ScheduleConfiguration,
	})
}

type scheduleConfiguration struct {
	OrchestrationName       string               `json:"OrchestrationName"`
	ScheduleID              string               `json:"ScheduleId"`
	OrchestrationInput      string               `json:"OrchestrationInput,omitempty"`
	OrchestrationInstanceID string               `json:"OrchestrationInstanceId,omitempty"`
	OrchestrationVersion    string               `json:"OrchestrationVersion,omitempty"`
	StartAt                 time.Time            `json:"StartAt,omitempty"`
	EndAt                   time.Time            `json:"EndAt,omitempty"`
	Interval                dotNetSpan           `json:"Interval"`
	StartImmediatelyIfLate  bool                 `json:"StartImmediatelyIfLate"`
	Tags                    map[string]string    `json:"Tags,omitempty"`
	ContextFields           api.ContextFields    `json:"ContextFields,omitempty"`
	RetryPolicy             *scheduleRetryPolicy `json:"RetryPolicy,omitempty"`
}

func (c scheduleConfiguration) MarshalJSON() ([]byte, error) {
	type configJSON scheduleConfiguration
	return json.Marshal(struct {
		configJSON
		StartAt *time.Time `json:"StartAt"`
		EndAt   *time.Time `json:"EndAt"`
	}{
		configJSON: configJSON(c),
		StartAt:    optionalTime(c.StartAt),
		EndAt:      optionalTime(c.EndAt),
	})
}

type scheduleRetryPolicy struct {
	MaxAttempts          int        `json:"MaxAttempts"`
	InitialRetryInterval dotNetSpan `json:"InitialRetryInterval"`
	BackoffCoefficient   float64    `json:"BackoffCoefficient"`
	MaxRetryInterval     dotNetSpan `json:"MaxRetryInterval"`
	RetryTimeout         dotNetSpan `json:"RetryTimeout"`
}

func scheduleRetryPolicyFromPublic(policy *ScheduleRetryPolicy) (*scheduleRetryPolicy, error) {
	if policy == nil {
		return nil, nil
	}
	taskPolicy, err := policy.taskPolicy().Normalized()
	if err != nil {
		return nil, &ScheduleValidationError{Message: err.Error()}
	}
	return &scheduleRetryPolicy{
		MaxAttempts:          taskPolicy.MaxAttempts,
		InitialRetryInterval: dotNetSpan(taskPolicy.InitialRetryInterval),
		BackoffCoefficient:   taskPolicy.BackoffCoefficient,
		MaxRetryInterval:     dotNetSpan(taskPolicy.MaxRetryInterval),
		RetryTimeout:         dotNetSpan(taskPolicy.RetryTimeout),
	}, nil
}

func (p *scheduleRetryPolicy) public() *ScheduleRetryPolicy {
	if p == nil {
		return nil
	}
	return &ScheduleRetryPolicy{
		MaxAttempts:          p.MaxAttempts,
		InitialRetryInterval: time.Duration(p.InitialRetryInterval),
		BackoffCoefficient:   p.BackoffCoefficient,
		MaxRetryInterval:     time.Duration(p.MaxRetryInterval),
		RetryTimeout:         time.Duration(p.RetryTimeout),
	}
}

func (p *scheduleRetryPolicy) taskPolicy() *task.RetryPolicy {
	if p == nil {
		return nil
	}
	return p.public().taskPolicy()
}

func (p *ScheduleRetryPolicy) taskPolicy() *task.RetryPolicy {
	if p == nil {
		return nil
	}
	return &task.RetryPolicy{
		MaxAttempts:          p.MaxAttempts,
		InitialRetryInterval: p.InitialRetryInterval,
		BackoffCoefficient:   p.BackoffCoefficient,
		MaxRetryInterval:     p.MaxRetryInterval,
		RetryTimeout:         p.RetryTimeout,
	}
}

func optionalTime(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	return &value
}

// dotNetSpan uses the System.TimeSpan JSON format so persisted schedule state
// remains readable by DTS clients that use the .NET default serializer.
type dotNetSpan time.Duration

func (d dotNetSpan) MarshalJSON() ([]byte, error) {
	duration := time.Duration(d)
	if duration < 0 {
		return nil, fmt.Errorf("negative TimeSpan is not supported")
	}
	days := duration / (24 * time.Hour)
	duration %= 24 * time.Hour
	hours := duration / time.Hour
	duration %= time.Hour
	minutes := duration / time.Minute
	duration %= time.Minute
	seconds := duration / time.Second
	fraction := duration % time.Second
	prefix := ""
	if days > 0 {
		prefix = fmt.Sprintf("%d.", days)
	}
	if fraction == 0 {
		return []byte(fmt.Sprintf("%q", fmt.Sprintf("%s%02d:%02d:%02d", prefix, hours, minutes, seconds))), nil
	}
	return []byte(fmt.Sprintf("%q", fmt.Sprintf("%s%02d:%02d:%02d.%07d", prefix, hours, minutes, seconds, fraction/100))), nil
}

func (d *dotNetSpan) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	invalid := fmt.Errorf("invalid TimeSpan %q", value)
	if strings.HasPrefix(value, "-") {
		return invalid
	}
	days := int64(0)
	timePart := value
	if daySeparator := strings.Index(value, "."); daySeparator >= 0 &&
		(daySeparator < strings.Index(value, ":")) {
		dayPart, rest := value[:daySeparator], value[daySeparator+1:]
		parsedDays, err := strconv.ParseInt(dayPart, 10, 64)
		if err != nil || parsedDays < 0 {
			return invalid
		}
		days, timePart = parsedDays, rest
	}
	parts := strings.Split(timePart, ":")
	if len(parts) != 3 {
		return invalid
	}
	hours, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || hours < 0 || (days > 0 && hours > 23) {
		return invalid
	}
	minutes, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || minutes < 0 || minutes > 59 {
		return invalid
	}
	secondPart, fraction, _ := strings.Cut(parts[2], ".")
	seconds, err := strconv.ParseInt(secondPart, 10, 64)
	if err != nil || seconds < 0 || seconds > 59 {
		return invalid
	}
	ticks := int64(0)
	if fraction != "" {
		if len(fraction) > 7 {
			return invalid
		}
		parsedTicks, err := strconv.ParseInt(fraction, 10, 64)
		if err != nil {
			return invalid
		}
		for i := len(fraction); i < 7; i++ {
			parsedTicks *= 10
		}
		ticks = parsedTicks
	}
	const day = 24 * time.Hour
	if days > int64((time.Duration(1<<63-1))/day) {
		return invalid
	}
	valueDuration := time.Duration(days)*day + time.Duration(hours)*time.Hour +
		time.Duration(minutes)*time.Minute + time.Duration(seconds)*time.Second + time.Duration(ticks)*100
	if valueDuration < 0 {
		return invalid
	}
	*d = dotNetSpan(valueDuration)
	return nil
}

func scheduleEntityWithDefaultVersion(ctx *task.EntityContext, defaultVersion string) (any, error) {
	var state scheduleState
	if rawState, ok := ctx.GetRawState(); ok {
		if err := json.Unmarshal([]byte(rawState), &state); err != nil {
			return nil, fmt.Errorf("failed to deserialize schedule state: %w", err)
		}
	}

	switch {
	case strings.EqualFold(ctx.Operation, deleteScheduleOperation):
		var token string
		if ctx.HasInput() {
			if err := ctx.GetInput(&token); err != nil {
				return nil, err
			}
		}
		if token != "" && token != state.ExecutionToken {
			return nil, nil
		}
		ctx.DeleteState()
		return nil, nil
	case strings.EqualFold(ctx.Operation, createScheduleOperation):
		var options ScheduleCreationOptions
		if err := ctx.GetInput(&options); err != nil {
			return nil, err
		}
		if err := createSchedule(ctx, &state, options); err != nil {
			return nil, err
		}
	case strings.EqualFold(ctx.Operation, updateScheduleOperation):
		var options ScheduleUpdateOptions
		if err := ctx.GetInput(&options); err != nil {
			return nil, err
		}
		if err := updateSchedule(ctx, &state, options); err != nil {
			return nil, err
		}
	case strings.EqualFold(ctx.Operation, pauseScheduleOperation):
		if err := pauseSchedule(&state); err != nil {
			return nil, err
		}
	case strings.EqualFold(ctx.Operation, resumeScheduleOperation):
		if err := resumeSchedule(ctx, &state); err != nil {
			return nil, err
		}
	case strings.EqualFold(ctx.Operation, runScheduleOperation):
		var token string
		if err := ctx.GetInput(&token); err != nil {
			return nil, err
		}
		if err := runSchedule(ctx, &state, token, defaultVersion); err != nil {
			return nil, err
		}
		if state.Status == ScheduleStatusUninitialized {
			return nil, nil
		}
	default:
		return nil, fmt.Errorf("schedule does not support operation %q", ctx.Operation)
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize schedule state: %w", err)
	}
	ctx.SetRawState(string(payload))
	return nil, nil
}

func createSchedule(ctx *task.EntityContext, state *scheduleState, options ScheduleCreationOptions) error {
	if err := validateCreation(options); err != nil {
		return err
	}
	config, err := scheduleConfigFromCreate(ctx, options)
	if err != nil {
		return err
	}
	now := scheduleNow(ctx)
	createdAt := state.ScheduleCreatedAt
	lastRunAt := state.LastRunAt
	*state = scheduleState{
		Status:                 ScheduleStatusActive,
		ExecutionToken:         newExecutionToken(),
		LastRunAt:              lastRunAt,
		ScheduleCreatedAt:      now,
		ScheduleLastModifiedAt: now,
		ScheduleConfiguration:  config,
	}
	if !createdAt.IsZero() {
		state.ScheduleCreatedAt = createdAt
	}
	return signalRun(ctx, config.ScheduleID, state.ExecutionToken, time.Time{})
}

func updateSchedule(ctx *task.EntityContext, state *scheduleState, options ScheduleUpdateOptions) error {
	if state.Status != ScheduleStatusActive && state.Status != ScheduleStatusPaused {
		return invalidTransition(scheduleID(state), state.Status, state.Status, updateScheduleOperation)
	}
	if state.ScheduleConfiguration == nil {
		return &ScheduleValidationError{Message: "schedule configuration is missing"}
	}
	config := *state.ScheduleConfiguration
	changed, resetNext, err := applyScheduleUpdate(ctx, &config, options)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	state.ScheduleConfiguration = &config
	state.ScheduleLastModifiedAt = scheduleNow(ctx)
	state.ExecutionToken = newExecutionToken()
	if resetNext {
		state.NextRunAt = time.Time{}
	}
	if state.Status == ScheduleStatusActive {
		return signalRun(ctx, config.ScheduleID, state.ExecutionToken, time.Time{})
	}
	return nil
}

func pauseSchedule(state *scheduleState) error {
	if state.Status != ScheduleStatusActive {
		return invalidTransition(scheduleID(state), state.Status, ScheduleStatusPaused, pauseScheduleOperation)
	}
	if state.ScheduleConfiguration == nil {
		return &ScheduleValidationError{Message: "schedule configuration is missing"}
	}
	state.Status = ScheduleStatusPaused
	state.NextRunAt = time.Time{}
	state.ExecutionToken = newExecutionToken()
	return nil
}

func resumeSchedule(ctx *task.EntityContext, state *scheduleState) error {
	if state.Status != ScheduleStatusPaused {
		return invalidTransition(scheduleID(state), state.Status, ScheduleStatusActive, resumeScheduleOperation)
	}
	if state.ScheduleConfiguration == nil {
		return &ScheduleValidationError{Message: "schedule configuration is missing"}
	}
	state.Status = ScheduleStatusActive
	state.NextRunAt = time.Time{}
	return signalRun(ctx, state.ScheduleConfiguration.ScheduleID, state.ExecutionToken, time.Time{})
}

func runSchedule(ctx *task.EntityContext, state *scheduleState, token, defaultVersion string) error {
	if state.Status == ScheduleStatusUninitialized {
		ctx.DeleteState()
		return nil
	}
	config := state.ScheduleConfiguration
	if config == nil {
		return &ScheduleValidationError{Message: "schedule configuration is missing"}
	}
	if token != state.ExecutionToken {
		return nil
	}
	if state.Status != ScheduleStatusActive {
		return invalidTransition(config.ScheduleID, state.Status, ScheduleStatusActive, runScheduleOperation)
	}
	now := scheduleNow(ctx)
	if !config.EndAt.IsZero() && now.After(config.EndAt) {
		state.NextRunAt = time.Time{}
		return ctx.SignalEntity(api.NewEntityID(ScheduleEntityName, config.ScheduleID), deleteScheduleOperation, state.ExecutionToken)
	}
	next, err := determineNextRun(state, config, now)
	if err != nil {
		return err
	}
	state.NextRunAt = next
	if !next.After(now) {
		if err := startScheduleOrchestration(ctx, config, next, defaultVersion); err != nil {
			return err
		}
		state.LastRunAt = next
		state.NextRunAt = time.Time{}
		state.NextRunAt, err = determineNextRun(state, config, now)
		if err != nil {
			return err
		}
	}
	return signalRun(ctx, config.ScheduleID, state.ExecutionToken, state.NextRunAt)
}

func signalRun(ctx *task.EntityContext, scheduleID, token string, at time.Time) error {
	entityID := api.NewEntityID(ScheduleEntityName, scheduleID)
	if at.IsZero() {
		return ctx.SignalEntity(entityID, runScheduleOperation, token)
	}
	return ctx.SignalEntityAt(entityID, at, runScheduleOperation, token)
}

func startScheduleOrchestration(ctx *task.EntityContext, config *scheduleConfiguration, at time.Time, defaultVersion string) error {
	instanceID := scheduledOrchestrationInstanceID(config.ScheduleID, config.OrchestrationInstanceID, at)
	version := config.OrchestrationVersion
	if version == "" {
		version = defaultVersion
	}
	if len(config.Tags) > 0 || len(config.ContextFields) > 0 || config.RetryPolicy != nil {
		request := scheduledTaskRequest{
			OrchestrationName:       config.OrchestrationName,
			OrchestrationInput:      config.OrchestrationInput,
			OrchestrationInstanceID: instanceID,
			OrchestrationVersion:    version,
			Tags:                    config.Tags,
			ContextFields:           config.ContextFields,
			RetryPolicy:             config.RetryPolicy,
		}
		payload, err := ctx.SerializeInput(request)
		if err != nil {
			return fmt.Errorf("failed to serialize scheduled orchestration request: %w", err)
		}
		return ctx.StartNewOrchestration(
			ExecuteScheduledTaskOrchestratorName,
			task.WithRawEntityStartOrchestrationInput(payload),
			task.WithEntityStartOrchestrationVersion(task.UnversionedTaskVersion),
		)
	}
	options := []task.EntityStartOrchestrationOption{
		task.WithEntityStartOrchestrationInstanceID(instanceID),
	}
	if config.OrchestrationInput != "" {
		options = append(options, task.WithRawEntityStartOrchestrationInput(config.OrchestrationInput))
	}
	if version != "" {
		options = append(options, task.WithEntityStartOrchestrationVersion(version))
	}
	return ctx.StartNewOrchestration(config.OrchestrationName, options...)
}

func scheduledOrchestrationInstanceID(scheduleID, instanceID string, at time.Time) string {
	if instanceID != "" {
		return instanceID
	}
	return scheduleID + "-" + at.UTC().Format("2006-01-02T15:04:05.0000000-07:00")
}

func determineNextRun(state *scheduleState, config *scheduleConfiguration, now time.Time) (time.Time, error) {
	if !state.NextRunAt.IsZero() {
		return state.NextRunAt, nil
	}
	start := config.StartAt
	if start.IsZero() {
		start = state.ScheduleCreatedAt
	}
	if start.IsZero() {
		start = now
	}
	if now.Before(start) {
		return start, nil
	}
	if state.LastRunAt.IsZero() && config.StartImmediatelyIfLate {
		return now, nil
	}
	interval := time.Duration(config.Interval)
	if interval < time.Second {
		return time.Time{}, &ScheduleValidationError{Message: "interval must be at least one second"}
	}
	// Land on the same instant .NET computes as
	// start + interval*(floor((now-start)/interval)+1) without relying on
	// time.Duration arithmetic, which saturates for extreme start times.
	delay := interval - intervalRemainder(start, now, interval)
	next := now.Add(delay)
	// Go can represent instants far beyond .NET's DateTimeOffset.MaxValue, so a
	// schedule running near the end of year 9999 would otherwise produce a next
	// run that only fails later when the state is serialized for the service.
	if next.Before(now) || next.After(maxScheduleTime) {
		return time.Time{}, &ScheduleValidationError{Message: "next scheduled time overflows time range"}
	}
	return next, nil
}

// maxScheduleTime is DateTimeOffset.MaxValue, the largest instant the .NET
// schedule wire format can carry. .NET timestamps have 100ns resolution, so the
// last representable tick is 999999900ns into the final second.
var maxScheduleTime = time.Date(9999, time.December, 31, 23, 59, 59, 999999900, time.UTC)

// intervalRemainder returns (now-start) mod interval exactly. It uses 128-bit
// arithmetic because the elapsed nanoseconds between the minimum and maximum
// representable timestamps do not fit in a time.Duration. Inputs outside the
// domain of that division clamp to zero: a non-positive interval, which would
// divide by zero, and a start after now, which would make the elapsed time
// negative.
func intervalRemainder(start, now time.Time, interval time.Duration) time.Duration {
	if interval <= 0 {
		return 0
	}
	seconds := now.Unix() - start.Unix()
	nanos := int64(now.Nanosecond()) - int64(start.Nanosecond())
	if nanos < 0 {
		seconds--
		nanos += int64(time.Second)
	}
	if seconds < 0 {
		return 0
	}
	high, low := bits.Mul64(uint64(seconds), uint64(time.Second))
	low, carry := bits.Add64(low, uint64(nanos), 0)
	high += carry
	divisor := uint64(interval)
	// Reducing the high word first keeps bits.Div64 in range and leaves the
	// remainder unchanged, since the discarded multiple of 2^64 divides evenly.
	_, remainder := bits.Div64(high%divisor, low, divisor)
	return time.Duration(remainder)
}

func scheduleConfigFromCreate(ctx *task.EntityContext, options ScheduleCreationOptions) (*scheduleConfiguration, error) {
	input, err := serializeScheduleInput(ctx, options.OrchestrationInput, options.TypedOrchestrationInput)
	if err != nil {
		return nil, err
	}
	retry, err := scheduleRetryPolicyFromPublic(options.RetryPolicy)
	if err != nil {
		return nil, err
	}
	config := &scheduleConfiguration{
		OrchestrationName:       options.OrchestrationName,
		ScheduleID:              options.ScheduleID,
		OrchestrationInput:      input,
		OrchestrationInstanceID: options.OrchestrationInstanceID,
		OrchestrationVersion:    options.OrchestrationVersion,
		StartAt:                 options.StartAt.UTC(),
		EndAt:                   options.EndAt.UTC(),
		Interval:                dotNetSpan(options.Interval),
		StartImmediatelyIfLate:  options.StartImmediatelyIfLate,
		Tags:                    cloneStrings(options.Tags),
		ContextFields:           api.ContextFields(cloneStrings(options.ContextFields)),
		RetryPolicy:             retry,
	}
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	return config, nil
}

func applyScheduleUpdate(ctx *task.EntityContext, config *scheduleConfiguration, options ScheduleUpdateOptions) (changed, resetNext bool, err error) {
	changed = updateNonEmptyString(&config.OrchestrationName, options.OrchestrationName) || changed
	changed = updateNonEmptyString(&config.OrchestrationInput, options.OrchestrationInput) || changed
	if options.TypedOrchestrationInput != nil {
		input, inputErr := serializeScheduleInput(ctx, "", options.TypedOrchestrationInput)
		if inputErr != nil {
			return false, false, inputErr
		}
		if input != config.OrchestrationInput {
			config.OrchestrationInput = input
			changed = true
		}
	}
	changed = updateNonEmptyString(&config.OrchestrationInstanceID, options.OrchestrationInstanceID) || changed
	changed = updateNonEmptyString(&config.OrchestrationVersion, options.OrchestrationVersion) || changed
	if updateTime(&config.StartAt, options.StartAt) {
		changed, resetNext = true, true
	}
	if updateTime(&config.EndAt, options.EndAt) {
		changed = true
	}
	if options.Interval != nil && *options.Interval != time.Duration(config.Interval) {
		if err := validateInterval(*options.Interval); err != nil {
			return false, false, err
		}
		config.Interval = dotNetSpan(*options.Interval)
		changed, resetNext = true, true
	}
	if updateComparable(&config.StartImmediatelyIfLate, options.StartImmediatelyIfLate) {
		changed, resetNext = true, true
	}
	if options.Tags != nil && !maps.Equal(options.Tags, config.Tags) {
		if err := validateScheduleKeys("tag", options.Tags); err != nil {
			return false, false, err
		}
		config.Tags = cloneStrings(options.Tags)
		changed = true
	}
	if options.ContextFields != nil && !maps.Equal(options.ContextFields, config.ContextFields) {
		if err := validateScheduleKeys("context field", options.ContextFields); err != nil {
			return false, false, err
		}
		config.ContextFields = api.ContextFields(cloneStrings(options.ContextFields))
		changed = true
	}
	if options.RetryPolicy != nil {
		retry, retryErr := scheduleRetryPolicyFromPublic(options.RetryPolicy)
		if retryErr != nil {
			return false, false, retryErr
		}
		if !reflect.DeepEqual(retry, config.RetryPolicy) {
			config.RetryPolicy = retry
			changed = true
		}
	}
	if err := validateConfig(config); err != nil {
		return false, false, err
	}
	return changed, resetNext, nil
}

func updateNonEmptyString(target *string, value *string) bool {
	if value == nil || *value == "" || *value == *target {
		return false
	}
	*target = *value
	return true
}

func updateTime(target *time.Time, value *time.Time) bool {
	if value == nil || value.Equal(*target) {
		return false
	}
	*target = value.UTC()
	return true
}

func updateComparable[T comparable](target *T, value *T) bool {
	if value == nil || *value == *target {
		return false
	}
	*target = *value
	return true
}

func serializeScheduleInput(ctx *task.EntityContext, raw string, typed any) (string, error) {
	if typed == nil {
		return raw, nil
	}
	return ctx.SerializeInput(typed)
}

func validateCreation(options ScheduleCreationOptions) error {
	if options.ScheduleID == "" {
		return &ScheduleValidationError{Message: "schedule ID is required"}
	}
	if options.OrchestrationName == "" {
		return &ScheduleValidationError{Message: "orchestration name is required"}
	}
	if err := validateInterval(options.Interval); err != nil {
		return err
	}
	if !options.StartAt.IsZero() && !options.EndAt.IsZero() && options.StartAt.After(options.EndAt) {
		return &ScheduleValidationError{Message: "start time cannot be later than end time"}
	}
	if options.RetryPolicy != nil && options.OrchestrationInstanceID != "" {
		return &ScheduleValidationError{
			Message: "a fixed orchestration instance ID cannot be combined with a retry policy",
		}
	}
	if options.RetryPolicy == nil {
		// The timestamp suffix contains no '@', so its value cannot change
		// whether the derived target uses the reserved entity namespace.
		instanceID := scheduledOrchestrationInstanceID(options.ScheduleID, options.OrchestrationInstanceID, options.StartAt)
		if err := helpers.ValidateOrchestrationInstanceID(instanceID); err != nil {
			return &ScheduleValidationError{Message: err.Error()}
		}
	}
	if err := validateScheduleKeys("tag", options.Tags); err != nil {
		return err
	}
	if err := validateScheduleKeys("context field", options.ContextFields); err != nil {
		return err
	}
	if _, err := scheduleRetryPolicyFromPublic(options.RetryPolicy); err != nil {
		return err
	}
	return nil
}

func validateConfig(config *scheduleConfiguration) error {
	return validateCreation(ScheduleCreationOptions{
		ScheduleID:              config.ScheduleID,
		OrchestrationName:       config.OrchestrationName,
		OrchestrationInstanceID: config.OrchestrationInstanceID,
		Interval:                time.Duration(config.Interval),
		StartAt:                 config.StartAt,
		EndAt:                   config.EndAt,
		Tags:                    config.Tags,
		ContextFields:           config.ContextFields,
		RetryPolicy:             config.RetryPolicy.public(),
	})
}

func validateInterval(interval time.Duration) error {
	if interval <= 0 {
		return &ScheduleValidationError{Message: "interval must be positive"}
	}
	if interval < time.Second {
		return &ScheduleValidationError{Message: "interval must be at least one second"}
	}
	return nil
}

func invalidTransition(scheduleID string, from, to ScheduleStatus, operation string) error {
	return &ScheduleInvalidTransitionError{ScheduleID: scheduleID, From: from, To: to, Operation: operation}
}

func scheduleID(state *scheduleState) string {
	if state.ScheduleConfiguration == nil {
		return ""
	}
	return state.ScheduleConfiguration.ScheduleID
}

func scheduleNow(ctx *task.EntityContext) time.Time {
	if now := ctx.CurrentTimeUTC(); !now.IsZero() {
		return now.UTC()
	}
	return time.Now().UTC()
}

func newExecutionToken() string {
	return strings.ReplaceAll(uuid.NewString(), "-", "")
}

// validateScheduleKeys rejects empty keys and reserved wire prefixes.
func validateScheduleKeys(kind string, values map[string]string) error {
	for key := range values {
		if key == "" {
			return &ScheduleValidationError{Message: kind + " key cannot be empty"}
		}
		if strings.HasPrefix(key, api.ReservedContextFieldPrefix) ||
			strings.HasPrefix(key, tagcodec.UserTagPrefix) {
			return &ScheduleValidationError{Message: fmt.Sprintf("%s %q uses a reserved prefix", kind, key)}
		}
	}
	return nil
}

func cloneStrings[M ~map[string]string](values M) map[string]string {
	if len(values) == 0 {
		return nil
	}
	return maps.Clone(values)
}

type scheduledTaskBackend interface {
	ScheduleNewOrchestration(context.Context, string, ...api.NewOrchestrationOptions) (api.InstanceID, error)
	WaitForOrchestrationCompletion(context.Context, api.InstanceID, ...api.FetchOrchestrationMetadataOptions) (*api.OrchestrationMetadata, error)
	GetEntity(context.Context, api.EntityID, ...api.GetEntityOptions) (*api.EntityMetadata, error)
	QueryEntities(context.Context, api.EntityQuery) (*api.EntityQueryResults, error)
}

// ScheduledTaskClient manages all schedules in a DTS task hub.
type ScheduledTaskClient struct {
	client    scheduledTaskBackend
	converter api.DataConverter
}

// NewScheduledTaskClient creates a scheduled-task client with the default JSON
// data converter.
func NewScheduledTaskClient(client *durabletaskclient.TaskHubGrpcClient) *ScheduledTaskClient {
	return NewScheduledTaskClientWithDataConverter(client, nil)
}

// NewScheduledTaskClientWithDataConverter creates a scheduled-task client with
// an explicit data converter.
func NewScheduledTaskClientWithDataConverter(
	client *durabletaskclient.TaskHubGrpcClient,
	converter api.DataConverter,
) *ScheduledTaskClient {
	if client == nil {
		return nil
	}
	return &ScheduledTaskClient{client: client, converter: api.NormalizeDataConverter(converter)}
}

// ScheduledTasks returns a scheduled-task client backed by c.
func (c *Client) ScheduledTasks() *ScheduledTaskClient {
	if c == nil {
		return nil
	}
	return NewScheduledTaskClientWithDataConverter(c.TaskHubGrpcClient, c.converter)
}

// GetScheduleClient returns a handle for scheduleID.
func (c *ScheduledTaskClient) GetScheduleClient(scheduleID string) (*ScheduleClient, error) {
	if c == nil || c.client == nil {
		return nil, fmt.Errorf("scheduled-task client is required")
	}
	if scheduleID == "" {
		return nil, &ScheduleValidationError{Message: "schedule ID is required"}
	}
	return &ScheduleClient{client: c.client, converter: c.converter, scheduleID: scheduleID}, nil
}

// Create creates or replaces a schedule and returns its handle.
func (c *ScheduledTaskClient) Create(ctx context.Context, options ScheduleCreationOptions) (*ScheduleClient, error) {
	handle, err := c.GetScheduleClient(options.ScheduleID)
	if err != nil {
		return nil, err
	}
	if err := handle.Create(ctx, options); err != nil {
		return nil, err
	}
	return handle, nil
}

// Get returns nil, nil if scheduleID has no schedule.
func (c *ScheduledTaskClient) Get(ctx context.Context, scheduleID string) (*ScheduleDescription, error) {
	handle, err := c.GetScheduleClient(scheduleID)
	if err != nil {
		return nil, err
	}
	description, err := handle.Describe(ctx)
	if errors.Is(err, ErrScheduleNotFound) {
		return nil, nil
	}
	return description, err
}

// List returns one page of schedules matching query.
func (c *ScheduledTaskClient) List(ctx context.Context, query ScheduleQuery) (*ScheduleQueryResult, error) {
	if c == nil || c.client == nil {
		return nil, fmt.Errorf("scheduled-task client is required")
	}
	pageSize := query.PageSize
	if pageSize < 0 {
		return nil, &ScheduleValidationError{Message: "page size cannot be negative"}
	}
	if pageSize == 0 {
		pageSize = 100
	}
	if pageSize > api.MaxInstanceQueryPageSize {
		return nil, &ScheduleValidationError{
			Message: fmt.Sprintf("page size cannot exceed %d", api.MaxInstanceQueryPageSize),
		}
	}
	entities, err := c.client.QueryEntities(ctx, api.EntityQuery{
		InstanceIDStartsWith: "@schedule@" + query.ScheduleIDPrefix,
		PageSize:             pageSize,
		ContinuationToken:    query.ContinuationToken,
	})
	if err != nil {
		return nil, err
	}
	if entities == nil {
		return nil, errors.New("schedule query returned no result")
	}
	result := &ScheduleQueryResult{ContinuationToken: entities.ContinuationToken}
	for _, entity := range entities.Entities {
		description, err := scheduleDescription(entity, c.converter)
		if err != nil {
			return nil, err
		}
		if description == nil || !matchesScheduleQuery(description, query) {
			continue
		}
		result.Schedules = append(result.Schedules, description)
	}
	return result, nil
}

// ScheduleClient manages one DTS schedule.
type ScheduleClient struct {
	client     scheduledTaskBackend
	converter  api.DataConverter
	scheduleID string
}

// ID returns the schedule identifier managed by this handle.
func (c *ScheduleClient) ID() string {
	if c == nil {
		return ""
	}
	return c.scheduleID
}

// Create creates or replaces this schedule.
func (c *ScheduleClient) Create(ctx context.Context, options ScheduleCreationOptions) error {
	if c == nil || c.client == nil {
		return fmt.Errorf("schedule client is required")
	}
	if options.ScheduleID != c.scheduleID {
		return &ScheduleValidationError{Message: "creation schedule ID does not match schedule client"}
	}
	return c.operate(ctx, createScheduleOperation, options)
}

// Describe returns the current schedule description.
func (c *ScheduleClient) Describe(ctx context.Context) (*ScheduleDescription, error) {
	if c == nil || c.client == nil {
		return nil, fmt.Errorf("schedule client is required")
	}
	entity, err := c.client.GetEntity(ctx, scheduleEntityID(c.scheduleID))
	if err != nil {
		return nil, err
	}
	if entity == nil {
		return nil, &ScheduleNotFoundError{ScheduleID: c.scheduleID}
	}
	description, err := scheduleDescription(entity, c.converter)
	if err != nil {
		return nil, err
	}
	if description == nil {
		return nil, &ScheduleNotFoundError{ScheduleID: c.scheduleID}
	}
	return description, nil
}

// Delete removes this schedule without affecting orchestration runs already started.
func (c *ScheduleClient) Delete(ctx context.Context) error {
	return c.operate(ctx, deleteScheduleOperation, nil)
}

// Pause stops future executions without affecting runs already started.
func (c *ScheduleClient) Pause(ctx context.Context) error {
	return c.operate(ctx, pauseScheduleOperation, nil)
}

// Resume restarts future executions.
func (c *ScheduleClient) Resume(ctx context.Context) error {
	return c.operate(ctx, resumeScheduleOperation, nil)
}

// Update applies a sparse configuration update.
func (c *ScheduleClient) Update(ctx context.Context, options ScheduleUpdateOptions) error {
	return c.operate(ctx, updateScheduleOperation, options)
}

func (c *ScheduleClient) operate(ctx context.Context, operation string, input any) error {
	if c == nil || c.client == nil {
		return fmt.Errorf("schedule client is required")
	}
	preparedInput, err := prepareScheduleOperationInput(c.converter, input)
	if err != nil {
		return err
	}
	instanceID, err := c.client.ScheduleNewOrchestration(ctx, ExecuteScheduleOperationOrchestratorName,
		api.WithInput(ScheduleOperationRequest{
			EntityID:      scheduleEntityID(c.scheduleID),
			OperationName: operation,
			Input:         preparedInput,
		}),
		// System operations must not inherit application default versions.
		api.WithVersion(""),
	)
	if err != nil {
		return err
	}
	metadata, err := c.client.WaitForOrchestrationCompletion(ctx, instanceID)
	if err != nil {
		return err
	}
	if metadata.RuntimeStatus != api.RUNTIME_STATUS_COMPLETED {
		return scheduleOperationFailure(c.scheduleID, operation, metadata)
	}
	return nil
}

func scheduleEntityID(scheduleID string) api.EntityID {
	return api.NewEntityID(ScheduleEntityName, scheduleID)
}

func scheduleDescription(
	metadata *api.EntityMetadata,
	converter api.DataConverter,
) (*ScheduleDescription, error) {
	if metadata == nil || metadata.SerializedState == "" {
		return nil, nil
	}
	var state scheduleState
	if err := json.Unmarshal([]byte(metadata.SerializedState), &state); err != nil {
		return nil, fmt.Errorf("failed to deserialize schedule state: %w", err)
	}
	description := &ScheduleDescription{
		ScheduleID:     metadata.InstanceID.Key,
		Status:         state.Status,
		ExecutionToken: state.ExecutionToken,
		LastRunAt:      state.LastRunAt,
		NextRunAt:      state.NextRunAt,
		CreatedAt:      state.ScheduleCreatedAt,
		LastModifiedAt: state.ScheduleLastModifiedAt,
		Converter:      api.NormalizeDataConverter(converter),
	}
	if config := state.ScheduleConfiguration; config != nil {
		description.ScheduleID = config.ScheduleID
		description.OrchestrationName = config.OrchestrationName
		description.OrchestrationInput = config.OrchestrationInput
		description.OrchestrationInstanceID = config.OrchestrationInstanceID
		description.OrchestrationVersion = config.OrchestrationVersion
		description.StartAt = config.StartAt
		description.EndAt = config.EndAt
		description.Interval = time.Duration(config.Interval)
		description.StartImmediatelyIfLate = config.StartImmediatelyIfLate
		description.Tags = cloneStrings(config.Tags)
		description.ContextFields = api.ContextFields(cloneStrings(config.ContextFields))
		description.RetryPolicy = config.RetryPolicy.public()
	}
	return description, nil
}

func matchesScheduleQuery(description *ScheduleDescription, query ScheduleQuery) bool {
	if query.Status != nil && description.Status != *query.Status {
		return false
	}
	if !query.CreatedFrom.IsZero() && !description.CreatedAt.After(query.CreatedFrom) {
		return false
	}
	if !query.CreatedTo.IsZero() && !description.CreatedAt.Before(query.CreatedTo) {
		return false
	}
	return true
}

func prepareScheduleOperationInput(converter api.DataConverter, input any) (any, error) {
	switch options := input.(type) {
	case ScheduleCreationOptions:
		if options.TypedOrchestrationInput != nil {
			payload, err := api.SerializeData(converter, options.TypedOrchestrationInput)
			if err != nil {
				return nil, fmt.Errorf("failed to serialize scheduled orchestration input: %w", err)
			}
			options.OrchestrationInput = payload
			options.TypedOrchestrationInput = nil
		}
		return options, nil
	case ScheduleUpdateOptions:
		if options.TypedOrchestrationInput != nil {
			payload, err := api.SerializeData(converter, options.TypedOrchestrationInput)
			if err != nil {
				return nil, fmt.Errorf("failed to serialize scheduled orchestration input: %w", err)
			}
			options.OrchestrationInput = &payload
			options.TypedOrchestrationInput = nil
		}
		return options, nil
	default:
		return input, nil
	}
}

func scheduleOperationFailure(
	scheduleID string,
	operation string,
	metadata *api.OrchestrationMetadata,
) error {
	if metadata != nil {
		if details := findScheduleFailure(metadata.FailureDetails, scheduleInvalidTransitionType); details != nil {
			return &ScheduleInvalidTransitionError{
				ScheduleID: stringProperty(details.Properties, "scheduleId", scheduleID),
				From:       ScheduleStatus(intProperty(details.Properties, "from")),
				To:         ScheduleStatus(intProperty(details.Properties, "to")),
				Operation:  stringProperty(details.Properties, "operation", operation),
			}
		}
		if details := findScheduleFailure(metadata.FailureDetails, scheduleValidationErrorType); details != nil {
			return &ScheduleValidationError{Message: stringProperty(details.Properties, "message", details.ErrorMessage)}
		}
		return &ScheduleOperationError{
			ScheduleID:     scheduleID,
			Operation:      operation,
			RuntimeStatus:  metadata.RuntimeStatus,
			FailureDetails: metadata.FailureDetails,
		}
	}
	return &ScheduleOperationError{ScheduleID: scheduleID, Operation: operation}
}

func findScheduleFailure(details *api.FailureDetails, errorType api.ErrorType) *api.FailureDetails {
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
