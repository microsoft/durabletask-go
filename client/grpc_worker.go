package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"math/rand/v2"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/task"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	ErrTaskHubGrpcWorkerAlreadyRunning = errors.New("gRPC worker is already running")
	errSilentDisconnect                = errors.New("work item stream was silent beyond the configured timeout")
)

// maxWorkerConcurrency is the largest concurrency limit representable on the
// GetWorkItems wire contract, whose fields are 32-bit.
const maxWorkerConcurrency = math.MaxInt32

type workItemsStream interface {
	Recv() (*protos.WorkItem, error)
}

type grpcWorkerClientFactory func(context.Context) (protos.TaskHubSidecarServiceClient, io.Closer, error)

// TaskHubGrpcWorkerConnectionFactory creates a gRPC connection for a worker
// stream generation. A non-nil closer transfers ownership to the worker.
type TaskHubGrpcWorkerConnectionFactory func(context.Context) (grpc.ClientConnInterface, io.Closer, error)

type TaskHubGrpcWorkerOption func(*taskHubGrpcWorkerOptions) error

type WorkerCapability = protos.WorkerCapability

const (
	WorkerCapabilityHistoryStreaming WorkerCapability = protos.WorkerCapability_WORKER_CAPABILITY_HISTORY_STREAMING
	WorkerCapabilityScheduledTasks   WorkerCapability = protos.WorkerCapability_WORKER_CAPABILITY_SCHEDULED_TASKS
	WorkerCapabilityLargePayloads    WorkerCapability = protos.WorkerCapability_WORKER_CAPABILITY_LARGE_PAYLOADS

	// DefaultMaxStreamedHistoryEvents and DefaultMaxStreamedHistoryBytes are
	// the worker's default per-work-item replay-history safety bounds.
	DefaultMaxStreamedHistoryEvents       = api.DefaultHistoryMaxEvents
	DefaultMaxStreamedHistoryBytes  int64 = api.DefaultHistoryMaxBytes
	// MaxStreamedHistoryEvents and MaxStreamedHistoryBytes bound configurable
	// worker replay-history limits.
	MaxStreamedHistoryEvents       = api.MaxHistoryMaxEvents
	MaxStreamedHistoryBytes  int64 = api.MaxHistoryMaxBytes

	// DefaultMaxOrchestratorCompletionBytes leaves headroom below the modern DTS
	// orchestration-completion message limit.
	DefaultMaxOrchestratorCompletionBytes = 4_089_446
	minOrchestratorCompletionBytes        = 64 * 1024
)

type WorkItemFilter struct {
	Name     string
	Versions []string
}

type WorkItemFilters struct {
	Orchestrations          []WorkItemFilter
	Activities              []WorkItemFilter
	Entities                []string
	RejectAllOrchestrations bool
	RejectAllActivities     bool
	RejectAllEntities       bool
}

type taskHubGrpcWorkerOptions struct {
	maxConcurrentOrchestrations    int
	maxConcurrentActivities        int
	maxConcurrentEntities          int
	helloTimeout                   time.Duration
	silentDisconnectTimeout        time.Duration
	rpcTimeout                     time.Duration
	reconnectBaseDelay             time.Duration
	reconnectMaxDelay              time.Duration
	transientRetryMaxAttempts      int
	transientRetryBaseDelay        time.Duration
	transientRetryMaxDelay         time.Duration
	maxStreamedHistoryEvents       int
	maxStreamedHistoryBytes        int64
	maxOrchestratorCompletionBytes int
	maximumTimerInterval           *time.Duration
	taskExecutorOptions            []task.TaskExecutorOption
	versioning                     *task.VersioningOptions
	capabilities                   []WorkerCapability
	workItemFilters                *WorkItemFilters
	workItemFiltersConfigured      bool
	autoWorkItemFilters            bool
	largePayloads                  *api.LargePayloadOptions
	converter                      api.DataConverter
	unversionedOrchestrators       map[string]struct{}
	unversionedActivities          map[string]struct{}
	reconnectRandom                randomInt64N
	// waitFn overrides every delay the worker imposes on itself: reconnect
	// backoff, transient RPC retry backoff, and work-item abandon delays. It is
	// only set by tests so the deterministic delay schedule can be observed
	// without sleeping.
	waitFn func(context.Context, time.Duration) error
}

func defaultTaskHubGrpcWorkerOptions() taskHubGrpcWorkerOptions {
	defaultConcurrency := 100 * runtime.GOMAXPROCS(0)
	return taskHubGrpcWorkerOptions{
		maxConcurrentOrchestrations:    defaultConcurrency,
		maxConcurrentActivities:        defaultConcurrency,
		maxConcurrentEntities:          defaultConcurrency,
		helloTimeout:                   30 * time.Second,
		silentDisconnectTimeout:        2 * time.Minute,
		rpcTimeout:                     30 * time.Second,
		reconnectBaseDelay:             200 * time.Millisecond,
		reconnectMaxDelay:              15 * time.Second,
		transientRetryMaxAttempts:      10,
		transientRetryBaseDelay:        200 * time.Millisecond,
		transientRetryMaxDelay:         15 * time.Second,
		maxStreamedHistoryEvents:       DefaultMaxStreamedHistoryEvents,
		maxStreamedHistoryBytes:        DefaultMaxStreamedHistoryBytes,
		maxOrchestratorCompletionBytes: DefaultMaxOrchestratorCompletionBytes,
		reconnectRandom:                rand.Int64N,
		capabilities:                   []WorkerCapability{WorkerCapabilityHistoryStreaming},
	}
}

func WithMaxConcurrentOrchestrationWorkItems(n int) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		if err := validateWorkerConcurrency("orchestration", n); err != nil {
			return err
		}
		options.maxConcurrentOrchestrations = n
		return nil
	}
}

func WithMaxConcurrentActivityWorkItems(n int) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		if err := validateWorkerConcurrency("activity", n); err != nil {
			return err
		}
		options.maxConcurrentActivities = n
		return nil
	}
}

// WithMaxConcurrentEntityWorkItems limits concurrent entity batch execution.
func WithMaxConcurrentEntityWorkItems(n int) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		if err := validateWorkerConcurrency("entity", n); err != nil {
			return err
		}
		options.maxConcurrentEntities = n
		return nil
	}
}

// WithMaxStreamedHistoryEvents limits the number of history events retained
// while receiving a streamed orchestration history.
func WithMaxStreamedHistoryEvents(n int) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		if n <= 0 || n > MaxStreamedHistoryEvents {
			return fmt.Errorf(
				"maximum streamed history events must be between 1 and %d",
				MaxStreamedHistoryEvents,
			)
		}
		options.maxStreamedHistoryEvents = n
		return nil
	}
}

// WithMaxStreamedHistoryBytes limits the protobuf bytes retained while
// receiving a streamed orchestration history.
func WithMaxStreamedHistoryBytes(n int64) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		if n <= 0 || n > MaxStreamedHistoryBytes {
			return fmt.Errorf(
				"maximum streamed history bytes must be between 1 and %d",
				MaxStreamedHistoryBytes,
			)
		}
		options.maxStreamedHistoryBytes = n
		return nil
	}
}

// WithMaxOrchestratorCompletionBytes lowers the post-externalization response
// bound used before completing an orchestration work item. Multiple options are
// monotonic: a later option cannot raise an earlier transport-derived cap.
func WithMaxOrchestratorCompletionBytes(n int) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		if n < minOrchestratorCompletionBytes || n > DefaultMaxOrchestratorCompletionBytes {
			return fmt.Errorf(
				"maximum orchestrator completion bytes must be between %d and %d",
				minOrchestratorCompletionBytes,
				DefaultMaxOrchestratorCompletionBytes,
			)
		}
		options.maxOrchestratorCompletionBytes = min(options.maxOrchestratorCompletionBytes, n)
		return nil
	}
}

// validateWorkerConcurrency rejects limits that cannot be advertised on the
// 32-bit GetWorkItems fields, which would otherwise silently wrap to a negative
// value on 64-bit platforms.
func validateWorkerConcurrency(kind string, n int) error {
	if n <= 0 {
		return fmt.Errorf("maximum concurrent %s work items must be greater than zero", kind)
	}
	if n > maxWorkerConcurrency {
		return fmt.Errorf(
			"maximum concurrent %s work items must be at most %d",
			kind,
			maxWorkerConcurrency,
		)
	}
	return nil
}

func WithWorkerHelloTimeout(timeout time.Duration) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		if timeout <= 0 {
			return fmt.Errorf("worker Hello timeout must be greater than zero")
		}
		options.helloTimeout = timeout
		return nil
	}
}

func WithWorkerSilentDisconnectTimeout(timeout time.Duration) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		if timeout <= 0 {
			return fmt.Errorf("worker silent disconnect timeout must be greater than zero")
		}
		options.silentDisconnectTimeout = timeout
		return nil
	}
}

func WithWorkerRPCTimeout(timeout time.Duration) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		if timeout <= 0 {
			return fmt.Errorf("worker RPC timeout must be greater than zero")
		}
		options.rpcTimeout = timeout
		return nil
	}
}

// WithWorkerReconnectBackoff configures the reconnect schedule. Nominal delays
// double from baseDelay, receive ±25% jitter, and remain within
// [baseDelay, maxDelay].
func WithWorkerReconnectBackoff(baseDelay, maxDelay time.Duration) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		if baseDelay <= 0 || maxDelay < baseDelay {
			return fmt.Errorf("worker reconnect delays must be positive and max delay must be at least the base delay")
		}
		options.reconnectBaseDelay = baseDelay
		options.reconnectMaxDelay = maxDelay
		return nil
	}
}

// WithWorkerTransientRetryPolicy configures the deterministic retry schedule for
// completion and abandon RPCs. Delays double from baseDelay and are always
// within [baseDelay, maxDelay].
func WithWorkerTransientRetryPolicy(maxAttempts int, baseDelay, maxDelay time.Duration) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		if maxAttempts <= 0 {
			return fmt.Errorf("worker transient retry attempts must be greater than zero")
		}
		if baseDelay <= 0 || maxDelay < baseDelay {
			return fmt.Errorf("worker transient retry delays must be positive and max delay must be at least the base delay")
		}
		options.transientRetryMaxAttempts = maxAttempts
		options.transientRetryBaseDelay = baseDelay
		options.transientRetryMaxDelay = maxDelay
		return nil
	}
}

// WithMaximumTimerInterval configures deterministic splitting of long durable
// timers. Changing this value is replay-breaking for affected in-flight
// orchestrations.
func WithMaximumTimerInterval(interval time.Duration) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		if interval < 0 {
			return fmt.Errorf("maximum timer interval cannot be negative")
		}
		if interval == 0 {
			interval = task.DefaultMaximumTimerInterval
		}
		options.maximumTimerInterval = &interval
		return nil
	}
}

// WithTaskExecutorOptions configures the task executor used by the gRPC worker.
// This allows task options such as task.WithVersioning to participate in DTS dispatch.
func WithTaskExecutorOptions(options ...task.TaskExecutorOption) TaskHubGrpcWorkerOption {
	return func(workerOptions *taskHubGrpcWorkerOptions) error {
		for _, option := range options {
			if option == nil {
				return errors.New("task executor option cannot be nil")
			}
		}
		workerOptions.taskExecutorOptions = append(workerOptions.taskExecutorOptions, options...)
		return nil
	}
}

// WithTaskVersioning configures worker version acceptance and the default
// version used for sub-orchestrations.
func WithTaskVersioning(versioning task.VersioningOptions) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		if err := versioning.Validate(); err != nil {
			return err
		}
		options.versioning = &versioning
		return nil
	}
}

// WithWorkerCapabilities explicitly configures the capabilities advertised to DTS.
func WithWorkerCapabilities(capabilities ...WorkerCapability) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		seen := make(map[WorkerCapability]struct{}, len(capabilities))
		configured := make([]WorkerCapability, 0, len(capabilities))
		for _, capability := range capabilities {
			switch capability {
			case WorkerCapabilityHistoryStreaming, WorkerCapabilityScheduledTasks, WorkerCapabilityLargePayloads:
			default:
				return fmt.Errorf("unsupported worker capability: %d", capability)
			}
			if _, ok := seen[capability]; ok {
				continue
			}
			seen[capability] = struct{}{}
			configured = append(configured, capability)
		}
		options.capabilities = configured
		return nil
	}
}

// WithScheduledTaskCapability controls scheduled-task capability advertisement.
func WithScheduledTaskCapability(enabled bool) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		options.capabilities = setWorkerCapability(options.capabilities, WorkerCapabilityScheduledTasks, enabled)
		return nil
	}
}

// CombineTaskHubGrpcWorkerOptions combines worker options into one option.
func CombineTaskHubGrpcWorkerOptions(options ...TaskHubGrpcWorkerOption) TaskHubGrpcWorkerOption {
	return func(target *taskHubGrpcWorkerOptions) error {
		for _, configure := range options {
			if configure == nil {
				continue
			}
			if err := configure(target); err != nil {
				return err
			}
		}
		return nil
	}
}

// WithUnversionedOrchestratorNames allows explicitly unversioned system
// orchestrators to remain routable when strict worker versioning is enabled.
func WithUnversionedOrchestratorNames(names ...string) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		return addUnversionedTaskNames(&options.unversionedOrchestrators, "orchestrator", names)
	}
}

// WithUnversionedActivityNames allows explicitly unversioned system activities
// to remain routable when strict worker versioning is enabled. System
// orchestrations run unversioned and an activity inherits its caller's version,
// so a system component's activities must be advertised unversioned too.
func WithUnversionedActivityNames(names ...string) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		return addUnversionedTaskNames(&options.unversionedActivities, "activity", names)
	}
}

func addUnversionedTaskNames(
	allowed *map[string]struct{},
	kind string,
	names []string,
) error {
	if *allowed == nil {
		*allowed = make(map[string]struct{}, len(names))
	}
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			return fmt.Errorf("unversioned %s name cannot be empty", kind)
		}
		(*allowed)[strings.ToLower(name)] = struct{}{}
	}
	return nil
}

// WithWorkItemFilters restricts orchestration/activity names and versions and entity names accepted by the worker.
// A nil per-kind list means no restriction for that kind. RejectAll* explicitly rejects a kind.
func WithWorkItemFilters(filters *WorkItemFilters) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		options.workItemFiltersConfigured = true
		if filters == nil {
			options.workItemFilters = nil
			return nil
		}
		clone, err := cloneWorkItemFilters(filters)
		if err != nil {
			return err
		}
		options.workItemFilters = clone
		return nil
	}
}

// WithAutoWorkItemFilters derives task-name and task-version filters from the registry.
// Explicit [WithWorkItemFilters] configuration takes precedence.
func WithAutoWorkItemFilters() TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		options.autoWorkItemFilters = true
		return nil
	}
}

// WithWorkerLargePayloads configures worker payload hydration/externalization and advertises support.
func WithWorkerLargePayloads(options *api.LargePayloadOptions) TaskHubGrpcWorkerOption {
	return func(workerOptions *taskHubGrpcWorkerOptions) error {
		normalized, err := api.NormalizeLargePayloadOptions(options)
		if err != nil {
			return err
		}
		workerOptions.largePayloads = normalized
		workerOptions.capabilities = setWorkerCapability(
			workerOptions.capabilities,
			WorkerCapabilityLargePayloads,
			normalized != nil,
		)
		return nil
	}
}

// WithWorkerDataConverter configures application payload serialization.
func WithWorkerDataConverter(converter api.DataConverter) TaskHubGrpcWorkerOption {
	return func(options *taskHubGrpcWorkerOptions) error {
		options.converter = api.NormalizeDataConverter(converter)
		return nil
	}
}

// TaskHubGrpcWorker executes orchestration, activity, and entity work received from a
// DTS gRPC work-item stream (named TaskHubSidecarService in the wire contract).
type TaskHubGrpcWorker struct {
	clientFactory grpcWorkerClientFactory
	executor      task.Executor
	logger        api.Logger
	options       taskHubGrpcWorkerOptions

	mu      sync.Mutex
	run     *grpcWorkerRun
	lastErr error
}

type grpcWorkerRun struct {
	intakeCtx        context.Context
	cancelIntake     context.CancelFunc
	processingCtx    context.Context
	cancelProcessing context.CancelFunc
	done             chan struct{}

	orchestrationSlots chan struct{}
	activitySlots      chan struct{}
	entitySlots        chan struct{}
	pending            sync.WaitGroup
	retired            sync.WaitGroup
	err                error
}

type grpcWorkerConnection struct {
	client       protos.TaskHubSidecarServiceClient
	stream       workItemsStream
	cancelStream context.CancelFunc
	closer       io.Closer

	pending sync.WaitGroup
}

// NewTaskHubGrpcWorker creates a worker that borrows a caller-owned connection.
//
// The worker never closes or replaces the supplied connection: a reconnect only
// recreates the work-item stream on that same connection. Recovery is therefore
// limited to what the caller's connection can do on its own. A connection that
// is permanently wedged (for example one whose credentials expired, or one whose
// endpoint moved) keeps producing poisoned streams and the worker keeps retrying
// on the escalating reconnect schedule without ever obtaining a fresh channel.
// Use [NewTaskHubGrpcWorkerWithConnectionFactory], or the Durable Task Scheduler
// NewWorker, when the worker should own and recreate its channels.
func NewTaskHubGrpcWorker(
	cc grpc.ClientConnInterface,
	registry *task.TaskRegistry,
	logger api.Logger,
	opts ...TaskHubGrpcWorkerOption,
) (*TaskHubGrpcWorker, error) {
	if cc == nil {
		return nil, fmt.Errorf("gRPC connection is required")
	}
	return newTaskHubGrpcWorker(
		func(context.Context) (protos.TaskHubSidecarServiceClient, io.Closer, error) {
			return protos.NewTaskHubSidecarServiceClient(cc), nil, nil
		},
		registry,
		logger,
		opts...,
	)
}

// NewTaskHubGrpcWorkerWithConnectionFactory creates a worker whose connection
// can be recreated after disconnects. Each returned closer is called only after
// all work dispatched through that connection has completed or been abandoned.
func NewTaskHubGrpcWorkerWithConnectionFactory(
	factory TaskHubGrpcWorkerConnectionFactory,
	registry *task.TaskRegistry,
	logger api.Logger,
	opts ...TaskHubGrpcWorkerOption,
) (*TaskHubGrpcWorker, error) {
	if factory == nil {
		return nil, fmt.Errorf("gRPC worker connection factory is required")
	}
	return newTaskHubGrpcWorker(
		func(ctx context.Context) (protos.TaskHubSidecarServiceClient, io.Closer, error) {
			cc, closer, err := factory(ctx)
			if err != nil {
				return nil, nil, err
			}
			if cc == nil {
				if closer != nil {
					_ = closer.Close()
				}
				return nil, nil, fmt.Errorf("gRPC worker connection factory returned a nil connection")
			}
			return protos.NewTaskHubSidecarServiceClient(cc), closer, nil
		},
		registry,
		logger,
		opts...,
	)
}

func newTaskHubGrpcWorker(
	factory grpcWorkerClientFactory,
	registry *task.TaskRegistry,
	logger api.Logger,
	opts ...TaskHubGrpcWorkerOption,
) (*TaskHubGrpcWorker, error) {
	if factory == nil {
		return nil, fmt.Errorf("gRPC worker client factory is required")
	}
	if registry == nil {
		return nil, fmt.Errorf("task registry is required")
	}
	if logger == nil {
		logger = api.DefaultLogger()
	}

	options := defaultTaskHubGrpcWorkerOptions()
	for _, configure := range opts {
		if configure == nil {
			continue
		}
		if err := configure(&options); err != nil {
			return nil, err
		}
	}
	hasLargePayloadCapability := slices.Contains(options.capabilities, WorkerCapabilityLargePayloads)
	if hasLargePayloadCapability && options.largePayloads == nil {
		return nil, fmt.Errorf("large-payload capability requires worker large-payload options")
	}
	if options.largePayloads != nil {
		options.capabilities = setWorkerCapability(options.capabilities, WorkerCapabilityLargePayloads, true)
	}
	snapshot := registry.Snapshot()
	if options.workItemFiltersConfigured && options.workItemFilters != nil {
		if err := validateWorkItemFilters(options.workItemFilters, snapshot); err != nil {
			return nil, err
		}
	}
	if options.autoWorkItemFilters && !options.workItemFiltersConfigured {
		if err := validateStrictAutoFilters(snapshot, options.versioning); err != nil {
			return nil, err
		}
		options.workItemFilters = workItemFiltersFromRegistry(
			snapshot,
			options.versioning,
			options.unversionedOrchestrators,
			options.unversionedActivities,
		)
	}
	return &TaskHubGrpcWorker{
		clientFactory: factory,
		executor:      task.NewTaskExecutor(registry, options.executorOptions()...),
		logger:        logger,
		options:       options,
	}, nil
}

// executorOptions returns the canonical executor configuration. Dedicated
// worker options are appended last so they consistently override generic ones.
func (options taskHubGrpcWorkerOptions) executorOptions() []task.TaskExecutorOption {
	executorOptions := slices.Clone(options.taskExecutorOptions)
	if options.maximumTimerInterval != nil {
		executorOptions = append(executorOptions, task.WithMaximumTimerInterval(*options.maximumTimerInterval))
	}
	if options.versioning != nil {
		executorOptions = append(executorOptions, task.WithVersioning(*options.versioning))
	}
	if options.converter != nil {
		executorOptions = append(executorOptions, task.WithDataConverter(options.converter))
	}
	if len(options.unversionedOrchestrators) > 0 {
		executorOptions = append(
			executorOptions,
			task.WithUnversionedOrchestratorNames(slices.Collect(maps.Keys(options.unversionedOrchestrators))...),
		)
	}
	if len(options.unversionedActivities) > 0 {
		executorOptions = append(
			executorOptions,
			task.WithUnversionedActivityNames(slices.Collect(maps.Keys(options.unversionedActivities))...),
		)
	}
	return executorOptions
}

func workItemFiltersFromRegistry(
	snapshot task.TaskRegistrySnapshot,
	versioning *task.VersioningOptions,
	allowedUnversionedOrchestrators map[string]struct{},
	allowedUnversionedActivities map[string]struct{},
) *WorkItemFilters {
	entities := slices.Clone(snapshot.Entities)
	if slices.Contains(entities, "*") {
		entities = nil
	}
	orchestrations := taskRegistrationsToFilters(
		snapshot.Orchestrators, versioning, allowedUnversionedOrchestrators)
	activities := taskRegistrationsToFilters(
		snapshot.Activities, versioning, allowedUnversionedActivities)
	return &WorkItemFilters{
		Orchestrations:          orchestrations,
		Activities:              activities,
		Entities:                entities,
		RejectAllOrchestrations: len(snapshot.Orchestrators) == 0,
		RejectAllActivities:     len(snapshot.Activities) == 0,
		RejectAllEntities:       len(snapshot.Entities) == 0,
	}
}

func validateStrictAutoFilters(
	snapshot task.TaskRegistrySnapshot,
	versioning *task.VersioningOptions,
) error {
	if versioning == nil || versioning.MatchStrategy != task.VersionMatchStrict {
		return nil
	}
	if err := validateStrictRegistrations("orchestrator", snapshot.Orchestrators, versioning.Version); err != nil {
		return err
	}
	return validateStrictRegistrations("activity", snapshot.Activities, versioning.Version)
}

func validateStrictRegistrations(
	kind string,
	registrations []task.TaskRegistration,
	version string,
) error {
	type versions struct {
		hasUnversioned bool
		versioned      map[string]struct{}
	}
	byName := make(map[string]*versions)
	for _, registration := range registrations {
		name := strings.ToLower(registration.Name)
		group := byName[name]
		if group == nil {
			group = &versions{versioned: make(map[string]struct{})}
			byName[name] = group
		}
		if registration.Version == "" {
			group.hasUnversioned = true
		} else {
			group.versioned[strings.ToLower(registration.Version)] = struct{}{}
		}
	}
	for name, group := range byName {
		if len(group.versioned) == 0 && group.hasUnversioned {
			continue
		}
		if _, ok := group.versioned[strings.ToLower(version)]; !ok {
			return fmt.Errorf("%s %q has no registration for strict worker version %q", kind, name, version)
		}
	}
	return nil
}

func validateWorkItemFilters(filters *WorkItemFilters, snapshot task.TaskRegistrySnapshot) error {
	if err := validateTaskFilterNames("orchestration", filters.Orchestrations, snapshot.Orchestrators); err != nil {
		return err
	}
	if err := validateTaskFilterNames("activity", filters.Activities, snapshot.Activities); err != nil {
		return err
	}
	if len(snapshot.Entities) == 0 || slices.Contains(snapshot.Entities, "*") {
		return nil
	}
	entityNames := make(map[string]struct{}, len(snapshot.Entities))
	for _, name := range snapshot.Entities {
		entityNames[helpers.ToLowerInvariant(name)] = struct{}{}
	}
	for _, name := range filters.Entities {
		if _, ok := entityNames[helpers.ToLowerInvariant(name)]; !ok {
			return fmt.Errorf("entity work-item filter %q is not registered", name)
		}
	}
	return nil
}

func validateTaskFilterNames(
	kind string,
	filters []WorkItemFilter,
	registrations []task.TaskRegistration,
) error {
	if len(registrations) == 0 {
		return nil
	}
	names := make(map[string]struct{}, len(registrations))
	for _, registration := range registrations {
		if registration.Name == "*" {
			return nil
		}
		names[strings.ToLower(registration.Name)] = struct{}{}
	}
	for _, filter := range filters {
		if _, ok := names[strings.ToLower(filter.Name)]; !ok {
			return fmt.Errorf("%s work-item filter %q is not registered", kind, filter.Name)
		}
	}
	return nil
}

func taskRegistrationsToFilters(
	registrations []task.TaskRegistration,
	versioning *task.VersioningOptions,
	allowedUnversioned map[string]struct{},
) []WorkItemFilter {
	type filterGroup struct {
		name     string
		versions map[string]string
	}
	groups := make(map[string]*filterGroup)
	for _, registration := range registrations {
		if registration.Name == "*" {
			return nil
		}
		normalizedName := strings.ToLower(registration.Name)
		group := groups[normalizedName]
		if group == nil {
			group = &filterGroup{name: registration.Name, versions: make(map[string]string)}
			groups[normalizedName] = group
		}
		group.versions[strings.ToLower(registration.Version)] = registration.Version
	}
	filters := make([]WorkItemFilter, 0, len(groups))
	for _, group := range groups {
		if versioning != nil && versioning.MatchStrategy == task.VersionMatchStrict {
			if _, allowed := allowedUnversioned[strings.ToLower(group.name)]; allowed {
				if unversioned, ok := group.versions[""]; ok {
					filters = append(filters, WorkItemFilter{
						Name:     group.name,
						Versions: []string{unversioned},
					})
					continue
				}
			}
			filters = append(filters, WorkItemFilter{
				Name:     group.name,
				Versions: []string{versioning.Version},
			})
			continue
		}
		versions := make([]string, 0, len(group.versions))
		_, hasUnversioned := group.versions[""]
		for normalized, version := range group.versions {
			if normalized != "" {
				versions = append(versions, version)
			}
		}
		slices.SortFunc(versions, func(left, right string) int {
			return strings.Compare(strings.ToLower(left), strings.ToLower(right))
		})
		if hasUnversioned && len(versions) > 0 {
			versions = append([]string{""}, versions...)
		}
		filters = append(filters, WorkItemFilter{Name: group.name, Versions: versions})
	}
	slices.SortFunc(filters, func(left, right WorkItemFilter) int {
		return strings.Compare(strings.ToLower(left.Name), strings.ToLower(right.Name))
	})
	return filters
}

// Start connects to the service, performs the Hello handshake, and starts the
// worker in the background.
func (w *TaskHubGrpcWorker) Start(ctx context.Context) error {
	run, connection, err := w.begin(ctx)
	if err != nil {
		return err
	}
	go w.execute(run, connection)
	return nil
}

// Run connects to the service, performs the Hello handshake, and blocks until
// the worker stops.
func (w *TaskHubGrpcWorker) Run(ctx context.Context) error {
	run, connection, err := w.begin(ctx)
	if err != nil {
		return err
	}
	w.execute(run, connection)
	return run.err
}

// Shutdown stops intake and waits for in-flight work to finish. If ctx expires,
// in-flight execution and completion RPCs are canceled.
func (w *TaskHubGrpcWorker) Shutdown(ctx context.Context) error {
	w.mu.Lock()
	run := w.run
	if run == nil {
		w.mu.Unlock()
		return nil
	}
	run.cancelIntake()
	w.mu.Unlock()

	select {
	case <-run.done:
		return run.err
	case <-ctx.Done():
		run.cancelProcessing()
		return ctx.Err()
	}
}

// Wait waits for a worker started with Start to stop.
func (w *TaskHubGrpcWorker) Wait(ctx context.Context) error {
	w.mu.Lock()
	run := w.run
	lastErr := w.lastErr
	w.mu.Unlock()
	if run == nil {
		return lastErr
	}

	select {
	case <-run.done:
		return run.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *TaskHubGrpcWorker) Running() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.run != nil
}

func (w *TaskHubGrpcWorker) begin(ctx context.Context) (*grpcWorkerRun, *grpcWorkerConnection, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	w.mu.Lock()
	if w.run != nil {
		w.mu.Unlock()
		return nil, nil, ErrTaskHubGrpcWorkerAlreadyRunning
	}
	intakeCtx, cancelIntake := context.WithCancel(ctx)
	processingCtx, cancelProcessing := context.WithCancel(context.WithoutCancel(ctx))
	run := &grpcWorkerRun{
		intakeCtx:        intakeCtx,
		cancelIntake:     cancelIntake,
		processingCtx:    processingCtx,
		cancelProcessing: cancelProcessing,
		done:             make(chan struct{}),
		orchestrationSlots: make(
			chan struct{},
			w.options.maxConcurrentOrchestrations,
		),
		activitySlots: make(chan struct{}, w.options.maxConcurrentActivities),
		entitySlots:   make(chan struct{}, w.options.maxConcurrentEntities),
	}
	w.run = run
	w.lastErr = nil
	w.mu.Unlock()

	connection, err := w.connect(run.intakeCtx)
	if err != nil {
		run.err = err
		run.cancelIntake()
		run.cancelProcessing()
		close(run.done)
		w.mu.Lock()
		if w.run == run {
			w.run = nil
			w.lastErr = err
		}
		w.mu.Unlock()
		return nil, nil, err
	}
	return run, connection, nil
}

func (w *TaskHubGrpcWorker) execute(run *grpcWorkerRun, connection *grpcWorkerConnection) {
	defer func() {
		run.cancelIntake()
		run.pending.Wait()
		run.cancelProcessing()
		run.retired.Wait()
		if err := w.executor.Shutdown(context.Background()); err != nil && run.err == nil {
			run.err = fmt.Errorf("failed to shut down worker executor: %w", err)
		}
		if run.err != nil {
			w.logger.Errorf("gRPC worker stopped: %v", run.err)
		}

		w.mu.Lock()
		if w.run == run {
			w.run = nil
			w.lastErr = run.err
		}
		close(run.done)
		w.mu.Unlock()
	}()

	run.err = w.runLoop(run, connection)
}

func (w *TaskHubGrpcWorker) runLoop(run *grpcWorkerRun, connection *grpcWorkerConnection) error {
	reconnectBackoff := newWorkerBackoff(
		w.options.reconnectBaseDelay,
		w.options.reconnectMaxDelay,
		w.options.reconnectRandom,
	)
	for {
		observedMessage, err := w.consumeConnection(run, connection)
		w.retireConnection(run, connection)

		if run.intakeCtx.Err() != nil {
			return nil
		}
		if !isTransientWorkerError(err) {
			return fmt.Errorf("work item stream stopped with a non-retryable error: %w", err)
		}
		if code := status.Code(err); code == codes.Unauthenticated || code == codes.PermissionDenied {
			w.logger.Warnf("gRPC worker authentication or authorization failed; reconnecting so refreshed credentials or RBAC can recover: %v", err)
		}
		if observedMessage {
			// The stream proved the endpoint healthy before it ended, so this is
			// a drain rather than a poisoned connection and the escalating
			// reconnect schedule restarts from the base delay.
			reconnectBackoff.Reset()
		} else if errors.Is(err, errSilentDisconnect) {
			// Silence before the first message means the stream was accepted but
			// never produced anything, so keep escalating instead of hammering a
			// wedged endpoint.
			w.logger.Warnf(
				"gRPC work item stream stayed silent for %v before delivering any message; treating it as poisoned",
				w.options.silentDisconnectTimeout,
			)
		}

		for {
			delay := reconnectBackoff.Next()
			if err := w.wait(run.intakeCtx, delay); err != nil {
				return nil
			}

			next, connectErr := w.connect(run.intakeCtx)
			if connectErr == nil {
				w.logger.Info("reconnected gRPC work item stream")
				connection = next
				break
			}
			if run.intakeCtx.Err() != nil {
				return nil
			}
			if !isTransientWorkerError(connectErr) {
				return fmt.Errorf("failed to reconnect gRPC work item stream: %w", connectErr)
			}
			w.logger.Warnf("transient gRPC worker reconnect failure: %v", connectErr)
		}
	}
}

func (w *TaskHubGrpcWorker) connect(ctx context.Context) (*grpcWorkerConnection, error) {
	client, closer, err := w.clientFactory(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC worker connection: %w", err)
	}
	closeOnError := func() {
		if closer != nil {
			_ = closer.Close()
		}
	}

	helloCtx, cancelHello := context.WithTimeout(ctx, w.options.helloTimeout)
	_, err = client.Hello(helloCtx, &emptypb.Empty{})
	cancelHello()
	if err != nil {
		closeOnError()
		return nil, fmt.Errorf("gRPC worker Hello failed: %w", err)
	}

	streamCtx, cancelStream := context.WithCancel(ctx)
	stream, err := client.GetWorkItems(streamCtx, &protos.GetWorkItemsRequest{
		MaxConcurrentOrchestrationWorkItems: int32(w.options.maxConcurrentOrchestrations),
		MaxConcurrentActivityWorkItems:      int32(w.options.maxConcurrentActivities),
		MaxConcurrentEntityWorkItems:        int32(w.options.maxConcurrentEntities),
		Capabilities:                        slices.Clone(w.options.capabilities),
		WorkItemFilters:                     workItemFiltersToProto(w.options.workItemFilters),
	})
	if err != nil {
		cancelStream()
		closeOnError()
		return nil, fmt.Errorf("failed to open gRPC work item stream: %w", err)
	}

	return &grpcWorkerConnection{
		client:       client,
		stream:       stream,
		cancelStream: cancelStream,
		closer:       closer,
	}, nil
}

func setWorkerCapability(capabilities []WorkerCapability, capability WorkerCapability, enabled bool) []WorkerCapability {
	index := slices.Index(capabilities, capability)
	switch {
	case enabled && index < 0:
		return append(capabilities, capability)
	case !enabled && index >= 0:
		return slices.Delete(capabilities, index, index+1)
	default:
		return capabilities
	}
}

func cloneWorkItemFilters(filters *WorkItemFilters) (*WorkItemFilters, error) {
	result := &WorkItemFilters{
		RejectAllOrchestrations: filters.RejectAllOrchestrations,
		RejectAllActivities:     filters.RejectAllActivities,
		RejectAllEntities:       filters.RejectAllEntities,
	}
	var err error
	if result.Orchestrations, err = cloneNamedFilters(
		"orchestration", "RejectAllOrchestrations", filters.RejectAllOrchestrations, filters.Orchestrations,
	); err != nil {
		return nil, err
	}
	if result.Activities, err = cloneNamedFilters(
		"activity", "RejectAllActivities", filters.RejectAllActivities, filters.Activities,
	); err != nil {
		return nil, err
	}
	if result.Entities, err = cloneEntityFilters(filters.RejectAllEntities, filters.Entities); err != nil {
		return nil, err
	}
	return result, nil
}

// cloneNamedFilters validates one kind of named filter and returns a sorted deep
// copy with sorted version sets, so the advertised filter set is deterministic.
func cloneNamedFilters(
	kind string,
	rejectAllField string,
	rejectAll bool,
	filters []WorkItemFilter,
) ([]WorkItemFilter, error) {
	if rejectAll && len(filters) > 0 {
		return nil, fmt.Errorf("%s filters cannot be combined with %s", kind, rejectAllField)
	}
	if filters == nil {
		return nil, nil
	}
	cloned := make([]WorkItemFilter, len(filters))
	for i, filter := range filters {
		if filter.Name == "" {
			return nil, fmt.Errorf("%s filter name cannot be empty", kind)
		}
		versions := slices.Clone(filter.Versions)
		slices.Sort(versions)
		cloned[i] = WorkItemFilter{Name: filter.Name, Versions: versions}
	}
	slices.SortFunc(cloned, func(left, right WorkItemFilter) int {
		return strings.Compare(left.Name, right.Name)
	})
	if err := rejectDuplicateFilterNames(kind, cloned); err != nil {
		return nil, err
	}
	return cloned, nil
}

// cloneEntityFilters validates entity filter names and returns them lowercased
// and sorted, which is the case-insensitive form the service is given.
func cloneEntityFilters(rejectAll bool, names []string) ([]string, error) {
	if rejectAll && len(names) > 0 {
		return nil, errors.New("entity filters cannot be combined with RejectAllEntities")
	}
	if names == nil {
		return nil, nil
	}
	cloned := make([]string, len(names))
	for i, name := range names {
		normalized := helpers.ToLowerInvariant(strings.TrimSpace(name))
		if normalized == "" {
			return nil, errors.New("entity filter name cannot be empty")
		}
		cloned[i] = normalized
	}
	slices.Sort(cloned)
	for i := 1; i < len(cloned); i++ {
		if cloned[i] == cloned[i-1] {
			return nil, fmt.Errorf("entity work-item filter %q is declared more than once", cloned[i])
		}
	}
	return cloned, nil
}

// rejectDuplicateFilterNames reports names declared more than once, which would
// otherwise make the effective version set silently depend on iteration order.
func rejectDuplicateFilterNames(kind string, filters []WorkItemFilter) error {
	seen := make(map[string]struct{}, len(filters))
	for _, filter := range filters {
		normalized := strings.ToLower(filter.Name)
		if _, ok := seen[normalized]; ok {
			return fmt.Errorf("%s work-item filter %q is declared more than once", kind, filter.Name)
		}
		seen[normalized] = struct{}{}
	}
	return nil
}

func workItemFiltersToProto(filters *WorkItemFilters) *protos.WorkItemFilters {
	if filters == nil {
		return nil
	}
	result := &protos.WorkItemFilters{
		Orchestrations: make([]*protos.OrchestrationFilter, 0, len(filters.Orchestrations)),
		Activities:     make([]*protos.ActivityFilter, 0, len(filters.Activities)),
		Entities:       make([]*protos.EntityFilter, 0, len(filters.Entities)),
	}
	if filters.RejectAllOrchestrations {
		result.Orchestrations = append(result.Orchestrations, &protos.OrchestrationFilter{
			Name: helpers.RejectAllWorkItemFilterName,
		})
	} else {
		for _, filter := range filters.Orchestrations {
			result.Orchestrations = append(result.Orchestrations, &protos.OrchestrationFilter{
				Name:     filter.Name,
				Versions: slices.Clone(filter.Versions),
			})
		}
	}
	if filters.RejectAllActivities {
		result.Activities = append(result.Activities, &protos.ActivityFilter{
			Name: helpers.RejectAllWorkItemFilterName,
		})
	} else {
		for _, filter := range filters.Activities {
			result.Activities = append(result.Activities, &protos.ActivityFilter{
				Name:     filter.Name,
				Versions: slices.Clone(filter.Versions),
			})
		}
	}
	if filters.RejectAllEntities {
		result.Entities = append(result.Entities, &protos.EntityFilter{
			Name: helpers.RejectAllWorkItemFilterName,
		})
	} else {
		for _, name := range filters.Entities {
			result.Entities = append(result.Entities, &protos.EntityFilter{Name: name})
		}
	}
	return result
}

func (w *TaskHubGrpcWorker) retireConnection(run *grpcWorkerRun, connection *grpcWorkerConnection) {
	connection.cancelStream()
	run.retired.Add(1)
	go func() {
		defer run.retired.Done()
		connection.pending.Wait()
		if connection.closer != nil {
			if err := connection.closer.Close(); err != nil {
				w.logger.Warnf("failed to close retired gRPC worker connection: %v", err)
			}
		}
	}()
}

func isTransientWorkerError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrStreamedHistoryLimitExceeded) {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, errSilentDisconnect) {
		return true
	}
	grpcStatus, ok := status.FromError(err)
	if !ok {
		return false
	}
	if grpcStatus.Code() == codes.Canceled &&
		strings.Contains(grpcStatus.Message(), "client connection is closing") {
		return false
	}
	return isTransientWorkerGRPCStatus(grpcStatus)
}

func isTransientWorkerGRPCStatus(grpcStatus *status.Status) bool {
	if grpcStatus.Code() == codes.ResourceExhausted {
		message := grpcStatus.Message()
		if strings.Contains(message, "received message larger than max") ||
			strings.Contains(message, "trying to send message larger than max") {
			return false
		}
	}
	return isTransientWorkerGRPCCode(grpcStatus.Code())
}

func isTransientWorkerGRPCCode(code codes.Code) bool {
	switch code {
	case codes.Canceled,
		codes.DeadlineExceeded,
		codes.NotFound,
		codes.Unauthenticated,
		codes.PermissionDenied,
		codes.ResourceExhausted,
		codes.Aborted,
		codes.Internal,
		codes.Unavailable,
		codes.Unknown:
		return true
	default:
		return false
	}
}

// wait blocks for the given delay, honoring the test wait seam when one is
// configured. Every worker-imposed delay must go through it so no code path
// sleeps for real under test.
func (w *TaskHubGrpcWorker) wait(ctx context.Context, delay time.Duration) error {
	if w.options.waitFn != nil {
		return w.options.waitFn(ctx, delay)
	}
	return waitForRetry(ctx, delay)
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		// A zero timer fires immediately and would race with cancellation, so
		// observe the context first and keep shutdown deterministic.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type randomInt64N func(int64) int64

// workerBackoff is a bounded, jittered exponential backoff. Every delay it
// produces stays within [base, max], and doubling saturates instead of
// overflowing for very large maximums.
type workerBackoff struct {
	base    time.Duration
	max     time.Duration
	current time.Duration
	random  randomInt64N
}

func newWorkerBackoff(base, max time.Duration, random randomInt64N) *workerBackoff {
	if base <= 0 {
		base = time.Millisecond
	}
	if max < base {
		max = base
	}
	return &workerBackoff{base: base, max: max, current: base, random: random}
}

// Reset restarts the schedule at the base delay.
func (b *workerBackoff) Reset() {
	b.current = b.base
}

// Next returns the current delay and advances the schedule.
func (b *workerBackoff) Next() time.Duration {
	delay := b.current
	b.current = doubleDurationBounded(b.current, b.max)
	if b.random != nil {
		jitter := delay / 4
		lower := max(b.base, delay-jitter)
		upper := b.max
		if delay <= time.Duration(math.MaxInt64)-jitter {
			upper = min(b.max, delay+jitter)
		}
		if width := upper - lower; width > 0 {
			delay = lower + time.Duration(b.random(int64(width)+1))
		}
	}
	return delay
}

// doubleDurationBounded doubles delay without overflowing int64 and clamps the
// result to max.
func doubleDurationBounded(delay, max time.Duration) time.Duration {
	if delay >= max || delay > math.MaxInt64/2 {
		return max
	}
	if doubled := delay * 2; doubled < max {
		return doubled
	}
	return max
}
