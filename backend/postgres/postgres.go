package postgres

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/proto"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

var emptyString string = ""

// maxRowsPerInsert caps the number of rows per multi-row INSERT to keep the
// generated SQL well below PostgreSQL's parameter and query-size limits.
const maxRowsPerInsert = 1000

type PostgresOptions struct {
	PgOptions                *pgxpool.Config
	OrchestrationLockTimeout time.Duration
	ActivityLockTimeout      time.Duration
}

type postgresBackend struct {
	db         *pgxpool.Pool
	workerName string
	logger     backend.Logger
	options    *PostgresOptions
}

// NewPostgresOptions creates a new options object for the postgres backend provider.
func NewPostgresOptions(host string, port uint16, database string, user string, password string) *PostgresOptions {
	conf, err := pgxpool.ParseConfig(fmt.Sprintf("postgresql://%s:%s@%s:%d/%s", user, password, host, port, database))
	if err != nil {
		panic(fmt.Errorf("failed to parse the postgres connection string: %w", err))
	}
	conf.ConnConfig.ConnectTimeout = 2 * time.Minute
	conf.MaxConnLifetime = 2 * time.Minute
	conf.MaxConnIdleTime = 2 * time.Minute
	conf.MaxConns = 1

	return &PostgresOptions{
		PgOptions:                conf,
		OrchestrationLockTimeout: 2 * time.Minute,
		ActivityLockTimeout:      2 * time.Minute,
	}
}

// NewPostgresBackend creates a new postgres-based Backend object.
func NewPostgresBackend(opts *PostgresOptions, logger backend.Logger) backend.Backend {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	pid := os.Getpid()
	u, err := uuid.NewV7()
	if err != nil {
		u = uuid.New()
	}
	uuidStr := u.String()

	if opts == nil {
		opts = NewPostgresOptions("localhost", 5432, "postgres", "postgres", "postgres")
	}

	return &postgresBackend{
		db:         nil,
		workerName: fmt.Sprintf("%s,%d,%s", hostname, pid, uuidStr),
		options:    opts,
		logger:     logger,
	}
}

// CreateTaskHub creates the postgres database and applies the schema
func (be *postgresBackend) CreateTaskHub(ctx context.Context) error {
	if err := be.Start(ctx); err != nil {
		be.logger.Error("CreateTaskHub", "failed to start the backend", err)
		return fmt.Errorf("failed to start the backend: %w", err)
	}

	// Initialize database
	if _, err := be.db.Exec(ctx, schema); err != nil {
		be.logger.Error("CreateTaskHub", "failed to initialize the database", err)
		return fmt.Errorf("failed to initialize the database: %w", err)
	}

	return nil
}

func (be *postgresBackend) DeleteTaskHub(ctx context.Context) error {
	if be.db == nil {
		return nil
	}

	_, err := be.db.Exec(ctx, "DROP TABLE IF EXISTS Instances CASCADE")
	if err != nil {
		be.logger.Error("DeleteTaskHub", "failed to drop Instances table", err)
		return fmt.Errorf("failed to drop Instances table: %w", err)
	}
	_, err = be.db.Exec(ctx, "DROP TABLE IF EXISTS History CASCADE")
	if err != nil {
		be.logger.Error("DeleteTaskHub", "failed to drop History table", err)
		return fmt.Errorf("failed to drop History table: %w", err)
	}
	_, err = be.db.Exec(ctx, "DROP TABLE IF EXISTS NewEvents CASCADE")
	if err != nil {
		be.logger.Error("DeleteTaskHub", "failed to drop NewEvents table", err)
		return fmt.Errorf("failed to drop NewEvents table: %w", err)
	}
	_, err = be.db.Exec(ctx, "DROP TABLE IF EXISTS NewTasks CASCADE")
	if err != nil {
		be.logger.Error("DeleteTaskHub", "failed to drop NewTasks table", err)
		return fmt.Errorf("failed to drop NewTasks table: %w", err)
	}

	if err := be.Stop(ctx); err != nil {
		be.logger.Error("DeleteTaskHub", "failed to stop the backend", err)
		return fmt.Errorf("failed to stop the backend: %w", err)
	}

	return nil
}

// AbandonOrchestrationWorkItem implements backend.Backend
func (be *postgresBackend) AbandonOrchestrationWorkItem(ctx context.Context, wi *backend.OrchestrationWorkItem) error {
	if err := be.ensureDB(); err != nil {
		return err
	}

	tx, err := be.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	var visibleTime *time.Time = nil
	if delay := wi.GetAbandonDelay(); delay > 0 {
		t := time.Now().UTC().Add(delay)
		visibleTime = &t
	}

	// Verify the orchestration lease is still held before touching NewEvents.
	dbResult, err := tx.Exec(
		ctx,
		"UPDATE Instances SET LockedBy = NULL, LockExpiration = NULL WHERE InstanceID = $1 AND LockedBy = $2",
		string(wi.InstanceID),
		wi.LockedBy,
	)
	if err != nil {
		return fmt.Errorf("failed to update Instances table: %w", err)
	}

	rowsAffected := dbResult.RowsAffected()
	if rowsAffected == 0 {
		return backend.ErrWorkItemLockLost
	}

	// Delay reprocessing of the abandoned events, if requested.
	if len(wi.NewEventSequenceNumbers) > 0 {
		_, err = tx.Exec(
			ctx,
			"UPDATE NewEvents SET VisibleTime = $1 WHERE InstanceID = $2 AND SequenceNumber = ANY($3::bigint[])",
			visibleTime,
			string(wi.InstanceID),
			wi.NewEventSequenceNumbers,
		)
		if err != nil {
			return fmt.Errorf("failed to update NewEvents table: %w", err)
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// CompleteOrchestrationWorkItem implements backend.Backend
func (be *postgresBackend) CompleteOrchestrationWorkItem(ctx context.Context, wi *backend.OrchestrationWorkItem) error {
	const updateInstancesSQL = `
		UPDATE Instances SET
			CreatedTime = COALESCE($1::timestamp, CreatedTime),
			Input = COALESCE($2::text, Input),
			CompletedTime = COALESCE($3::timestamp, CompletedTime),
			Output = COALESCE($4::text, Output),
			FailureDetails = COALESCE($5::bytea, FailureDetails),
			CustomStatus = COALESCE($6::text, CustomStatus),
			RuntimeStatus = $7,
			LastUpdatedTime = $8::timestamp,
			LockExpiration = NULL
		WHERE InstanceID = $9 AND LockedBy = $10
	`

	if err := be.ensureDB(); err != nil {
		return err
	}

	tx, err := be.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	now := time.Now().UTC()

	// Update the Instances table with a fixed-shape statement.
	runtimeStatus := helpers.ToRuntimeStatusString(wi.State.RuntimeStatus())
	updateArgs := []any{
		(*time.Time)(nil), // CreatedTime
		(*string)(nil),    // Input
		(*time.Time)(nil), // CompletedTime
		(*string)(nil),    // Output
		([]byte)(nil),     // FailureDetails
		(*string)(nil),    // CustomStatus
		runtimeStatus,
		now,
		string(wi.InstanceID),
		wi.LockedBy,
	}

	isCreated := false
	isCompleted := false
	for _, e := range wi.State.NewEvents() {
		if es := e.GetExecutionStarted(); es != nil {
			if isCreated {
				// TODO: Log warning about duplicate start event
				continue
			}
			isCreated = true
			created := e.Timestamp.AsTime()
			input := es.Input.GetValue()
			updateArgs[0] = &created
			updateArgs[1] = &input
		} else if ec := e.GetExecutionCompleted(); ec != nil {
			if isCompleted {
				// TODO: Log warning about duplicate completion event
				continue
			}
			isCompleted = true
			completed := now
			output := ec.Result.GetValue()
			updateArgs[2] = &completed
			updateArgs[3] = &output
			if ec.FailureDetails != nil {
				failureDetails, err := proto.Marshal(ec.FailureDetails)
				if err != nil {
					return fmt.Errorf("failed to marshal FailureDetails: %w", err)
				}
				updateArgs[4] = failureDetails
			}
		}
		// TODO: Execution suspended & resumed
	}

	if wi.State.CustomStatus != nil {
		customStatus := wi.State.CustomStatus.Value
		updateArgs[5] = &customStatus
	}

	result, err := tx.Exec(ctx, updateInstancesSQL, updateArgs...)
	if err != nil {
		return fmt.Errorf("failed to update Instances table: %w", err)
	}

	count := result.RowsAffected()
	if count == 0 {
		return fmt.Errorf("instance '%s' no longer exists or was locked by a different worker", string(wi.InstanceID))
	}

	// Delete the exact set of inbound events acquired during dequeue.
	if len(wi.NewEventSequenceNumbers) > 0 {
		dbResult, err := tx.Exec(
			ctx,
			"DELETE FROM NewEvents WHERE InstanceID = $1 AND SequenceNumber = ANY($2::bigint[])",
			string(wi.InstanceID),
			wi.NewEventSequenceNumbers,
		)
		if err != nil {
			return fmt.Errorf("failed to delete from NewEvents table: %w", err)
		}

		rowsAffected := dbResult.RowsAffected()
		if rowsAffected < int64(len(wi.NewEventSequenceNumbers)) {
			return backend.ErrWorkItemLockLost
		}
	}

	// Create any sub-orchestration instances first, before batching the outbound events.
	pendingTimers := wi.State.PendingTimers()
	pendingMessages := wi.State.PendingMessages()
	for _, msg := range pendingMessages {
		if es := msg.HistoryEvent.GetExecutionStarted(); es != nil {
			if _, err := be.createOrchestrationInstanceInternal(ctx, msg.HistoryEvent, tx, backend.WithOrchestrationIdReusePolicy(&protos.OrchestrationIdReusePolicy{
				OperationStatus: []protos.OrchestrationStatus{protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED},
				Action:          api.REUSE_ID_ACTION_TERMINATE,
			})); err != nil {
				if errors.Is(err, backend.ErrDuplicateEvent) {
					be.logger.Warnf(
						"%v: dropping sub-orchestration creation event because an instance with the target ID (%v) already exists.",
						wi.InstanceID,
						es.OrchestrationInstance.InstanceId)
				} else {
					return err
				}
			}
		}
	}

	// Save outbound orchestrator events
	newEventCount := len(pendingTimers) + len(pendingMessages)
	if newEventCount > 0 {
		const insertNewEventsSQL = "INSERT INTO NewEvents (InstanceID, EventPayload, VisibleTime) VALUES "
		instanceID := string(wi.InstanceID)

		type newEvent struct {
			instanceID string
			payload    []byte
			visible    any
		}
		events := make([]newEvent, 0, newEventCount)
		for _, e := range pendingTimers {
			eventPayload, err := backend.MarshalHistoryEvent(e)
			if err != nil {
				return err
			}
			visibileTime := e.GetTimerFired().GetFireAt().AsTime()
			events = append(events, newEvent{instanceID, eventPayload, visibileTime})
		}
		for _, msg := range pendingMessages {
			eventPayload, err := backend.MarshalHistoryEvent(msg.HistoryEvent)
			if err != nil {
				return err
			}
			events = append(events, newEvent{msg.TargetInstanceID, eventPayload, nil})
		}

		for start := 0; start < newEventCount; start += maxRowsPerInsert {
			n := newEventCount - start
			if n > maxRowsPerInsert {
				n = maxRowsPerInsert
			}

			query := insertNewEventsSQL + multiRowPlaceholders(n, 3)
			args := make([]any, 0, n*3)
			for i := 0; i < n; i++ {
				ev := events[start+i]
				args = append(args, ev.instanceID, ev.payload, ev.visible)
			}

			_, err = tx.Exec(ctx, query, args...)
			if err != nil {
				return fmt.Errorf("failed to insert into the NewEvents table: %w", err)
			}
		}
	}

	// Save outbound activity tasks
	pendingTasks := wi.State.PendingTasks()
	newActivityCount := len(pendingTasks)
	if newActivityCount > 0 {
		const insertNewTasksSQL = "INSERT INTO NewTasks (InstanceID, EventPayload) VALUES "
		instanceID := string(wi.InstanceID)

		for start := 0; start < newActivityCount; start += maxRowsPerInsert {
			n := newActivityCount - start
			if n > maxRowsPerInsert {
				n = maxRowsPerInsert
			}

			query := insertNewTasksSQL + multiRowPlaceholders(n, 2)
			args := make([]any, 0, n*2)
			for i := 0; i < n; i++ {
				e := pendingTasks[start+i]
				eventPayload, err := backend.MarshalHistoryEvent(e)
				if err != nil {
					return err
				}
				args = append(args, instanceID, eventPayload)
			}

			_, err = tx.Exec(ctx, query, args...)
			if err != nil {
				return fmt.Errorf("failed to insert into the NewTasks table: %w", err)
			}
		}
	}

	// If continue-as-new, delete all existing history before appending the new events.
	if wi.State.ContinuedAsNew() {
		if _, err := tx.Exec(ctx, "DELETE FROM History WHERE InstanceID = $1", string(wi.InstanceID)); err != nil {
			return fmt.Errorf("failed to delete from History table: %w", err)
		}
	}

	// Save new history events
	newHistoryCount := len(wi.State.NewEvents())
	if newHistoryCount > 0 {
		const insertHistorySQL = "INSERT INTO History (InstanceID, SequenceNumber, EventPayload) VALUES "
		nextSequenceNumber := int64(len(wi.State.OldEvents()))
		instanceID := string(wi.InstanceID)

		for start := 0; start < newHistoryCount; start += maxRowsPerInsert {
			n := newHistoryCount - start
			if n > maxRowsPerInsert {
				n = maxRowsPerInsert
			}

			query := insertHistorySQL + multiRowPlaceholders(n, 3)
			args := make([]any, 0, n*3)
			for i := 0; i < n; i++ {
				e := wi.State.NewEvents()[start+i]
				eventPayload, err := backend.MarshalHistoryEvent(e)
				if err != nil {
					return err
				}
				args = append(args, instanceID, nextSequenceNumber+int64(i), eventPayload)
			}

			_, err = tx.Exec(ctx, query, args...)
			if err != nil {
				return fmt.Errorf("failed to insert into the History table: %w", err)
			}
			nextSequenceNumber += int64(n)
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// CreateOrchestrationInstance implements backend.Backend
func (be *postgresBackend) CreateOrchestrationInstance(ctx context.Context, e *backend.HistoryEvent, opts ...backend.OrchestrationIdReusePolicyOptions) error {
	if err := be.ensureDB(); err != nil {
		return err
	}

	tx, err := be.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	var instanceID string
	if instanceID, err = be.createOrchestrationInstanceInternal(ctx, e, tx, opts...); errors.Is(err, api.ErrIgnoreInstance) {
		// choose to ignore, do nothing
		return nil
	} else if err != nil {
		return err
	}

	eventPayload, err := backend.MarshalHistoryEvent(e)
	if err != nil {
		return err
	}

	_, err = tx.Exec(
		ctx,
		`INSERT INTO NewEvents (InstanceID, EventPayload) VALUES ($1, $2)`,
		instanceID,
		eventPayload,
	)

	if err != nil {
		return fmt.Errorf("failed to insert row into NewEvents table: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to create orchestration: %w", err)
	}

	return nil
}

func (be *postgresBackend) createOrchestrationInstanceInternal(ctx context.Context, e *backend.HistoryEvent, tx pgx.Tx, opts ...backend.OrchestrationIdReusePolicyOptions) (string, error) {
	if e == nil {
		return "", backend.ErrNilHistoryEvent
	} else if e.Timestamp == nil {
		return "", backend.ErrNilEventTimestamp
	}

	startEvent := e.GetExecutionStarted()
	if startEvent == nil {
		return "", backend.ErrNotExecutionStarted
	}
	instanceID := startEvent.OrchestrationInstance.InstanceId

	policy := &protos.OrchestrationIdReusePolicy{}

	for _, opt := range opts {
		if err := opt(policy); err != nil {
			return "", err
		}
	}

	rows, err := insertOrIgnoreInstanceTableInternal(ctx, tx, e, startEvent)
	if err != nil {
		return "", err
	}

	// instance with same ID already exists
	if rows <= 0 {
		return instanceID, be.handleInstanceExists(ctx, tx, startEvent, policy, e)
	}
	return instanceID, nil
}

func insertOrIgnoreInstanceTableInternal(ctx context.Context, tx pgx.Tx, e *backend.HistoryEvent, startEvent *protos.ExecutionStartedEvent) (int64, error) {
	var parentInstanceID *string
	if pi := startEvent.GetParentInstance(); pi != nil {
		if instanceID := pi.GetOrchestrationInstance().GetInstanceId(); instanceID != "" {
			parentInstanceID = &instanceID
		}
	}
	res, err := tx.Exec(
		ctx,
		`INSERT INTO Instances (
			Name,
			Version,
			InstanceID,
			ExecutionID,
			Input,
			RuntimeStatus,
			CreatedTime,
			ParentInstanceID
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING`,
		startEvent.Name,
		startEvent.Version.GetValue(),
		startEvent.OrchestrationInstance.InstanceId,
		startEvent.OrchestrationInstance.ExecutionId.GetValue(),
		startEvent.Input.GetValue(),
		"PENDING",
		e.Timestamp.AsTime(),
		parentInstanceID,
	)
	if err != nil {
		return -1, fmt.Errorf("failed to insert into Instances table: %w", err)
	}

	rows := res.RowsAffected()
	if err != nil {
		return -1, fmt.Errorf("failed to count the rows affected: %w", err)
	}
	return rows, nil
}

func (be *postgresBackend) handleInstanceExists(ctx context.Context, tx pgx.Tx, startEvent *protos.ExecutionStartedEvent, policy *protos.OrchestrationIdReusePolicy, e *backend.HistoryEvent) error {
	// query RuntimeStatus for the existing instance
	queryRow := tx.QueryRow(
		ctx,
		`SELECT RuntimeStatus FROM Instances WHERE InstanceID = $1`,
		startEvent.OrchestrationInstance.InstanceId,
	)
	var runtimeStatus *string
	err := queryRow.Scan(&runtimeStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return api.ErrInstanceNotFound
	} else if err != nil {
		return fmt.Errorf("failed to scan the Instances table result: %w", err)
	}

	// status not match, return instance duplicate error
	if !isStatusMatch(policy.OperationStatus, helpers.FromRuntimeStatusString(*runtimeStatus)) {
		return api.ErrDuplicateInstance
	}

	// status match
	switch policy.Action {
	case protos.CreateOrchestrationAction_IGNORE:
		// Log an warning message and ignore creating new instance
		be.logger.Warnf("An instance with ID '%s' already exists; dropping duplicate create request", startEvent.OrchestrationInstance.InstanceId)
		return api.ErrIgnoreInstance
	case protos.CreateOrchestrationAction_TERMINATE:
		// terminate existing instance
		if err := be.cleanupOrchestrationStateInternal(ctx, tx, api.InstanceID(startEvent.OrchestrationInstance.InstanceId), false); err != nil {
			return fmt.Errorf("failed to cleanup orchestration status: %w", err)
		}
		// create a new instance
		var rows int64
		if rows, err = insertOrIgnoreInstanceTableInternal(ctx, tx, e, startEvent); err != nil {
			return err
		}

		// should never happen, because we clean up instance before create new one
		if rows <= 0 {
			return fmt.Errorf("failed to insert into Instances table because entry already exists")
		}
		return nil
	}
	// default behavior
	return api.ErrDuplicateInstance
}

func isStatusMatch(statuses []protos.OrchestrationStatus, runtimeStatus protos.OrchestrationStatus) bool {
	for _, status := range statuses {
		if status == runtimeStatus {
			return true
		}
	}
	return false
}

func (be *postgresBackend) cleanupOrchestrationStateInternal(ctx context.Context, tx pgx.Tx, id api.InstanceID, requireCompleted bool) error {
	row := tx.QueryRow(ctx, "SELECT 1 FROM Instances WHERE InstanceID = $1", string(id))
	var unused int
	if err := row.Scan(&unused); errors.Is(err, pgx.ErrNoRows) {
		return api.ErrInstanceNotFound
	} else if err != nil {
		return fmt.Errorf("failed to scan instance existence: %w", err)
	}

	if requireCompleted {
		// purge orchestration in ['COMPLETED', 'FAILED', 'TERMINATED']
		dbResult, err := tx.Exec(ctx, "DELETE FROM Instances WHERE InstanceID = $1 AND RuntimeStatus IN ('COMPLETED', 'FAILED', 'TERMINATED')", string(id))
		if err != nil {
			return fmt.Errorf("failed to delete from the Instances table: %w", err)
		}

		rowsAffected := dbResult.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected in Instances delete operation: %w", err)
		}
		if rowsAffected == 0 {
			return api.ErrNotCompleted
		}
	} else {
		// clean up orchestration in all RuntimeStatus
		_, err := tx.Exec(ctx, "DELETE FROM Instances WHERE InstanceID = $1", string(id))
		if err != nil {
			return fmt.Errorf("failed to delete from the Instances table: %w", err)
		}
	}

	_, err := tx.Exec(ctx, "DELETE FROM NewEvents WHERE InstanceID = $1", string(id))
	if err != nil {
		return fmt.Errorf("failed to delete from NewEvents table: %w", err)
	}

	_, err = tx.Exec(ctx, "DELETE FROM NewTasks WHERE InstanceID = $1", string(id))
	if err != nil {
		return fmt.Errorf("failed to delete from NewTasks table: %w", err)
	}

	_, err = tx.Exec(ctx, "DELETE FROM History WHERE InstanceID = $1", string(id))
	if err != nil {
		return fmt.Errorf("failed to delete from History table: %w", err)
	}
	return nil
}

func (be *postgresBackend) AddNewOrchestrationEvent(ctx context.Context, iid api.InstanceID, e *backend.HistoryEvent) error {
	if e == nil {
		return backend.ErrNilHistoryEvent
	} else if e.Timestamp == nil {
		return backend.ErrNilEventTimestamp
	}

	eventPayload, err := backend.MarshalHistoryEvent(e)
	if err != nil {
		return err
	}

	_, err = be.db.Exec(
		ctx,
		`INSERT INTO NewEvents (InstanceID, EventPayload) VALUES ($1, $2)`,
		string(iid),
		eventPayload,
	)

	if err != nil {
		return fmt.Errorf("failed to insert row into NewEvents table: %w", err)
	}

	return nil
}

// GetOrchestrationMetadata implements backend.Backend
func (be *postgresBackend) GetOrchestrationMetadata(ctx context.Context, iid api.InstanceID) (*api.OrchestrationMetadata, error) {
	if err := be.ensureDB(); err != nil {
		return nil, err
	}

	row := be.db.QueryRow(
		ctx,
		`SELECT InstanceID, Name, RuntimeStatus, CreatedTime, LastUpdatedTime, Input, Output, CustomStatus, FailureDetails
		FROM Instances WHERE InstanceID = $1`,
		string(iid),
	)

	var instanceID *string
	var name *string
	var runtimeStatus *string
	var createdAt *time.Time
	var lastUpdatedAt *time.Time
	var input *string
	var output *string
	var customStatus *string
	var failureDetails *protos.TaskFailureDetails

	var failureDetailsPayload []byte
	err := row.Scan(&instanceID, &name, &runtimeStatus, &createdAt, &lastUpdatedAt, &input, &output, &customStatus, &failureDetailsPayload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, api.ErrInstanceNotFound
	} else if err != nil {
		return nil, fmt.Errorf("failed to scan the Instances table result: %w", err)
	}

	if input == nil {
		input = &emptyString
	}

	if output == nil {
		output = &emptyString
	}

	if customStatus == nil {
		customStatus = &emptyString
	}

	if len(failureDetailsPayload) > 0 {
		failureDetails = new(protos.TaskFailureDetails)
		if err := proto.Unmarshal(failureDetailsPayload, failureDetails); err != nil {
			return nil, fmt.Errorf("failed to unmarshal failure details: %w", err)
		}
	}

	metadata := api.NewOrchestrationMetadata(
		iid,
		*name,
		helpers.FromRuntimeStatusString(*runtimeStatus),
		*createdAt,
		*lastUpdatedAt,
		*input,
		*output,
		*customStatus,
		failureDetails,
	)
	return metadata, nil
}

// GetOrchestrationRuntimeState implements backend.Backend
func (be *postgresBackend) GetOrchestrationRuntimeState(ctx context.Context, wi *backend.OrchestrationWorkItem) (*backend.OrchestrationRuntimeState, error) {
	if err := be.ensureDB(); err != nil {
		return nil, err
	}

	rows, err := be.db.Query(
		ctx,
		"SELECT EventPayload FROM History WHERE InstanceID = $1 ORDER BY SequenceNumber ASC",
		string(wi.InstanceID),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	existingEvents := make([]*protos.HistoryEvent, 0, 50)
	for rows.Next() {
		var eventPayload []byte
		if err := rows.Scan(&eventPayload); err != nil {
			return nil, fmt.Errorf("failed to read history event: %w", err)
		}

		e, err := backend.UnmarshalHistoryEvent(eventPayload)
		if err != nil {
			return nil, err
		}

		existingEvents = append(existingEvents, e)
	}

	state := backend.NewOrchestrationRuntimeState(wi.InstanceID, existingEvents)
	return state, nil
}

// GetOrchestrationWorkItem implements backend.Backend
func (be *postgresBackend) GetOrchestrationWorkItem(ctx context.Context) (*backend.OrchestrationWorkItem, error) {
	if err := be.ensureDB(); err != nil {
		return nil, err
	}

	tx, err := be.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	now := time.Now().UTC()
	newLockExpiration := now.Add(be.options.OrchestrationLockTimeout)

	// Place a lock on an orchestration instance that has new events that are ready to be executed.
	row := tx.QueryRow(
		ctx,
		`UPDATE Instances SET LockedBy = $1, LockExpiration = $2, DequeueCount = DequeueCount + 1
		WHERE SequenceNumber = (
			SELECT SequenceNumber FROM Instances I
			WHERE (I.LockExpiration IS NULL OR I.LockExpiration < $3) AND EXISTS (
				SELECT 1 FROM NewEvents E
				WHERE E.InstanceID = I.InstanceID AND (E.VisibleTime IS NULL OR E.VisibleTime < $4)
			)
			ORDER BY I.InstanceID, I.SequenceNumber ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		) RETURNING InstanceID, DequeueCount`,
		be.workerName,     // LockedBy for Instances table
		newLockExpiration, // Updated LockExpiration for Instances table
		now,               // LockExpiration for Instances table
		now,               // VisibleTime for NewEvents table
	)

	var instanceID string
	var dequeueCount int32
	if err := row.Scan(&instanceID, &dequeueCount); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// No new events to process
			return nil, backend.ErrNoWorkItems
		}

		return nil, fmt.Errorf("failed to scan the orchestration work-item: %w", err)
	}

	// Get all the unprocessed events associated with the locked instance.
	// The orchestration instance lock is the lease; NewEvents rows are only read here
	// and deleted by SequenceNumber on a successful completion.
	events, err := tx.Query(
		ctx,
		`SELECT SequenceNumber, EventPayload
		FROM NewEvents
		WHERE InstanceID = $1 AND (VisibleTime IS NULL OR VisibleTime <= $2)
		ORDER BY SequenceNumber
		LIMIT 1000`,
		instanceID,
		now,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query for orchestration work-items: %w", err)
	}
	defer events.Close()

	type rawEvent struct {
		sequenceNumber int64
		payload      []byte
	}

	rawEvents := []rawEvent{}
	for events.Next() {
		var sequenceNumber int64
		var eventPayload []byte
		if err := events.Scan(&sequenceNumber, &eventPayload); err != nil {
			return nil, fmt.Errorf("failed to read history event: %w", err)
		}
		rawEvents = append(rawEvents, rawEvent{
			sequenceNumber: sequenceNumber,
			payload:      eventPayload,
		})
	}
	events.Close()

	// Load history events within the same transaction to eliminate a separate round trip
	historyRows, err := tx.Query(
		ctx,
		"SELECT EventPayload FROM History WHERE InstanceID = $1 ORDER BY SequenceNumber ASC",
		instanceID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query history events: %w", err)
	}
	defer historyRows.Close()

	existingEvents := make([]*protos.HistoryEvent, 0, 50)
	for historyRows.Next() {
		var eventPayload []byte
		if err := historyRows.Scan(&eventPayload); err != nil {
			return nil, fmt.Errorf("failed to read history event: %w", err)
		}
		e, err := backend.UnmarshalHistoryEvent(eventPayload)
		if err != nil {
			return nil, err
		}
		existingEvents = append(existingEvents, e)
	}
	historyRows.Close()

	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to update orchestration work-item: %w", err)
	}

	sequenceNumbers := make([]int64, 0, len(rawEvents))
	newEvents := make([]*protos.HistoryEvent, 0, len(rawEvents))
	for _, e := range rawEvents {
		sequenceNumbers = append(sequenceNumbers, e.sequenceNumber)

		evt, err := backend.UnmarshalHistoryEvent(e.payload)
		if err != nil {
			return nil, err
		}

		newEvents = append(newEvents, evt)
	}

	wi := &backend.OrchestrationWorkItem{
		InstanceID:              api.InstanceID(instanceID),
		NewEvents:               newEvents,
		NewEventSequenceNumbers: sequenceNumbers,
		State:                   backend.NewOrchestrationRuntimeState(api.InstanceID(instanceID), existingEvents),
		LockedBy:                be.workerName,
		RetryCount:              dequeueCount - 1,
	}

	return wi, nil
}

func (be *postgresBackend) GetActivityWorkItem(ctx context.Context) (*backend.ActivityWorkItem, error) {
	if err := be.ensureDB(); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	newLockExpiration := now.Add(be.options.OrchestrationLockTimeout)

	row := be.db.QueryRow(
		ctx,
		`UPDATE NewTasks SET LockedBy = $1, LockExpiration = $2, DequeueCount = DequeueCount + 1
		WHERE SequenceNumber = (
			SELECT SequenceNumber FROM NewTasks T
			WHERE T.LockExpiration IS NULL OR T.LockExpiration < $3
			ORDER BY T.InstanceID, T.SequenceNumber ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		) RETURNING SequenceNumber, InstanceID, EventPayload`,
		be.workerName,
		newLockExpiration,
		now,
	)

	var sequenceNumber int64
	var instanceID string
	var eventPayload []byte

	if err := row.Scan(&sequenceNumber, &instanceID, &eventPayload); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// No new activity tasks to process
			return nil, backend.ErrNoWorkItems
		}

		return nil, fmt.Errorf("failed to scan the activity work-item: %w", err)
	}

	e, err := backend.UnmarshalHistoryEvent(eventPayload)
	if err != nil {
		return nil, err
	}

	wi := &backend.ActivityWorkItem{
		SequenceNumber: sequenceNumber,
		InstanceID:     api.InstanceID(instanceID),
		NewEvent:       e,
		LockedBy:       be.workerName,
	}
	return wi, nil
}

func (be *postgresBackend) CompleteActivityWorkItem(ctx context.Context, wi *backend.ActivityWorkItem) error {
	if err := be.ensureDB(); err != nil {
		return err
	}

	tx, err := be.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	bytes, err := backend.MarshalHistoryEvent(wi.Result)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, "INSERT INTO NewEvents (InstanceID, EventPayload) VALUES ($1, $2)", string(wi.InstanceID), bytes)
	if err != nil {
		return fmt.Errorf("failed to insert into NewEvents table: %w", err)
	}

	dbResult, err := tx.Exec(ctx, "DELETE FROM NewTasks WHERE SequenceNumber = $1 AND LockedBy = $2", wi.SequenceNumber, wi.LockedBy)
	if err != nil {
		return fmt.Errorf("failed to delete from NewTasks table: %w", err)
	}

	rowsAffected := dbResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed get rows affected by delete statement: %w", err)
	} else if rowsAffected == 0 {
		return backend.ErrWorkItemLockLost
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (be *postgresBackend) AbandonActivityWorkItem(ctx context.Context, wi *backend.ActivityWorkItem) error {
	if err := be.ensureDB(); err != nil {
		return err
	}

	dbResult, err := be.db.Exec(
		ctx,
		"UPDATE NewTasks SET LockedBy = NULL, LockExpiration = NULL WHERE SequenceNumber = $1 AND LockedBy = $2",
		wi.SequenceNumber,
		wi.LockedBy,
	)
	if err != nil {
		return fmt.Errorf("failed to update the NewTasks table for abandon: %w", err)
	}

	rowsAffected := dbResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed get rows affected by update statement for abandon: %w", err)
	} else if rowsAffected == 0 {
		return backend.ErrWorkItemLockLost
	}

	return nil
}

func (be *postgresBackend) PurgeOrchestrationState(ctx context.Context, id api.InstanceID) error {
	if err := be.ensureDB(); err != nil {
		return err
	}

	tx, err := be.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	if err := be.cleanupOrchestrationStateInternal(ctx, tx, id, true); err != nil {
		return err
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// Start implements backend.Backend
func (be *postgresBackend) Start(ctx context.Context) error {
	if be.db == nil {
		pool, err := pgxpool.NewWithConfig(ctx, be.options.PgOptions)
		if err != nil {
			be.logger.Error("Start", "failed to create a new postgres pool", err)
			return fmt.Errorf("failed to create a new postgres pool %w", err)
		}
		be.db = pool
	}

	return nil
}

// Stop implements backend.Backend
func (be *postgresBackend) Stop(context.Context) error {
	if be.db != nil {
		be.db.Close()
		be.db = nil
	}

	return nil
}

func (be *postgresBackend) String() string {
	maskedPassword := strings.Repeat("*", len(be.options.PgOptions.ConnConfig.Password))
	connectionURI := fmt.Sprintf("postgresql://%s:%s@%s:%d/%s", be.options.PgOptions.ConnConfig.User, maskedPassword, be.options.PgOptions.ConnConfig.Host, be.options.PgOptions.ConnConfig.Port, be.options.PgOptions.ConnConfig.Database)
	return connectionURI
}

func (be *postgresBackend) ensureDB() error {
	if be.db == nil {
		return backend.ErrNotInitialized
	}
	return nil
}

// multiRowPlaceholders returns a "($1,$2),($3,$4),..." value clause for the
// given number of rows and columns. It uses strconv rather than fmt to avoid
// per-row formatter overhead and to keep the placeholder math in one place.
func multiRowPlaceholders(rowCount, colCount int) string {
	var b strings.Builder
	// Pre-size the builder roughly: each cell costs about 8 bytes including
	// the comma/placeholder/parentheses overhead.
	b.Grow(rowCount * colCount * 8)

	for i := 0; i < rowCount; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('(')
		for j := 0; j < colCount; j++ {
			if j > 0 {
				b.WriteByte(',')
			}
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(i*colCount + j + 1))
		}
		b.WriteByte(')')
	}
	return b.String()
}
