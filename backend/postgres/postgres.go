package postgres

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"os"
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
	conf.ConnConfig.Config.ConnectTimeout = 2 * time.Minute
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
	uuidStr := uuid.NewString()

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
func (be *postgresBackend) CreateTaskHub(context.Context) error {
	pool, err := pgxpool.NewWithConfig(context.Background(), be.options.PgOptions)
	if err != nil {
		be.logger.Error("CreateTaskHub", "failed to create a new postgres pool", err)
		return err
	}
	be.db = pool
	// Initialize database
	if _, err := be.db.Exec(context.Background(), schema); err != nil {
		panic(fmt.Errorf("failed to initialize the database: %w", err))
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

	be.db.Close()
	be.db = nil

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
	defer tx.Rollback(ctx)

	var visibleTime *time.Time = nil
	if delay := wi.GetAbandonDelay(); delay > 0 {
		t := time.Now().UTC().Add(delay)
		visibleTime = &t
	}

	dbResult, err := tx.Exec(
		ctx,
		"UPDATE NewEvents SET LockedBy = NULL, VisibleTime = $1 WHERE InstanceID = $2 AND LockedBy = $3",
		visibleTime,
		string(wi.InstanceID),
		wi.LockedBy,
	)
	if err != nil {
		return fmt.Errorf("failed to update NewEvents table: %w", err)
	}

	rowsAffected := dbResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed get rows affected by UPDATE NewEvents statement: %w", err)
	} else if rowsAffected == 0 {
		return backend.ErrWorkItemLockLost
	}

	dbResult, err = tx.Exec(
		ctx,
		"UPDATE Instances SET LockedBy = NULL, LockExpiration = NULL WHERE InstanceID = $1 AND LockedBy = $2",
		string(wi.InstanceID),
		wi.LockedBy,
	)

	if err != nil {
		return fmt.Errorf("failed to update Instances table: %w", err)
	}

	rowsAffected = dbResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed get rows affected by UPDATE Instances statement: %w", err)
	} else if rowsAffected == 0 {
		return backend.ErrWorkItemLockLost
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// CompleteOrchestrationWorkItem implements backend.Backend
func (be *postgresBackend) CompleteOrchestrationWorkItem(ctx context.Context, wi *backend.OrchestrationWorkItem) error {
	if err := be.ensureDB(); err != nil {
		return err
	}

	tx, err := be.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	now := time.Now().UTC()

	// Dynamically generate the UPDATE statement for the Instances table
	var sqlSB strings.Builder
	sqlSB.WriteString("UPDATE Instances SET ")

	sqlUpdateArgs := make([]interface{}, 0, 10)
	isCreated := false
	isCompleted := false

	currIndex := 1
	for _, e := range wi.State.NewEvents() {
		if es := e.GetExecutionStarted(); es != nil {
			if isCreated {
				// TODO: Log warning about duplicate start event
				continue
			}
			isCreated = true
			sqlSB.WriteString(fmt.Sprintf("CreatedTime = $%d, Input = $%d, ", currIndex, currIndex+1))
			currIndex += 2
			sqlUpdateArgs = append(sqlUpdateArgs, e.Timestamp.AsTime())
			sqlUpdateArgs = append(sqlUpdateArgs, es.Input.GetValue())
		} else if ec := e.GetExecutionCompleted(); ec != nil {
			if isCompleted {
				// TODO: Log warning about duplicate completion event
				continue
			}
			isCompleted = true
			sqlSB.WriteString(fmt.Sprintf("CompletedTime = $%d, Output = $%d, FailureDetails = $%d, ", currIndex, currIndex+1, currIndex+2))
			currIndex += 3
			sqlUpdateArgs = append(sqlUpdateArgs, now)
			sqlUpdateArgs = append(sqlUpdateArgs, ec.Result.GetValue())
			if ec.FailureDetails != nil {
				bytes, err := proto.Marshal(ec.FailureDetails)
				if err != nil {
					return fmt.Errorf("failed to marshal FailureDetails: %w", err)
				}
				sqlUpdateArgs = append(sqlUpdateArgs, &bytes)
			} else {
				sqlUpdateArgs = append(sqlUpdateArgs, nil)
			}
		}
		// TODO: Execution suspended & resumed
	}

	if wi.State.CustomStatus != nil {
		sqlSB.WriteString(fmt.Sprintf("CustomStatus = $%d, ", currIndex))
		currIndex++
		sqlUpdateArgs = append(sqlUpdateArgs, wi.State.CustomStatus.Value)
	}

	// TODO: Support for stickiness, which would extend the LockExpiration
	sqlSB.WriteString(fmt.Sprintf("RuntimeStatus = $%d, LastUpdatedTime = $%d, LockExpiration = NULL WHERE InstanceID = $%d AND LockedBy = $%d", currIndex, currIndex+1, currIndex+2, currIndex+3))
	currIndex += 4
	sqlUpdateArgs = append(sqlUpdateArgs, helpers.ToRuntimeStatusString(wi.State.RuntimeStatus()), now, string(wi.InstanceID), wi.LockedBy)

	result, err := tx.Exec(ctx, sqlSB.String(), sqlUpdateArgs...)
	if err != nil {
		return fmt.Errorf("failed to update Instances table: %w", err)
	}

	count := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get the number of rows affected by the Instance table update: %w", err)
	} else if count == 0 {
		return fmt.Errorf("instance '%s' no longer exists or was locked by a different worker", string(wi.InstanceID))
	}

	// If continue-as-new, delete all existing history
	if wi.State.ContinuedAsNew() {
		if _, err := tx.Exec(ctx, "DELETE FROM History WHERE InstanceID = $1", string(wi.InstanceID)); err != nil {
			return fmt.Errorf("failed to delete from History table: %w", err)
		}
	}

	// Save new history events
	newHistoryCount := len(wi.State.NewEvents())
	if newHistoryCount > 0 {
		builder := strings.Builder{}
		builder.WriteString("INSERT INTO History (InstanceID, SequenceNumber, EventPayload) VALUES ")
		for i := 0; i < newHistoryCount; i++ {
			builder.WriteString(fmt.Sprintf("($%d, $%d, $%d)", 3*i+1, 3*i+2, 3*i+3))
			if i < newHistoryCount-1 {
				builder.WriteString(", ")
			}
		}
		query := builder.String()

		args := make([]interface{}, 0, newHistoryCount*3)
		nextSequenceNumber := len(wi.State.OldEvents())
		for _, e := range wi.State.NewEvents() {
			eventPayload, err := backend.MarshalHistoryEvent(e)
			if err != nil {
				return err
			}

			args = append(args, string(wi.InstanceID), nextSequenceNumber, eventPayload)
			nextSequenceNumber++
		}

		_, err = tx.Exec(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("failed to insert into the History table: %w", err)
		}
	}

	// Save outbound activity tasks
	newActivityCount := len(wi.State.PendingTasks())
	if newActivityCount > 0 {
		builder := strings.Builder{}
		builder.WriteString("INSERT INTO NewTasks (InstanceID, EventPayload) VALUES ")
		for i := 0; i < newActivityCount; i++ {
			builder.WriteString(fmt.Sprintf("($%d, $%d)", 2*i+1, 2*i+2))
			if i < newActivityCount-1 {
				builder.WriteString(", ")
			}
		}
		insertSql := builder.String()

		sqlInsertArgs := make([]interface{}, 0, newActivityCount*2)
		for _, e := range wi.State.PendingTasks() {
			eventPayload, err := backend.MarshalHistoryEvent(e)
			if err != nil {
				return err
			}

			sqlInsertArgs = append(sqlInsertArgs, string(wi.InstanceID), eventPayload)
		}

		_, err = tx.Exec(ctx, insertSql, sqlInsertArgs...)
		if err != nil {
			return fmt.Errorf("failed to insert into the NewTasks table: %w", err)
		}
	}

	// Save outbound orchestrator events
	newEventCount := len(wi.State.PendingTimers()) + len(wi.State.PendingMessages())
	if newEventCount > 0 {
		builder := strings.Builder{}
		builder.WriteString("INSERT INTO NewEvents (InstanceID, EventPayload, VisibleTime) VALUES ")
		for i := 0; i < newEventCount; i++ {
			builder.WriteString(fmt.Sprintf("($%d, $%d, $%d)", 3*i+1, 3*i+2, 3*i+3))
			if i < newEventCount-1 {
				builder.WriteString(", ")
			}
		}
		insertSql := builder.String()

		sqlInsertArgs := make([]interface{}, 0, newEventCount*3)
		for _, e := range wi.State.PendingTimers() {
			eventPayload, err := backend.MarshalHistoryEvent(e)
			if err != nil {
				return err
			}

			visibileTime := e.GetTimerFired().GetFireAt().AsTime()
			sqlInsertArgs = append(sqlInsertArgs, string(wi.InstanceID), eventPayload, visibileTime)
		}

		for _, msg := range wi.State.PendingMessages() {
			if es := msg.HistoryEvent.GetExecutionStarted(); es != nil {
				// Need to insert a new row into the DB
				if _, err := be.createOrchestrationInstanceInternal(ctx, msg.HistoryEvent, tx); err != nil {
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

			eventPayload, err := backend.MarshalHistoryEvent(msg.HistoryEvent)
			if err != nil {
				return err
			}

			sqlInsertArgs = append(sqlInsertArgs, msg.TargetInstanceID, eventPayload, nil)
		}

		_, err = tx.Exec(ctx, insertSql, sqlInsertArgs...)
		if err != nil {
			return fmt.Errorf("failed to insert into the NewEvents table: %w", err)
		}
	}

	// Delete inbound events
	dbResult, err := tx.Exec(
		ctx,
		"DELETE FROM NewEvents WHERE InstanceID = $1 AND LockedBy = $2",
		string(wi.InstanceID),
		wi.LockedBy,
	)
	if err != nil {
		return fmt.Errorf("failed to delete from NewEvents table: %w", err)
	}

	rowsAffected := dbResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed get rows affected by delete statement: %w", err)
	} else if rowsAffected == 0 {
		return backend.ErrWorkItemLockLost
	}

	if err != nil {
		return fmt.Errorf("failed to delete from the NewEvents table: %w", err)
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
	defer tx.Rollback(ctx)

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
		return "", errors.New("HistoryEvent must be non-nil")
	} else if e.Timestamp == nil {
		return "", errors.New("HistoryEvent must have a non-nil timestamp")
	}

	startEvent := e.GetExecutionStarted()
	if startEvent == nil {
		return "", errors.New("HistoryEvent must be an ExecutionStartedEvent")
	}
	instanceID := startEvent.OrchestrationInstance.InstanceId

	policy := &protos.OrchestrationIdReusePolicy{}

	for _, opt := range opts {
		opt(policy)
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
	res, err := tx.Exec(
		ctx,
		`INSERT INTO Instances (
			Name,
			Version,
			InstanceID,
			ExecutionID,
			Input,
			RuntimeStatus,
			CreatedTime
		) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING`,
		startEvent.Name,
		startEvent.Version.GetValue(),
		startEvent.OrchestrationInstance.InstanceId,
		startEvent.OrchestrationInstance.ExecutionId.GetValue(),
		startEvent.Input.GetValue(),
		"PENDING",
		e.Timestamp.AsTime(),
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

	_, err := tx.Exec(ctx, "DELETE FROM History WHERE InstanceID = $1", string(id))
	if err != nil {
		return fmt.Errorf("failed to delete from History table: %w", err)
	}

	_, err = tx.Exec(ctx, "DELETE FROM NewEvents WHERE InstanceID = $1", string(id))
	if err != nil {
		return fmt.Errorf("failed to delete from NewEvents table: %w", err)
	}

	_, err = tx.Exec(ctx, "DELETE FROM NewTasks WHERE InstanceID = $1", string(id))
	if err != nil {
		return fmt.Errorf("failed to delete from NewTasks table: %w", err)
	}
	return nil
}

func (be *postgresBackend) AddNewOrchestrationEvent(ctx context.Context, iid api.InstanceID, e *backend.HistoryEvent) error {
	if e == nil {
		return errors.New("HistoryEvent must be non-nil")
	} else if e.Timestamp == nil {
		return errors.New("HistoryEvent must have a non-nil timestamp")
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
	defer tx.Rollback(ctx)

	now := time.Now().UTC()
	newLockExpiration := now.Add(be.options.OrchestrationLockTimeout)

	// Place a lock on an orchestration instance that has new events that are ready to be executed.
	row := tx.QueryRow(
		ctx,
		`UPDATE Instances SET LockedBy = $1, LockExpiration = $2
		WHERE SequenceNumber = (
			SELECT SequenceNumber FROM Instances I
			WHERE (I.LockExpiration IS NULL OR I.LockExpiration < $3) AND EXISTS (
				SELECT 1 FROM NewEvents E
				WHERE E.InstanceID = I.InstanceID AND (E.VisibleTime IS NULL OR E.VisibleTime < $4)
			)
			LIMIT 1
		) RETURNING InstanceID`,
		be.workerName,     // LockedBy for Instances table
		newLockExpiration, // Updated LockExpiration for Instances table
		now,               // LockExpiration for Instances table
		now,               // VisibleTime for NewEvents table
	)

	var instanceID string
	if err := row.Scan(&instanceID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// No new events to process
			return nil, backend.ErrNoWorkItems
		}

		return nil, fmt.Errorf("failed to scan the orchestration work-item: %w", err)
	}

	// TODO: Get all the unprocessed events associated with the locked instance
	events, err := tx.Query(
		ctx,
		`UPDATE NewEvents SET DequeueCount = DequeueCount + 1, LockedBy = $1 WHERE SequenceNumber IN (
			SELECT SequenceNumber FROM NewEvents
			WHERE InstanceID = $2 AND (VisibleTime IS NULL OR VisibleTime <= $3)
			LIMIT 1000
		)
		RETURNING EventPayload, DequeueCount`,
		be.workerName,
		instanceID,
		now,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query for orchestration work-items: %w", err)
	}
	defer events.Close()

	maxDequeueCount := int32(0)

	newEvents := make([]*protos.HistoryEvent, 0, 10)
	for events.Next() {
		var eventPayload []byte
		var dequeueCount int32
		if err := events.Scan(&eventPayload, &dequeueCount); err != nil {
			return nil, fmt.Errorf("failed to read history event: %w", err)
		}

		if dequeueCount > maxDequeueCount {
			maxDequeueCount = dequeueCount
		}

		e, err := backend.UnmarshalHistoryEvent(eventPayload)
		if err != nil {
			return nil, err
		}

		newEvents = append(newEvents, e)
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to update orchestration work-item: %w", err)
	}

	wi := &backend.OrchestrationWorkItem{
		InstanceID: api.InstanceID(instanceID),
		NewEvents:  newEvents,
		LockedBy:   be.workerName,
		RetryCount: maxDequeueCount - 1,
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
			LIMIT 1
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
	defer tx.Rollback(ctx)

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
	defer tx.Rollback(ctx)

	if err := be.cleanupOrchestrationStateInternal(ctx, tx, id, true); err != nil {
		return err
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// Start implements backend.Backend
func (*postgresBackend) Start(context.Context) error {
	return nil
}

// Stop implements backend.Backend
func (*postgresBackend) Stop(context.Context) error {
	return nil
}

func (be *postgresBackend) ensureDB() error {
	if be.db == nil {
		return backend.ErrNotInitialized
	}
	return nil
}

func (be *postgresBackend) String() string {
	connectionURI := fmt.Sprintf("postgresql://%s:%s@%s:%d/%s", be.options.PgOptions.ConnConfig.User, be.options.PgOptions.ConnConfig.Password, be.options.PgOptions.ConnConfig.Host, be.options.PgOptions.ConnConfig.Port, be.options.PgOptions.ConnConfig.Database)
	return connectionURI
}
