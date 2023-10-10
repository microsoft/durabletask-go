package postgres

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"

	"google.golang.org/protobuf/encoding/protojson"

	_ "github.com/jackc/pgx/v5/stdlib"
)

//go:embed schema.sql
var schema string

var emptyString string = ""

type postgresBackend struct {
	connStr     string
	db          *sql.DB
	workerName  string
	logger      backend.Logger
	initLock    sync.Mutex
	initialized bool

	orchestrationLockTimeout time.Duration
	activityLockTimeout      time.Duration
}

// NewPostgresBackend creates a new postgres-based Backend object.
func NewPostgresBackend(connString string, logger backend.Logger) backend.Backend {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	pid := os.Getpid()
	uuidStr := uuid.NewString()

	be := &postgresBackend{
		connStr:                  connString,
		workerName:               fmt.Sprintf("%s,%d,%s", hostname, pid, uuidStr),
		logger:                   logger,
		orchestrationLockTimeout: 2 * time.Minute,
		activityLockTimeout:      2 * time.Minute,
	}

	return be
}

// CreateTaskHub creates the postgres database and applies the schema
func (be *postgresBackend) CreateTaskHub(ctx context.Context) error {
	be.initLock.Lock()
	defer be.initLock.Unlock()

	if be.db == nil {
		be.logger.Debug("Opening database connection")
		db, err := sql.Open("pgx", be.connStr)
		if err != nil {
			return fmt.Errorf("failed to open the database: %w", err)
		}
		be.db = db
	}

	if !be.initialized {
		// Initialize database
		be.logger.Info("Running database initialization script")
		if _, err := be.db.Exec(schema); err != nil {
			return fmt.Errorf("failed to initialize the database: %w", err)
		}
		be.initialized = true
	}
	return nil
}

func (be *postgresBackend) DeleteTaskHub(ctx context.Context) error {
	be.initLock.Lock()
	defer be.initLock.Unlock()

	if be.db == nil {
		return nil
	}

	// Delete all the tables in the durabletask schema
	be.logger.Info("Dropping durabletask schema from database")
	if _, err := be.db.Exec("DROP SCHEMA IF EXISTS durabletask CASCADE;"); err != nil {
		panic(fmt.Errorf("failed to delete the 'durabletask' database schema: %w", err))
	}

	if err := be.db.Close(); err != nil {
		return fmt.Errorf("failed to close the database connection: %w", err)
	}
	be.db = nil

	// TODO: Figure out what this means
	return nil
}

// AbandonOrchestrationWorkItem implements backend.Backend
func (be *postgresBackend) AbandonOrchestrationWorkItem(ctx context.Context, wi *backend.OrchestrationWorkItem) error {
	if err := be.ensureDB(ctx); err != nil {
		return err
	}

	tx, err := be.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var visibleTime *time.Time = nil
	if delay := wi.GetAbandonDelay(); delay > 0 {
		t := time.Now().UTC().Add(delay)
		visibleTime = &t
	}

	dbResult, err := tx.ExecContext(
		ctx,
		"UPDATE durabletask.NewEvents SET LockedBy = NULL, VisibleTime = $1 WHERE InstanceID = $2 AND LockedBy = $3",
		visibleTime,
		string(wi.InstanceID),
		wi.LockedBy,
	)
	if err != nil {
		return fmt.Errorf("failed to update NewEvents table: %w", err)
	}

	rowsAffected, err := dbResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed get rows affected by UPDATE NewEvents statement: %w", err)
	} else if rowsAffected == 0 {
		return backend.ErrWorkItemLockLost
	}

	dbResult, err = tx.ExecContext(
		ctx,
		"UPDATE durabletask.Instances SET LockedBy = NULL, LockExpiration = NULL WHERE InstanceID = $1 AND LockedBy = $2",
		string(wi.InstanceID),
		wi.LockedBy,
	)

	if err != nil {
		return fmt.Errorf("failed to update Instances table: %w", err)
	}

	rowsAffected, err = dbResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed get rows affected by UPDATE Instances statement: %w", err)
	} else if rowsAffected == 0 {
		return backend.ErrWorkItemLockLost
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// CompleteOrchestrationWorkItem implements backend.Backend
func (be *postgresBackend) CompleteOrchestrationWorkItem(ctx context.Context, wi *backend.OrchestrationWorkItem) error {
	if err := be.ensureDB(ctx); err != nil {
		return err
	}

	tx, err := be.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := time.Now().UTC()

	// Dynamically generate the UPDATE statement for the Instances table
	var sqlSB strings.Builder
	sqlSB.WriteString("UPDATE durabletask.Instances SET ")

	sqlUpdateArgs := make([]interface{}, 0, 10)
	isCreated := false
	isCompleted := false
	argIndex := 1

	for _, e := range wi.State.NewEvents() {
		if es := e.GetExecutionStarted(); es != nil {
			if isCreated {
				// TODO: Log warning about duplicate start event
				continue
			}
			isCreated = true
			sqlSB.WriteString(fmt.Sprintf("CreatedTime = $%d, Input = $%d, ", argIndex, argIndex+1))
			sqlUpdateArgs = append(sqlUpdateArgs, e.Timestamp.AsTime())
			sqlUpdateArgs = append(sqlUpdateArgs, es.Input.GetValue())
			argIndex += 2
		} else if ec := e.GetExecutionCompleted(); ec != nil {
			if isCompleted {
				// TODO: Log warning about duplicate completion event
				continue
			}
			isCompleted = true
			sqlSB.WriteString(fmt.Sprintf("CompletedTime = $%d, Output = $%d, FailureDetails = $%d, ", argIndex, argIndex+1, argIndex+2))
			sqlUpdateArgs = append(sqlUpdateArgs, now)
			sqlUpdateArgs = append(sqlUpdateArgs, ec.Result.GetValue())
			if ec.FailureDetails != nil {
				bytes, err := protojson.Marshal(ec.FailureDetails)
				if err != nil {
					return fmt.Errorf("failed to marshal FailureDetails: %w", err)
				}
				sqlUpdateArgs = append(sqlUpdateArgs, &bytes)
			} else {
				sqlUpdateArgs = append(sqlUpdateArgs, nil)
			}
			argIndex += 3
		}
		// TODO: Execution suspended & resumed
	}

	if wi.State.CustomStatus != nil {
		sqlSB.WriteString(fmt.Sprintf("CustomStatus = $%d, ", argIndex))
		sqlUpdateArgs = append(sqlUpdateArgs, wi.State.CustomStatus.Value)
		argIndex++
	}

	// TODO: Support for stickiness, which would extend the LockExpiration
	sqlSB.WriteString(fmt.Sprintf(
		"RuntimeStatus = $%d, LastUpdatedTime = $%d, LockExpiration = NULL WHERE InstanceID = $%d AND LockedBy = $%d",
		argIndex, argIndex+1, argIndex+2, argIndex+3))
	sqlUpdateArgs = append(sqlUpdateArgs, helpers.ToRuntimeStatusString(wi.State.RuntimeStatus()), now, string(wi.InstanceID), wi.LockedBy)
	argIndex += 4

	command := sqlSB.String()
	result, err := tx.ExecContext(ctx, command, sqlUpdateArgs...)
	if err != nil {
		return fmt.Errorf("failed to update Instances table: %w", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get the number of rows affected by the Instance table update: %w", err)
	} else if count == 0 {
		return fmt.Errorf("instance '%s' no longer exists or was locked by a different worker", string(wi.InstanceID))
	}

	// If continue-as-new, delete all existing history
	if wi.State.ContinuedAsNew() {
		if _, err := tx.ExecContext(ctx, "DELETE FROM durabletask.History WHERE InstanceID = $1", string(wi.InstanceID)); err != nil {
			return fmt.Errorf("failed to delete from History table: %w", err)
		}
	}

	// Save new history events
	newHistoryCount := len(wi.State.NewEvents())
	if newHistoryCount > 0 {
		var sqlSB strings.Builder
		sqlSB.WriteString("INSERT INTO durabletask.History (InstanceID, SequenceNumber, EventPayload) VALUES ($1, $2, $3)")
		for i := 1; i < newHistoryCount; i++ {
			counter := i * 3
			sqlSB.WriteString(fmt.Sprintf(", ($%d, $%d, $%d)", counter+1, counter+2, counter+3))
		}
		query := sqlSB.String()
		args := make([]interface{}, 0, newHistoryCount*3)
		nextSequenceNumber := len(wi.State.OldEvents())
		for _, e := range wi.State.NewEvents() {
			eventPayload, err := protojson.Marshal(e) //
			if err != nil {
				return err
			}

			args = append(args, string(wi.InstanceID), nextSequenceNumber, eventPayload)
			nextSequenceNumber++
		}

		_, err = tx.ExecContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("failed to insert into the History table: %w", err)
		}
	}

	// Save outbound activity tasks
	newActivityCount := len(wi.State.PendingTasks())
	if newActivityCount > 0 {
		var sqlSB strings.Builder
		sqlSB.WriteString("INSERT INTO durabletask.NewTasks (InstanceID, EventPayload) VALUES ($1, $2)")
		for i := 1; i < newActivityCount; i++ {
			counter := i * 2
			sqlSB.WriteString(fmt.Sprintf(", ($%d, $%d)", counter+1, counter+2))
		}
		insertSql := sqlSB.String()

		sqlInsertArgs := make([]interface{}, 0, newActivityCount*2)
		for _, e := range wi.State.PendingTasks() {
			eventPayload, err := protojson.Marshal(e)
			if err != nil {
				return err
			}

			sqlInsertArgs = append(sqlInsertArgs, string(wi.InstanceID), eventPayload)
		}

		_, err = tx.ExecContext(ctx, insertSql, sqlInsertArgs...)
		if err != nil {
			return fmt.Errorf("failed to insert into the NewTasks table: %w", err)
		}
	}

	// Save outbound orchestrator events
	newEventCount := len(wi.State.PendingTimers()) + len(wi.State.PendingMessages())
	if newEventCount > 0 {
		var sqlSB strings.Builder
		sqlSB.WriteString("INSERT INTO durabletask.NewEvents (InstanceID, EventPayload, VisibleTime) VALUES ($1, $2, $3)")
		for i := 1; i < newEventCount; i++ {
			counter := i * 3
			sqlSB.WriteString(fmt.Sprintf(", ($%d, $%d, $%d)", counter+1, counter+2, counter+3))
		}
		insertSql := sqlSB.String()

		sqlInsertArgs := make([]interface{}, 0, newEventCount*3)
		for _, e := range wi.State.PendingTimers() {
			eventPayload, err := protojson.Marshal(e)
			if err != nil {
				return err
			}

			visibileTime := e.GetTimerFired().GetFireAt().AsTime()
			sqlInsertArgs = append(sqlInsertArgs, string(wi.InstanceID), eventPayload, visibileTime)
		}

		for _, msg := range wi.State.PendingMessages() {
			if es := msg.HistoryEvent.GetExecutionStarted(); es != nil {
				// Need to insert a new row into the DB
				var instanceID string
				if err := be.createOrchestrationInstanceInternal(ctx, msg.HistoryEvent, tx, &instanceID); err != nil {
					if err == backend.ErrDuplicateEvent {
						be.logger.Warnf(
							"%v: dropping sub-orchestration creation event because an instance with the target ID (%v) already exists.",
							wi.InstanceID,
							es.OrchestrationInstance.InstanceId)
					} else {
						return err
					}
				}
			}

			eventPayload, err := protojson.Marshal(msg.HistoryEvent)
			if err != nil {
				return err
			}

			sqlInsertArgs = append(sqlInsertArgs, msg.TargetInstanceID, eventPayload, nil)
		}

		_, err = tx.ExecContext(ctx, insertSql, sqlInsertArgs...)
		if err != nil {
			return fmt.Errorf("failed to insert into the NewEvents table: %w", err)
		}
	}

	// Delete inbound events
	dbResult, err := tx.ExecContext(
		ctx,
		"DELETE FROM durabletask.NewEvents WHERE InstanceID = $1 AND LockedBy = $2",
		string(wi.InstanceID),
		wi.LockedBy,
	)
	if err != nil {
		return fmt.Errorf("failed to delete from NewEvents table: %w", err)
	}

	rowsAffected, err := dbResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed get rows affected by delete statement: %w", err)
	} else if rowsAffected == 0 {
		return backend.ErrWorkItemLockLost
	}

	if err != nil {
		return fmt.Errorf("failed to delete from the NewEvents table: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// CreateOrchestrationInstance implements backend.Backend
func (be *postgresBackend) CreateOrchestrationInstance(ctx context.Context, e *backend.HistoryEvent) error {
	if err := be.ensureDB(ctx); err != nil {
		return err
	}

	tx, err := be.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()

	var instanceID string
	if err := be.createOrchestrationInstanceInternal(ctx, e, tx, &instanceID); err != nil {
		return err
	}

	eventPayload, err := protojson.Marshal(e)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO durabletask.NewEvents (InstanceID, EventPayload) VALUES ($1, $2)`,
		instanceID,
		eventPayload,
	)

	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to insert row into [NewEvents] table: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to create orchestration: %w", err)
	}

	return nil
}

func (be *postgresBackend) createOrchestrationInstanceInternal(ctx context.Context, e *backend.HistoryEvent, tx *sql.Tx, instanceID *string) error {
	if e == nil {
		return errors.New("HistoryEvent must be non-nil")
	} else if e.Timestamp == nil {
		return errors.New("HistoryEvent must have a non-nil timestamp")
	}

	startEvent := e.GetExecutionStarted()
	if startEvent == nil {
		return errors.New("HistoryEvent must be an ExecutionStartedEvent")
	}

	// TODO: Support for re-using orchestration instance IDs
	res, err := tx.ExecContext(
		ctx,
		`INSERT INTO durabletask.Instances (
			Name,
			Version,
			InstanceID,
			ExecutionID,
			Input,
			RuntimeStatus,
			CreatedTime
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (InstanceID) DO NOTHING`,
		startEvent.Name,
		startEvent.Version.GetValue(),
		startEvent.OrchestrationInstance.InstanceId,
		startEvent.OrchestrationInstance.ExecutionId.GetValue(),
		startEvent.Input.GetValue(),
		"PENDING",
		e.Timestamp.AsTime(),
	)
	if err != nil {
		return fmt.Errorf("failed to insert into [Instances] table: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to count the rows affected: %w", err)
	}

	if rows <= 0 {
		return backend.ErrDuplicateEvent
	}

	*instanceID = startEvent.OrchestrationInstance.InstanceId
	return nil
}

func (be *postgresBackend) AddNewOrchestrationEvent(ctx context.Context, iid api.InstanceID, e *backend.HistoryEvent) error {
	if e == nil {
		return errors.New("HistoryEvent must be non-nil")
	} else if e.Timestamp == nil {
		return errors.New("HistoryEvent must have a non-nil timestamp")
	}

	eventPayload, err := protojson.Marshal(e)
	if err != nil {
		return err
	}

	_, err = be.db.ExecContext(
		ctx,
		`INSERT INTO durabletask.NewEvents (InstanceID, EventPayload) VALUES ($1, $2)`,
		string(iid),
		eventPayload,
	)

	if err != nil {
		return fmt.Errorf("failed to insert row into [NewEvents] table: %w", err)
	}

	return nil
}

// GetOrchestrationMetadata implements backend.Backend
func (be *postgresBackend) GetOrchestrationMetadata(ctx context.Context, iid api.InstanceID) (*api.OrchestrationMetadata, error) {
	if err := be.ensureDB(ctx); err != nil {
		return nil, err
	}

	row := be.db.QueryRowContext(
		ctx,
		`SELECT InstanceID, Name, RuntimeStatus, CreatedTime, LastUpdatedTime, Input, Output, CustomStatus, FailureDetails
		FROM durabletask.Instances WHERE InstanceID = $1`,
		string(iid),
	)

	err := row.Err()
	if err == sql.ErrNoRows {
		return nil, api.ErrInstanceNotFound
	} else if err != nil {
		return nil, fmt.Errorf("failed to query the Instances table: %w", row.Err())
	}

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
	err = row.Scan(&instanceID, &name, &runtimeStatus, &createdAt, &lastUpdatedAt, &input, &output, &customStatus, &failureDetailsPayload)
	if err == sql.ErrNoRows {
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
		if err := protojson.Unmarshal(failureDetailsPayload, failureDetails); err != nil {
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
	if err := be.ensureDB(ctx); err != nil {
		return nil, err
	}

	rows, err := be.db.QueryContext(
		ctx,
		"SELECT EventPayload FROM durabletask.History WHERE InstanceID = $1 ORDER BY SequenceNumber ASC",
		string(wi.InstanceID),
	)
	if err != nil {
		return nil, err
	}

	existingEvents := make([]*protos.HistoryEvent, 0, 50)
	for rows.Next() {
		var eventPayload []byte
		if err := rows.Scan(&eventPayload); err != nil {
			return nil, fmt.Errorf("failed to read history event: %w", err)
		}
		e := new(protos.HistoryEvent)
		err := protojson.Unmarshal(eventPayload, e)
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
	if err := be.ensureDB(ctx); err != nil {
		return nil, err
	}

	tx, err := be.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	newLockExpiration := now.Add(be.orchestrationLockTimeout)

	// Place a lock on an orchestration instance that has new events that are ready to be executed.
	row := tx.QueryRowContext(
		ctx,
		`UPDATE durabletask.Instances SET LockedBy = $1, LockExpiration = $2
		WHERE ctid = (
			SELECT ctid FROM durabletask.Instances I
			WHERE (I.LockExpiration IS NULL OR I.LockExpiration < $3) AND EXISTS (
				SELECT 1 FROM durabletask.NewEvents E
				WHERE E.InstanceID = I.InstanceID AND (E.VisibleTime IS NULL OR E.VisibleTime < $4)
			)
			LIMIT 1
		) RETURNING InstanceID`,
		be.workerName,     // LockedBy for Instances table
		newLockExpiration, // Updated LockExpiration for Instances table
		now,               // LockExpiration for Instances table
		now,               // VisibleTime for NewEvents table
	)

	if err := row.Err(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("failed to query for orchestration work-items: %w", err)
	}

	var instanceID string
	if err := row.Scan(&instanceID); err != nil {
		if err == sql.ErrNoRows {
			// No new events to process
			return nil, backend.ErrNoWorkItems
		}

		return nil, fmt.Errorf("failed to scan the orchestration work-item: %w", err)
	}

	// TODO: Get all the unprocessed events associated with the locked instance
	events, err := tx.QueryContext(
		ctx,
		`WITH selected AS (
			UPDATE durabletask.NewEvents SET DequeueCount = DequeueCount + 1, LockedBy = $1 WHERE ctid IN (
				SELECT ctid FROM durabletask.NewEvents
				WHERE InstanceID = $2 AND (VisibleTime IS NULL OR VisibleTime <= $3)
				LIMIT 1000
			) RETURNING EventPayload, DequeueCount, SequenceNumber
		)
		SELECT EventPayload, DequeueCount FROM selected ORDER BY SequenceNumber ASC`,
		be.workerName,
		instanceID,
		now,
	)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("failed to query for orchestration work-items: %w", err)
	}

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

		e := new(protos.HistoryEvent)
		err := protojson.Unmarshal(eventPayload, e)
		if err != nil {
			return nil, err
		}

		newEvents = append(newEvents, e)
	}

	if err = tx.Commit(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
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
	if err := be.ensureDB(ctx); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	newLockExpiration := now.Add(be.activityLockTimeout)

	row := be.db.QueryRowContext(
		ctx,
		`UPDATE durabletask.NewTasks SET LockedBy = $1, LockExpiration = $2, DequeueCount = DequeueCount + 1
		WHERE SequenceNumber = (
			SELECT SequenceNumber FROM durabletask.NewTasks T
			WHERE T.LockExpiration IS NULL OR T.LockExpiration < $3
			ORDER BY SequenceNumber ASC
			LIMIT 1
		) RETURNING SequenceNumber, InstanceID, EventPayload`,
		be.workerName,
		newLockExpiration,
		now,
	)

	if err := row.Err(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("failed to query for activity work-items: %w", err)
	}

	var sequenceNumber int64
	var instanceID string
	var eventPayload []byte

	if err := row.Scan(&sequenceNumber, &instanceID, &eventPayload); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err == sql.ErrNoRows {
			// No new activity tasks to process
			return nil, backend.ErrNoWorkItems
		}
		return nil, fmt.Errorf("failed to scan the activity work-item: %w", err)
	}

	e := new(protos.HistoryEvent)
	err := protojson.Unmarshal(eventPayload, e)
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
	if err := be.ensureDB(ctx); err != nil {
		return err
	}

	tx, err := be.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bytes, err := protojson.Marshal(wi.Result)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, "INSERT INTO durabletask.NewEvents (InstanceID, EventPayload) VALUES ($1, $2)", string(wi.InstanceID), bytes)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to insert into NewEvents table: %w", err)
	}

	dbResult, err := tx.ExecContext(ctx, "DELETE FROM durabletask.NewTasks WHERE SequenceNumber = $1 AND LockedBy = $2", wi.SequenceNumber, wi.LockedBy)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to delete from NewTasks table: %w", err)
	}

	rowsAffected, err := dbResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed get rows affected by delete statement: %w", err)
	} else if rowsAffected == 0 {
		return backend.ErrWorkItemLockLost
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (be *postgresBackend) AbandonActivityWorkItem(ctx context.Context, wi *backend.ActivityWorkItem) error {
	if err := be.ensureDB(ctx); err != nil {
		return err
	}

	dbResult, err := be.db.ExecContext(
		ctx,
		"UPDATE durabletask.NewTasks SET LockedBy = NULL, LockExpiration = NULL WHERE SequenceNumber = $1 AND LockedBy = $2",
		wi.SequenceNumber,
		wi.LockedBy,
	)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to update the NewTasks table for abandon: %w", err)
	}

	rowsAffected, err := dbResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed get rows affected by update statement for abandon: %w", err)
	} else if rowsAffected == 0 {
		return backend.ErrWorkItemLockLost
	}

	return nil
}

func (be *postgresBackend) PurgeOrchestrationState(ctx context.Context, id api.InstanceID) error {
	if err := be.ensureDB(ctx); err != nil {
		return err
	}

	tx, err := be.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx, "SELECT 1 FROM durabletask.Instances WHERE InstanceID = $1", string(id))
	if err := row.Err(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to query for instance existence: %w", err)
	}

	var unused int
	if err := row.Scan(&unused); err == sql.ErrNoRows {
		return api.ErrInstanceNotFound
	} else if err != nil {
		return fmt.Errorf("failed to scan instance existence: %w", err)
	}

	dbResult, err := tx.ExecContext(ctx, "DELETE FROM durabletask.Instances WHERE InstanceID = $1 AND RuntimeStatus IN ('COMPLETED', 'FAILED', 'TERMINATED')", string(id))
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to delete from the Instances table: %w", err)
	}

	rowsAffected, err := dbResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected in Instances delete operation: %w", err)
	}
	if rowsAffected == 0 {
		return api.ErrNotCompleted
	}

	_, err = tx.ExecContext(ctx, "DELETE FROM durabletask.History WHERE InstanceID = $1", string(id))
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to delete from History table: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// Start implements backend.Backend
func (be *postgresBackend) Start(context.Context) error {
	be.initLock.Lock()
	defer be.initLock.Unlock()

	if be.db == nil {
		be.logger.Debug("Opening database connection")
		db, err := sql.Open("pgx", be.connStr)
		if err != nil {
			return fmt.Errorf("failed to open the database: %w", err)
		}
		be.db = db
	}
	return nil
}

// Stop implements backend.Backend
func (*postgresBackend) Stop(context.Context) error {
	return nil
}

func (be *postgresBackend) ensureDB(ctx context.Context) error {
	if be.db == nil {
		if err := be.CreateTaskHub(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (be *postgresBackend) String() string {
	return "postgres"
}
