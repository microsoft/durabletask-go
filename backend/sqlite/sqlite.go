package sqlite

import (
	"context"
	"database/sql"
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

	_ "github.com/mattn/go-sqlite3"
)

//go:embed schema.sql
var schema string

var emptyString string = ""

type SqliteOptions struct {
	OrchestrationLockTimeout time.Duration
	ActivityLockTimeout      time.Duration
	FilePath                 string
}

type sqliteBackend struct {
	dsn        string
	db         *sql.DB
	workerName string
	logger     backend.Logger
	options    *SqliteOptions
}

// NewSqliteOptions creates a new options object for the sqlite backend provider.
//
// Specify "" for filePath to configure an in-memory database.
func NewSqliteOptions(filePath string) *SqliteOptions {
	// Default values are provided for required options
	return &SqliteOptions{
		FilePath:                 filePath,
		OrchestrationLockTimeout: 2 * time.Minute,
		ActivityLockTimeout:      2 * time.Minute,
	}
}

// NewSqliteBackend creates a new sqlite-based Backend object.
func NewSqliteBackend(opts *SqliteOptions, logger backend.Logger) backend.Backend {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	pid := os.Getpid()
	uuidStr := uuid.NewString()

	be := &sqliteBackend{
		db:         nil,
		workerName: fmt.Sprintf("%v,%d,%v", hostname, pid, uuidStr),
		options:    opts,
		logger:     logger,
	}

	if opts == nil {
		opts = NewSqliteOptions("")
	}
	if opts.FilePath == "" {
		be.dsn = "file::memory:"
	} else {
		be.dsn = fmt.Sprintf("file:%v", opts.FilePath)
	}
	return be
}

// CreateTaskHub creates the sqlite database and applies the schema
func (be *sqliteBackend) CreateTaskHub(context.Context) error {
	db, err := sql.Open("sqlite3", be.dsn)
	if err != nil {
		panic(fmt.Errorf("failed to open the database: %w", err))
	}

	// Initialize database
	if _, err := db.Exec(schema); err != nil {
		panic(fmt.Errorf("failed to initialize the database: %w", err))
	}

	if be.options.FilePath == "" {
		db.SetMaxOpenConns(1)
	}

	be.db = db

	return nil
}

func (be *sqliteBackend) DeleteTaskHub(ctx context.Context) error {
	be.db = nil

	if be.options.FilePath == "" {
		// In-memory DB
		return nil
	} else {
		// File-system DB
		err := os.Remove(be.options.FilePath)
		if err == nil {
			return nil
		} else if os.IsNotExist(err) {
			return backend.ErrTaskHubNotFound
		} else {
			return err
		}
	}
}

// AbandonOrchestrationWorkItem implements backend.Backend
func (be *sqliteBackend) AbandonOrchestrationWorkItem(ctx context.Context, wi *backend.OrchestrationWorkItem) error {
	if err := be.ensureDB(); err != nil {
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
		"UPDATE NewEvents SET [LockedBy] = NULL, [VisibleTime] = ? WHERE [InstanceID] = ? AND [LockedBy] = ?",
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
		"UPDATE Instances SET [LockedBy] = NULL, [LockExpiration] = NULL WHERE [InstanceID] = ? AND [LockedBy] = ?",
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
func (be *sqliteBackend) CompleteOrchestrationWorkItem(ctx context.Context, wi *backend.OrchestrationWorkItem) error {
	if err := be.ensureDB(); err != nil {
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
	sqlSB.WriteString("UPDATE Instances SET ")

	sqlUpdateArgs := make([]interface{}, 0, 10)
	isCreated := false
	isCompleted := false

	for _, e := range wi.State.NewEvents() {
		if es := e.GetExecutionStarted(); es != nil {
			if isCreated {
				// TODO: Log warning about duplicate start event
				continue
			}
			isCreated = true
			sqlSB.WriteString("[CreatedTime] = ?, [Input] = ?, ")
			sqlUpdateArgs = append(sqlUpdateArgs, e.Timestamp.AsTime())
			sqlUpdateArgs = append(sqlUpdateArgs, es.Input.GetValue())
		} else if ec := e.GetExecutionCompleted(); ec != nil {
			if isCompleted {
				// TODO: Log warning about duplicate completion event
				continue
			}
			isCompleted = true
			sqlSB.WriteString("[CompletedTime] = ?, [Output] = ?, [FailureDetails] = ?, ")
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
		sqlSB.WriteString("[CustomStatus] = ?, ")
		sqlUpdateArgs = append(sqlUpdateArgs, wi.State.CustomStatus.Value)
	}

	// TODO: Support for stickiness, which would extend the LockExpiration
	sqlSB.WriteString("[RuntimeStatus] = ?, [LastUpdatedTime] = ?, [LockExpiration] = NULL WHERE [InstanceID] = ? AND [LockedBy] = ?")
	sqlUpdateArgs = append(sqlUpdateArgs, helpers.ToRuntimeStatusString(wi.State.RuntimeStatus()), now, string(wi.InstanceID), wi.LockedBy)

	result, err := tx.ExecContext(ctx, sqlSB.String(), sqlUpdateArgs...)
	if err != nil {
		return fmt.Errorf("failed to update Instances table: %w", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get the number of rows affected by the Instance table update: %w", err)
	} else if count == 0 {
		return fmt.Errorf("instance '%s' no longer exists or was locked by a different worker!", string(wi.InstanceID))
	}

	// If continue-as-new, delete all existing history
	if wi.State.ContinuedAsNew() {
		if _, err := tx.ExecContext(ctx, "DELETE FROM History WHERE InstanceID = ?", string(wi.InstanceID)); err != nil {
			return fmt.Errorf("failed to delete from History table: %w", err)
		}
	}

	// Save new history events
	newHistoryCount := len(wi.State.NewEvents())
	if newHistoryCount > 0 {
		query := "INSERT INTO History ([InstanceID], [SequenceNumber], [EventPayload]) VALUES (?, ?, ?)" +
			strings.Repeat(", (?, ?, ?)", newHistoryCount-1)

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

		_, err = tx.ExecContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("failed to insert into the History table: %w", err)
		}
	}

	// Save outbound activity tasks
	newActivityCount := len(wi.State.PendingTasks())
	if newActivityCount > 0 {
		insertSql := "INSERT INTO NewTasks ([InstanceID], [EventPayload]) VALUES (?, ?)" +
			strings.Repeat(", (?, ?)", newActivityCount-1)

		sqlInsertArgs := make([]interface{}, 0, newActivityCount*2)
		for _, e := range wi.State.PendingTasks() {
			eventPayload, err := backend.MarshalHistoryEvent(e)
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
		insertSql := "INSERT INTO NewEvents ([InstanceID], [EventPayload], [VisibleTime]) VALUES (?, ?, ?)" +
			strings.Repeat(", (?, ?, ?)", newEventCount-1)

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

			eventPayload, err := backend.MarshalHistoryEvent(msg.HistoryEvent)
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
		"DELETE FROM NewEvents WHERE [InstanceID] = ? AND [LockedBy] = ?",
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
func (be *sqliteBackend) CreateOrchestrationInstance(ctx context.Context, e *backend.HistoryEvent) error {
	if err := be.ensureDB(); err != nil {
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

	eventPayload, err := backend.MarshalHistoryEvent(e)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO NewEvents ([InstanceID], [EventPayload]) VALUES (?, ?)`,
		instanceID,
		eventPayload,
	)

	if err != nil {
		return fmt.Errorf("failed to insert row into [NewEvents] table: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to create orchestration: %w", err)
	}

	return nil
}

func (be *sqliteBackend) createOrchestrationInstanceInternal(ctx context.Context, e *backend.HistoryEvent, tx *sql.Tx, instanceID *string) error {
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
		`INSERT OR IGNORE INTO [Instances] (
			[Name],
			[Version],
			[InstanceID],
			[ExecutionID],
			[Input],
			[RuntimeStatus],
			[CreatedTime]
		) VALUES (?, ?, ?, ?, ?, ?, ?)`,
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

func (be *sqliteBackend) AddNewOrchestrationEvent(ctx context.Context, iid api.InstanceID, e *backend.HistoryEvent) error {
	if e == nil {
		return errors.New("HistoryEvent must be non-nil")
	} else if e.Timestamp == nil {
		return errors.New("HistoryEvent must have a non-nil timestamp")
	}

	eventPayload, err := backend.MarshalHistoryEvent(e)
	if err != nil {
		return err
	}

	_, err = be.db.ExecContext(
		ctx,
		`INSERT INTO NewEvents ([InstanceID], [EventPayload]) VALUES (?, ?)`,
		string(iid),
		eventPayload,
	)

	if err != nil {
		return fmt.Errorf("failed to insert row into [NewEvents] table: %w", err)
	}

	return nil
}

// GetOrchestrationMetadata implements backend.Backend
func (be *sqliteBackend) GetOrchestrationMetadata(ctx context.Context, iid api.InstanceID) (*api.OrchestrationMetadata, error) {
	if err := be.ensureDB(); err != nil {
		return nil, err
	}

	row := be.db.QueryRowContext(
		ctx,
		`SELECT [InstanceID], [Name], [RuntimeStatus], [CreatedTime], [LastUpdatedTime], [Input], [Output], [CustomStatus], [FailureDetails]
		FROM Instances WHERE [InstanceID] = ?`,
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
func (be *sqliteBackend) GetOrchestrationRuntimeState(ctx context.Context, wi *backend.OrchestrationWorkItem) (*backend.OrchestrationRuntimeState, error) {
	if err := be.ensureDB(); err != nil {
		return nil, err
	}

	rows, err := be.db.QueryContext(
		ctx,
		"SELECT [EventPayload] FROM History WHERE [InstanceID] = ? ORDER BY [SequenceNumber] ASC",
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
func (be *sqliteBackend) GetOrchestrationWorkItem(ctx context.Context) (*backend.OrchestrationWorkItem, error) {
	if err := be.ensureDB(); err != nil {
		return nil, err
	}

	tx, err := be.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	newLockExpiration := now.Add(be.options.OrchestrationLockTimeout)

	// Place a lock on an orchestration instance that has new events that are ready to be executed.
	row := tx.QueryRowContext(
		ctx,
		`UPDATE Instances SET [LockedBy] = ?, [LockExpiration] = ?
		WHERE [rowid] = (
			SELECT [rowid] FROM Instances I
			WHERE (I.[LockExpiration] IS NULL OR I.[LockExpiration] < ?) AND EXISTS (
				SELECT 1 FROM NewEvents E
				WHERE E.[InstanceID] = I.[InstanceID] AND (E.[VisibleTime] IS NULL OR E.[VisibleTime] < ?)
			)
			LIMIT 1
		) RETURNING [InstanceID]`,
		be.workerName,     // LockedBy for Instances table
		newLockExpiration, // Updated LockExpiration for Instances table
		now,               // LockExpiration for Instances table
		now,               // VisibleTime for NewEvents table
	)

	if err := row.Err(); err != nil {
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
		`UPDATE NewEvents SET [DequeueCount] = [DequeueCount] + 1, [LockedBy] = ? WHERE rowid IN (
			SELECT rowid FROM NewEvents
			WHERE [InstanceID] = ? AND ([VisibleTime] IS NULL OR [VisibleTime] <= ?)
			LIMIT 1000
		)
		RETURNING [EventPayload], [DequeueCount]`,
		be.workerName,
		instanceID,
		now,
	)

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

	if err = tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to fetch orchestration work-item: %w", err)
	}

	wi := &backend.OrchestrationWorkItem{
		InstanceID: api.InstanceID(instanceID),
		NewEvents:  newEvents,
		LockedBy:   be.workerName,
		RetryCount: maxDequeueCount - 1,
	}

	return wi, nil
}

func (be *sqliteBackend) GetActivityWorkItem(ctx context.Context) (*backend.ActivityWorkItem, error) {
	if err := be.ensureDB(); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	newLockExpiration := now.Add(be.options.OrchestrationLockTimeout)

	row := be.db.QueryRowContext(
		ctx,
		`UPDATE NewTasks SET [LockedBy] = ?, [LockExpiration] = ?, [DequeueCount] = [DequeueCount] + 1
		WHERE [SequenceNumber] = (
			SELECT [SequenceNumber] FROM NewTasks T
			WHERE T.[LockExpiration] IS NULL OR T.[LockExpiration] < ?
			LIMIT 1
		) RETURNING [SequenceNumber], [InstanceID], [EventPayload]`,
		be.workerName,
		newLockExpiration,
		now,
	)

	if err := row.Err(); err != nil {
		return nil, fmt.Errorf("failed to query for activity work-items: %w", err)
	}

	var sequenceNumber int64
	var instanceID string
	var eventPayload []byte

	if err := row.Scan(&sequenceNumber, &instanceID, &eventPayload); err != nil {
		if err == sql.ErrNoRows {
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

func (be *sqliteBackend) CompleteActivityWorkItem(ctx context.Context, wi *backend.ActivityWorkItem) error {
	if err := be.ensureDB(); err != nil {
		return err
	}

	tx, err := be.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bytes, err := backend.MarshalHistoryEvent(wi.Result)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, "INSERT INTO NewEvents ([InstanceID], [EventPayload]) VALUES (?, ?)", string(wi.InstanceID), bytes)
	if err != nil {
		return fmt.Errorf("failed to insert into NewEvents table: %w", err)
	}

	dbResult, err := tx.ExecContext(ctx, "DELETE FROM NewTasks WHERE [SequenceNumber] = ? AND [LockedBy] = ?", wi.SequenceNumber, wi.LockedBy)
	if err != nil {
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

func (be *sqliteBackend) AbandonActivityWorkItem(ctx context.Context, wi *backend.ActivityWorkItem) error {
	if err := be.ensureDB(); err != nil {
		return err
	}

	dbResult, err := be.db.ExecContext(
		ctx,
		"UPDATE NewTasks SET [LockedBy] = NULL, [LockExpiration] = NULL WHERE [SequenceNumber] = ? AND [LockedBy] = ?",
		wi.SequenceNumber,
		wi.LockedBy,
	)
	if err != nil {
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

// Start implements backend.Backend
func (*sqliteBackend) Start(context.Context) error {
	return nil
}

// Stop implements backend.Backend
func (*sqliteBackend) Stop(context.Context) error {
	return nil
}

func (be *sqliteBackend) ensureDB() error {
	if be.db == nil {
		return backend.ErrNotInitialized
	}
	return nil
}

func (be *sqliteBackend) String() string {
	return fmt.Sprintf("sqlite::%s", be.options.FilePath)
}
