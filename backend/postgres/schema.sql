CREATE SCHEMA IF NOT EXISTS durabletask;

CREATE TABLE IF NOT EXISTS durabletask.Instances (
    InstanceID VARCHAR PRIMARY KEY NOT NULL,
    ExecutionID VARCHAR NOT NULL,
    Name VARCHAR NOT NULL, -- the type name of the orchestration or entity
    Version VARCHAR NULL, -- the version of the orchestration (optional)
    RuntimeStatus VARCHAR NOT NULL,
    CreatedTime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    LastUpdatedTime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CompletedTime TIMESTAMP NULL,
    LockedBy VARCHAR NULL,
    LockExpiration TIMESTAMP NULL,
    Input VARCHAR NULL,
    Output VARCHAR NULL,
    CustomStatus VARCHAR NULL,
    FailureDetails JSON NULL,
    ParentInstanceID VARCHAR NULL
);

-- This index is used by LockNext and Purge logic
CREATE INDEX IF NOT EXISTS IX_Instances_RuntimeStatus ON durabletask.Instances(RuntimeStatus);

-- This index is intended to help the performance of multi-instance query
CREATE INDEX IF NOT EXISTS IX_Instances_CreatedTime ON durabletask.Instances(CreatedTime);

CREATE TABLE IF NOT EXISTS durabletask.History (
    InstanceID VARCHAR NOT NULL,
    SequenceNumber INTEGER NOT NULL,
    EventPayload JSON NOT NULL,

    CONSTRAINT PK_History PRIMARY KEY (InstanceID, SequenceNumber)
);

CREATE TABLE IF NOT EXISTS durabletask.NewEvents (
    SequenceNumber BIGSERIAL PRIMARY KEY, -- order is important for FIFO
    InstanceID VARCHAR NOT NULL,
    ExecutionID VARCHAR NULL,
    Timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    VisibleTime TIMESTAMP NULL, -- for scheduled or abandoned messages
    DequeueCount INTEGER NOT NULL DEFAULT 0,
    LockedBy VARCHAR NULL,
    EventPayload JSON NOT NULL,

    CONSTRAINT UX_NewEvents UNIQUE (InstanceID, SequenceNumber)
);

CREATE TABLE IF NOT EXISTS durabletask.NewTasks (
    SequenceNumber BIGSERIAL PRIMARY KEY,  -- order is important for FIFO
    InstanceID VARCHAR NOT NULL,
    ExecutionID VARCHAR NULL,
    Timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    DequeueCount INTEGER NOT NULL DEFAULT 0,
    LockedBy VARCHAR NULL,
    LockExpiration TIMESTAMP NULL,
    EventPayload JSON NOT NULL
);