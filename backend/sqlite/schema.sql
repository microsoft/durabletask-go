CREATE TABLE IF NOT EXISTS Instances (
    [InstanceID] TEXT PRIMARY KEY NOT NULL,
    [ExecutionID] TEXT NOT NULL,
    [Name] TEXT NOT NULL, -- the type name of the orchestration or entity
    [Version] TEXT NULL, -- the version of the orchestration (optional)
    [RuntimeStatus] TEXT NOT NULL,
    [CreatedTime] DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    [LastUpdatedTime] DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    [CompletedTime] DATETIME NULL,
    [LockedBy] TEXT NULL,
    [LockExpiration] DATETIME NULL,
    [Input] TEXT NULL,
    [Output] TEXT NULL,
    [CustomStatus] TEXT NULL,
    [FailureDetails] BLOB NULL,
    [ParentInstanceID] TEXT NULL
);

-- This index is used by LockNext and Purge logic
CREATE INDEX IF NOT EXISTS IX_Instances_RuntimeStatus ON Instances(RuntimeStatus);

-- This index is intended to help the performance of multi-instance query
CREATE INDEX IF NOT EXISTS IX_Instances_CreatedTime ON Instances(CreatedTime);

CREATE TABLE IF NOT EXISTS History (
    [InstanceID] TEXT NOT NULL,
    [SequenceNumber] INTEGER NOT NULL,
    [EventPayload] BLOB NOT NULL,

    CONSTRAINT PK_History PRIMARY KEY (InstanceID, SequenceNumber)
);

CREATE TABLE IF NOT EXISTS NewEvents (
    [SequenceNumber] INTEGER PRIMARY KEY, -- order is important for FIFO
    [InstanceID] TEXT NOT NULL,
    [ExecutionID] TEXT NULL,
    [Timestamp] DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    [VisibleTime] DATETIME NULL, -- for scheduled or abandoned messages
    [DequeueCount] INTEGER NOT NULL DEFAULT 0,
    [LockedBy] TEXT NULL,
    [EventPayload] BLOB NOT NULL,

    CONSTRAINT UX_NewEvents UNIQUE (InstanceID, SequenceNumber)
);

CREATE TABLE IF NOT EXISTS NewTasks (
    [SequenceNumber] INTEGER PRIMARY KEY,  -- order is important for FIFO
    [InstanceID] TEXT NOT NULL,
    [ExecutionID] TEXT NULL,
    [Timestamp] DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    [DequeueCount] INTEGER NOT NULL DEFAULT 0,
    [LockedBy] TEXT NULL,
    [LockExpiration] DATETIME NULL,
    [EventPayload] BLOB NOT NULL
);