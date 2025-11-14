CREATE TABLE IF NOT EXISTS Instances (
    SequenceNumber SERIAL,
    
    InstanceID TEXT PRIMARY KEY NOT NULL,
    ExecutionID TEXT NOT NULL,
    Name TEXT NOT NULL, -- the type name of the orchestration or entity
    Version TEXT NULL, -- the version of the orchestration (optional)
    RuntimeStatus TEXT NOT NULL,
    CreatedTime TIMESTAMP NOT NULL DEFAULT NOW(),
    LastUpdatedTime TIMESTAMP NOT NULL DEFAULT NOW(),
    CompletedTime TIMESTAMP NULL,
    LockedBy TEXT NULL,
    LockExpiration TIMESTAMP NULL,
    Input TEXT NULL,
    Output TEXT NULL,
    CustomStatus TEXT NULL,
    FailureDetails BYTEA NULL,
    ParentInstanceID TEXT NULL
);

-- This index is used to improve queries with ORDER BY Instances.SequenceNumber
CREATE INDEX IF NOT EXISTS IX_Instances_SequenceNumber ON Instances(SequenceNumber);

-- This index is used by LockNext and Purge logic
CREATE INDEX IF NOT EXISTS IX_Instances_RuntimeStatus ON Instances(RuntimeStatus);

-- This index is intended to help the performance of multi-instance query
CREATE INDEX IF NOT EXISTS IX_Instances_CreatedTime ON Instances(CreatedTime);

CREATE TABLE IF NOT EXISTS History (
    InstanceID TEXT NOT NULL,
    SequenceNumber SERIAL NOT NULL,
    EventPayload BYTEA NOT NULL,

    PRIMARY KEY (InstanceID, SequenceNumber)
);

CREATE TABLE IF NOT EXISTS NewEvents (
    SequenceNumber SERIAL PRIMARY KEY, -- order is important for FIFO
    InstanceID TEXT NOT NULL,
    ExecutionID TEXT NULL,
    Timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    VisibleTime TIMESTAMP NULL, -- for scheduled or abandoned messages
    DequeueCount INTEGER NOT NULL DEFAULT 0,
    LockedBy TEXT NULL,
    EventPayload BYTEA NOT NULL,

    UNIQUE (InstanceID, SequenceNumber)
);

CREATE TABLE IF NOT EXISTS NewTasks (
    SequenceNumber SERIAL PRIMARY KEY,  -- order is important for FIFO
    InstanceID TEXT NOT NULL,
    ExecutionID TEXT NULL,
    Timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    DequeueCount INTEGER NOT NULL DEFAULT 0,
    LockedBy TEXT NULL,
    LockExpiration TIMESTAMP NULL,
    EventPayload BYTEA NOT NULL
);