-- ============================================================================
-- Enhanced Schema for Durable Task Backend (v2)
-- Includes performance optimizations and partitioning
-- Fixed: Storage parameters now applied to individual partitions instead of parent tables
-- Updated: Increased from 8 to 32 partitions for high concurrency (160 connections)
-- ============================================================================

-- ============================================================================
-- Instances Table (with performance optimizations)
-- ============================================================================
CREATE TABLE IF NOT EXISTS Instances (
    SequenceNumber BIGSERIAL,  -- Changed to BIGSERIAL for high scale
    
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
    DequeueCount INTEGER NOT NULL DEFAULT 0,
    Input TEXT NULL,
    Output TEXT NULL,
    CustomStatus TEXT NULL,
    FailureDetails BYTEA NULL,
    ParentInstanceID TEXT NULL
);

ALTER TABLE Instances ADD COLUMN IF NOT EXISTS DequeueCount INTEGER NOT NULL DEFAULT 0;

-- Fillfactor: Reduce page splits for HOT updates (standardized to 70 to match NewEvents/NewTasks)
ALTER TABLE Instances SET (fillfactor = 70);

-- Autovacuum tuning: LockedBy/LockExpiration/RuntimeStatus are indexed, so lock-claim,
-- abandon, and completion updates are not HOT-eligible and leave dead tuples + stale
-- planner stats on the exact table the poll query scans. Match NewEvents/NewTasks tuning.
ALTER TABLE Instances SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

-- Original indexes from base schema
-- Removed IX_Instances_SequenceNumber (covered by partial index IX_Instances_SequenceNumber_WHERE_LockExpiration_IS_NULL)
-- Removed IX_Instances_CreatedTime (not used in any query patterns in postgres.go)
CREATE INDEX IF NOT EXISTS IX_Instances_ParentInstanceID ON Instances(ParentInstanceID);

-- Composite index for suborchestration priority queries (ORDER BY ParentInstanceID, InstanceID, SequenceNumber)
CREATE INDEX IF NOT EXISTS IX_Instances_ParentInstanceID_InstanceID_SequenceNumber ON Instances(ParentInstanceID, InstanceID, SequenceNumber);

-- Performance optimization indexes
CREATE INDEX IF NOT EXISTS IX_Instances_LockExp_SeqNum_WHERE_LockExp_NULL ON Instances(LockExpiration, SequenceNumber)
WHERE LockExpiration IS NULL;

CREATE INDEX IF NOT EXISTS IX_Instances_RuntimeStatus_WHERE_RuntimeStatus_IN_PENDING_RUNNING ON Instances(RuntimeStatus)
WHERE RuntimeStatus IN ('PENDING', 'RUNNING');

-- Full index for ORDER BY InstanceID, SequenceNumber (supports all rows)
CREATE INDEX IF NOT EXISTS IX_Instances_InstanceID_SequenceNumber ON Instances(InstanceID, SequenceNumber);

-- Index for abandon/complete operations (WHERE InstanceID = $1 AND LockedBy = $2)
CREATE INDEX IF NOT EXISTS IX_Instances_InstanceID_LockedBy ON Instances(InstanceID, LockedBy);

-- Index for locking queries with ORDER BY (LockExpiration, InstanceID, SequenceNumber)
CREATE INDEX IF NOT EXISTS IX_Instances_LockExp_ID_SeqNum_WHERE_LockExp_NULL ON Instances(LockExpiration, InstanceID, SequenceNumber)
WHERE LockExpiration IS NULL;

-- Index for batch orchestration queries (ORDER BY SequenceNumber)
CREATE INDEX IF NOT EXISTS IX_Instances_SequenceNumber_WHERE_LockExpiration_IS_NULL ON Instances(SequenceNumber)
WHERE LockExpiration IS NULL;

-- Index for purge operations (WHERE RuntimeStatus IN ('COMPLETED', 'FAILED', 'TERMINATED'))
CREATE INDEX IF NOT EXISTS IX_Instances_RuntimeStatus_WHERE_IN_COMP_FAIL_TERM ON Instances(RuntimeStatus)
WHERE RuntimeStatus IN ('COMPLETED', 'FAILED', 'TERMINATED');

-- Index for reclaiming expired locks (WHERE LockExpiration < now, e.g. crashed/restarted workers)
-- Complements IX_Instances_LockExp_ID_SeqNum_WHERE_LockExp_NULL, which only covers the NULL branch
-- of the poll query's (LockExpiration IS NULL OR LockExpiration < $3) predicate.
CREATE INDEX IF NOT EXISTS IX_Instances_LockExp_NotNull_ID_SeqNum ON Instances(LockExpiration, InstanceID, SequenceNumber)
WHERE LockExpiration IS NOT NULL;

-- Supplementary, more selective poll index bounded to actionable instances (PENDING/RUNNING).
-- Stays small regardless of purge cadence, unlike IX_Instances_LockExp_ID_SeqNum_WHERE_LockExp_NULL
-- which grows with every unpurged completed/failed/terminated instance.
CREATE INDEX IF NOT EXISTS IX_Instances_LockExp_Null_Status_ID_SeqNum ON Instances(LockExpiration, InstanceID, SequenceNumber)
WHERE LockExpiration IS NULL AND RuntimeStatus IN ('PENDING', 'RUNNING');

-- Covering index for GetOrchestrationMetadata reads (SELECT ... WHERE InstanceID = $1).
-- Enables index-only scans for the common status-check case. Excludes Input/Output/FailureDetails
-- (large/variable size) to avoid bloating the index.
CREATE INDEX IF NOT EXISTS IX_Instances_InstanceID_Include_Metadata ON Instances(InstanceID)
INCLUDE (Name, RuntimeStatus, CreatedTime, LastUpdatedTime, CustomStatus);

-- ============================================================================
-- History Table (with partitioning and performance optimizations)
-- ============================================================================
-- Hash-partitioned by InstanceID: every access pattern (reads at
-- GetOrchestrationRuntimeState, inserts, and the purge/continue-as-new deletes)
-- filters WHERE InstanceID = $1, so this always prunes to exactly one partition,
-- unlike Instances/NewTasks whose hottest poll queries scan globally.
CREATE TABLE IF NOT EXISTS History (
    InstanceID TEXT NOT NULL,
    SequenceNumber BIGSERIAL NOT NULL,  -- Changed to BIGSERIAL for high scale
    EventPayload BYTEA NOT NULL,

    PRIMARY KEY (InstanceID, SequenceNumber)
) PARTITION BY HASH (InstanceID);

-- Note: Fillfactor and autovacuum settings must be set on individual partitions, not the parent table

-- Create 128 partitions matching NewEvents for consistency. The PRIMARY KEY (InstanceID, SequenceNumber)
-- already covers the only query pattern (ORDER BY SequenceNumber ASC WHERE InstanceID = $1), so no
-- additional per-partition indexes are needed. Fillfactor=100 (append-only, no UPDATEs) and lighter
-- autovacuum thresholds than NewEvents (dead tuples only from occasional purge/continue-as-new deletes,
-- not constant UPDATE churn).
CREATE TABLE IF NOT EXISTS History_0000 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 0);
ALTER TABLE History_0000 SET (fillfactor = 100);
ALTER TABLE History_0000 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0000 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0001 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 1);
ALTER TABLE History_0001 SET (fillfactor = 100);
ALTER TABLE History_0001 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0001 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0002 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 2);
ALTER TABLE History_0002 SET (fillfactor = 100);
ALTER TABLE History_0002 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0002 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0003 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 3);
ALTER TABLE History_0003 SET (fillfactor = 100);
ALTER TABLE History_0003 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0003 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0004 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 4);
ALTER TABLE History_0004 SET (fillfactor = 100);
ALTER TABLE History_0004 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0004 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0005 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 5);
ALTER TABLE History_0005 SET (fillfactor = 100);
ALTER TABLE History_0005 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0005 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0006 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 6);
ALTER TABLE History_0006 SET (fillfactor = 100);
ALTER TABLE History_0006 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0006 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0007 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 7);
ALTER TABLE History_0007 SET (fillfactor = 100);
ALTER TABLE History_0007 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0007 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0008 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 8);
ALTER TABLE History_0008 SET (fillfactor = 100);
ALTER TABLE History_0008 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0008 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0009 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 9);
ALTER TABLE History_0009 SET (fillfactor = 100);
ALTER TABLE History_0009 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0009 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0010 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 10);
ALTER TABLE History_0010 SET (fillfactor = 100);
ALTER TABLE History_0010 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0010 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0011 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 11);
ALTER TABLE History_0011 SET (fillfactor = 100);
ALTER TABLE History_0011 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0011 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0012 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 12);
ALTER TABLE History_0012 SET (fillfactor = 100);
ALTER TABLE History_0012 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0012 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0013 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 13);
ALTER TABLE History_0013 SET (fillfactor = 100);
ALTER TABLE History_0013 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0013 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0014 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 14);
ALTER TABLE History_0014 SET (fillfactor = 100);
ALTER TABLE History_0014 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0014 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0015 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 15);
ALTER TABLE History_0015 SET (fillfactor = 100);
ALTER TABLE History_0015 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0015 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0016 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 16);
ALTER TABLE History_0016 SET (fillfactor = 100);
ALTER TABLE History_0016 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0016 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0017 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 17);
ALTER TABLE History_0017 SET (fillfactor = 100);
ALTER TABLE History_0017 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0017 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0018 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 18);
ALTER TABLE History_0018 SET (fillfactor = 100);
ALTER TABLE History_0018 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0018 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0019 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 19);
ALTER TABLE History_0019 SET (fillfactor = 100);
ALTER TABLE History_0019 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0019 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0020 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 20);
ALTER TABLE History_0020 SET (fillfactor = 100);
ALTER TABLE History_0020 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0020 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0021 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 21);
ALTER TABLE History_0021 SET (fillfactor = 100);
ALTER TABLE History_0021 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0021 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0022 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 22);
ALTER TABLE History_0022 SET (fillfactor = 100);
ALTER TABLE History_0022 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0022 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0023 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 23);
ALTER TABLE History_0023 SET (fillfactor = 100);
ALTER TABLE History_0023 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0023 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0024 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 24);
ALTER TABLE History_0024 SET (fillfactor = 100);
ALTER TABLE History_0024 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0024 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0025 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 25);
ALTER TABLE History_0025 SET (fillfactor = 100);
ALTER TABLE History_0025 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0025 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0026 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 26);
ALTER TABLE History_0026 SET (fillfactor = 100);
ALTER TABLE History_0026 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0026 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0027 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 27);
ALTER TABLE History_0027 SET (fillfactor = 100);
ALTER TABLE History_0027 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0027 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0028 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 28);
ALTER TABLE History_0028 SET (fillfactor = 100);
ALTER TABLE History_0028 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0028 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0029 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 29);
ALTER TABLE History_0029 SET (fillfactor = 100);
ALTER TABLE History_0029 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0029 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0030 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 30);
ALTER TABLE History_0030 SET (fillfactor = 100);
ALTER TABLE History_0030 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0030 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0031 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 31);
ALTER TABLE History_0031 SET (fillfactor = 100);
ALTER TABLE History_0031 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0031 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0032 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 32);
ALTER TABLE History_0032 SET (fillfactor = 100);
ALTER TABLE History_0032 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0032 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0033 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 33);
ALTER TABLE History_0033 SET (fillfactor = 100);
ALTER TABLE History_0033 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0033 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0034 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 34);
ALTER TABLE History_0034 SET (fillfactor = 100);
ALTER TABLE History_0034 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0034 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0035 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 35);
ALTER TABLE History_0035 SET (fillfactor = 100);
ALTER TABLE History_0035 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0035 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0036 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 36);
ALTER TABLE History_0036 SET (fillfactor = 100);
ALTER TABLE History_0036 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0036 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0037 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 37);
ALTER TABLE History_0037 SET (fillfactor = 100);
ALTER TABLE History_0037 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0037 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0038 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 38);
ALTER TABLE History_0038 SET (fillfactor = 100);
ALTER TABLE History_0038 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0038 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0039 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 39);
ALTER TABLE History_0039 SET (fillfactor = 100);
ALTER TABLE History_0039 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0039 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0040 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 40);
ALTER TABLE History_0040 SET (fillfactor = 100);
ALTER TABLE History_0040 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0040 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0041 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 41);
ALTER TABLE History_0041 SET (fillfactor = 100);
ALTER TABLE History_0041 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0041 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0042 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 42);
ALTER TABLE History_0042 SET (fillfactor = 100);
ALTER TABLE History_0042 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0042 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0043 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 43);
ALTER TABLE History_0043 SET (fillfactor = 100);
ALTER TABLE History_0043 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0043 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0044 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 44);
ALTER TABLE History_0044 SET (fillfactor = 100);
ALTER TABLE History_0044 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0044 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0045 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 45);
ALTER TABLE History_0045 SET (fillfactor = 100);
ALTER TABLE History_0045 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0045 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0046 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 46);
ALTER TABLE History_0046 SET (fillfactor = 100);
ALTER TABLE History_0046 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0046 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0047 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 47);
ALTER TABLE History_0047 SET (fillfactor = 100);
ALTER TABLE History_0047 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0047 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0048 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 48);
ALTER TABLE History_0048 SET (fillfactor = 100);
ALTER TABLE History_0048 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0048 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0049 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 49);
ALTER TABLE History_0049 SET (fillfactor = 100);
ALTER TABLE History_0049 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0049 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0050 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 50);
ALTER TABLE History_0050 SET (fillfactor = 100);
ALTER TABLE History_0050 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0050 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0051 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 51);
ALTER TABLE History_0051 SET (fillfactor = 100);
ALTER TABLE History_0051 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0051 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0052 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 52);
ALTER TABLE History_0052 SET (fillfactor = 100);
ALTER TABLE History_0052 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0052 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0053 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 53);
ALTER TABLE History_0053 SET (fillfactor = 100);
ALTER TABLE History_0053 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0053 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0054 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 54);
ALTER TABLE History_0054 SET (fillfactor = 100);
ALTER TABLE History_0054 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0054 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0055 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 55);
ALTER TABLE History_0055 SET (fillfactor = 100);
ALTER TABLE History_0055 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0055 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0056 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 56);
ALTER TABLE History_0056 SET (fillfactor = 100);
ALTER TABLE History_0056 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0056 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0057 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 57);
ALTER TABLE History_0057 SET (fillfactor = 100);
ALTER TABLE History_0057 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0057 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0058 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 58);
ALTER TABLE History_0058 SET (fillfactor = 100);
ALTER TABLE History_0058 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0058 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0059 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 59);
ALTER TABLE History_0059 SET (fillfactor = 100);
ALTER TABLE History_0059 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0059 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0060 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 60);
ALTER TABLE History_0060 SET (fillfactor = 100);
ALTER TABLE History_0060 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0060 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0061 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 61);
ALTER TABLE History_0061 SET (fillfactor = 100);
ALTER TABLE History_0061 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0061 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0062 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 62);
ALTER TABLE History_0062 SET (fillfactor = 100);
ALTER TABLE History_0062 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0062 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0063 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 63);
ALTER TABLE History_0063 SET (fillfactor = 100);
ALTER TABLE History_0063 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0063 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0064 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 64);
ALTER TABLE History_0064 SET (fillfactor = 100);
ALTER TABLE History_0064 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0064 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0065 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 65);
ALTER TABLE History_0065 SET (fillfactor = 100);
ALTER TABLE History_0065 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0065 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0066 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 66);
ALTER TABLE History_0066 SET (fillfactor = 100);
ALTER TABLE History_0066 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0066 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0067 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 67);
ALTER TABLE History_0067 SET (fillfactor = 100);
ALTER TABLE History_0067 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0067 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0068 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 68);
ALTER TABLE History_0068 SET (fillfactor = 100);
ALTER TABLE History_0068 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0068 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0069 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 69);
ALTER TABLE History_0069 SET (fillfactor = 100);
ALTER TABLE History_0069 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0069 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0070 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 70);
ALTER TABLE History_0070 SET (fillfactor = 100);
ALTER TABLE History_0070 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0070 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0071 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 71);
ALTER TABLE History_0071 SET (fillfactor = 100);
ALTER TABLE History_0071 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0071 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0072 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 72);
ALTER TABLE History_0072 SET (fillfactor = 100);
ALTER TABLE History_0072 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0072 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0073 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 73);
ALTER TABLE History_0073 SET (fillfactor = 100);
ALTER TABLE History_0073 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0073 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0074 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 74);
ALTER TABLE History_0074 SET (fillfactor = 100);
ALTER TABLE History_0074 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0074 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0075 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 75);
ALTER TABLE History_0075 SET (fillfactor = 100);
ALTER TABLE History_0075 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0075 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0076 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 76);
ALTER TABLE History_0076 SET (fillfactor = 100);
ALTER TABLE History_0076 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0076 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0077 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 77);
ALTER TABLE History_0077 SET (fillfactor = 100);
ALTER TABLE History_0077 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0077 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0078 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 78);
ALTER TABLE History_0078 SET (fillfactor = 100);
ALTER TABLE History_0078 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0078 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0079 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 79);
ALTER TABLE History_0079 SET (fillfactor = 100);
ALTER TABLE History_0079 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0079 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0080 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 80);
ALTER TABLE History_0080 SET (fillfactor = 100);
ALTER TABLE History_0080 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0080 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0081 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 81);
ALTER TABLE History_0081 SET (fillfactor = 100);
ALTER TABLE History_0081 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0081 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0082 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 82);
ALTER TABLE History_0082 SET (fillfactor = 100);
ALTER TABLE History_0082 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0082 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0083 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 83);
ALTER TABLE History_0083 SET (fillfactor = 100);
ALTER TABLE History_0083 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0083 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0084 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 84);
ALTER TABLE History_0084 SET (fillfactor = 100);
ALTER TABLE History_0084 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0084 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0085 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 85);
ALTER TABLE History_0085 SET (fillfactor = 100);
ALTER TABLE History_0085 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0085 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0086 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 86);
ALTER TABLE History_0086 SET (fillfactor = 100);
ALTER TABLE History_0086 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0086 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0087 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 87);
ALTER TABLE History_0087 SET (fillfactor = 100);
ALTER TABLE History_0087 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0087 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0088 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 88);
ALTER TABLE History_0088 SET (fillfactor = 100);
ALTER TABLE History_0088 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0088 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0089 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 89);
ALTER TABLE History_0089 SET (fillfactor = 100);
ALTER TABLE History_0089 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0089 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0090 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 90);
ALTER TABLE History_0090 SET (fillfactor = 100);
ALTER TABLE History_0090 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0090 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0091 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 91);
ALTER TABLE History_0091 SET (fillfactor = 100);
ALTER TABLE History_0091 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0091 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0092 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 92);
ALTER TABLE History_0092 SET (fillfactor = 100);
ALTER TABLE History_0092 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0092 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0093 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 93);
ALTER TABLE History_0093 SET (fillfactor = 100);
ALTER TABLE History_0093 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0093 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0094 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 94);
ALTER TABLE History_0094 SET (fillfactor = 100);
ALTER TABLE History_0094 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0094 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0095 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 95);
ALTER TABLE History_0095 SET (fillfactor = 100);
ALTER TABLE History_0095 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0095 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0096 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 96);
ALTER TABLE History_0096 SET (fillfactor = 100);
ALTER TABLE History_0096 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0096 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0097 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 97);
ALTER TABLE History_0097 SET (fillfactor = 100);
ALTER TABLE History_0097 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0097 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0098 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 98);
ALTER TABLE History_0098 SET (fillfactor = 100);
ALTER TABLE History_0098 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0098 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0099 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 99);
ALTER TABLE History_0099 SET (fillfactor = 100);
ALTER TABLE History_0099 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0099 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0100 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 100);
ALTER TABLE History_0100 SET (fillfactor = 100);
ALTER TABLE History_0100 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0100 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0101 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 101);
ALTER TABLE History_0101 SET (fillfactor = 100);
ALTER TABLE History_0101 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0101 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0102 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 102);
ALTER TABLE History_0102 SET (fillfactor = 100);
ALTER TABLE History_0102 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0102 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0103 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 103);
ALTER TABLE History_0103 SET (fillfactor = 100);
ALTER TABLE History_0103 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0103 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0104 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 104);
ALTER TABLE History_0104 SET (fillfactor = 100);
ALTER TABLE History_0104 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0104 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0105 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 105);
ALTER TABLE History_0105 SET (fillfactor = 100);
ALTER TABLE History_0105 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0105 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0106 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 106);
ALTER TABLE History_0106 SET (fillfactor = 100);
ALTER TABLE History_0106 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0106 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0107 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 107);
ALTER TABLE History_0107 SET (fillfactor = 100);
ALTER TABLE History_0107 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0107 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0108 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 108);
ALTER TABLE History_0108 SET (fillfactor = 100);
ALTER TABLE History_0108 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0108 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0109 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 109);
ALTER TABLE History_0109 SET (fillfactor = 100);
ALTER TABLE History_0109 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0109 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0110 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 110);
ALTER TABLE History_0110 SET (fillfactor = 100);
ALTER TABLE History_0110 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0110 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0111 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 111);
ALTER TABLE History_0111 SET (fillfactor = 100);
ALTER TABLE History_0111 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0111 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0112 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 112);
ALTER TABLE History_0112 SET (fillfactor = 100);
ALTER TABLE History_0112 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0112 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0113 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 113);
ALTER TABLE History_0113 SET (fillfactor = 100);
ALTER TABLE History_0113 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0113 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0114 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 114);
ALTER TABLE History_0114 SET (fillfactor = 100);
ALTER TABLE History_0114 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0114 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0115 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 115);
ALTER TABLE History_0115 SET (fillfactor = 100);
ALTER TABLE History_0115 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0115 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0116 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 116);
ALTER TABLE History_0116 SET (fillfactor = 100);
ALTER TABLE History_0116 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0116 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0117 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 117);
ALTER TABLE History_0117 SET (fillfactor = 100);
ALTER TABLE History_0117 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0117 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0118 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 118);
ALTER TABLE History_0118 SET (fillfactor = 100);
ALTER TABLE History_0118 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0118 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0119 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 119);
ALTER TABLE History_0119 SET (fillfactor = 100);
ALTER TABLE History_0119 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0119 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0120 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 120);
ALTER TABLE History_0120 SET (fillfactor = 100);
ALTER TABLE History_0120 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0120 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0121 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 121);
ALTER TABLE History_0121 SET (fillfactor = 100);
ALTER TABLE History_0121 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0121 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0122 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 122);
ALTER TABLE History_0122 SET (fillfactor = 100);
ALTER TABLE History_0122 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0122 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0123 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 123);
ALTER TABLE History_0123 SET (fillfactor = 100);
ALTER TABLE History_0123 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0123 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0124 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 124);
ALTER TABLE History_0124 SET (fillfactor = 100);
ALTER TABLE History_0124 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0124 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0125 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 125);
ALTER TABLE History_0125 SET (fillfactor = 100);
ALTER TABLE History_0125 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0125 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0126 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 126);
ALTER TABLE History_0126 SET (fillfactor = 100);
ALTER TABLE History_0126 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0126 SET (parallel_workers = 4);

CREATE TABLE IF NOT EXISTS History_0127 PARTITION OF History FOR VALUES WITH (MODULUS 128, REMAINDER 127);
ALTER TABLE History_0127 SET (fillfactor = 100);
ALTER TABLE History_0127 SET (autovacuum_vacuum_scale_factor = 0.1, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_scale_factor = 0.1, autovacuum_analyze_threshold = 1000);
ALTER TABLE History_0127 SET (parallel_workers = 4);

-- ============================================================================
-- NewEvents Table (with partitioning and performance optimizations)
-- ============================================================================
CREATE TABLE IF NOT EXISTS NewEvents (
    SequenceNumber BIGSERIAL,  -- Changed to BIGSERIAL for high scale
    InstanceID TEXT NOT NULL,
    ExecutionID TEXT NULL,
    Timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    VisibleTime TIMESTAMP NULL, -- for scheduled or abandoned messages
    DequeueCount INTEGER NOT NULL DEFAULT 0,
    LockedBy TEXT NULL,
    EventPayload BYTEA NOT NULL,
    UNIQUE (InstanceID, SequenceNumber)
) PARTITION BY HASH (InstanceID);

-- Note: Fillfactor and autovacuum settings must be set on individual partitions, not the parent table

-- Create 128 partitions for parallelism with storage parameters
CREATE TABLE IF NOT EXISTS NewEvents_0000 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 0);
ALTER TABLE NewEvents_0000 SET (fillfactor = 70);
ALTER TABLE NewEvents_0000 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0001 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 1);
ALTER TABLE NewEvents_0001 SET (fillfactor = 70);
ALTER TABLE NewEvents_0001 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0002 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 2);
ALTER TABLE NewEvents_0002 SET (fillfactor = 70);
ALTER TABLE NewEvents_0002 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0003 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 3);
ALTER TABLE NewEvents_0003 SET (fillfactor = 70);
ALTER TABLE NewEvents_0003 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0004 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 4);
ALTER TABLE NewEvents_0004 SET (fillfactor = 70);
ALTER TABLE NewEvents_0004 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0005 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 5);
ALTER TABLE NewEvents_0005 SET (fillfactor = 70);
ALTER TABLE NewEvents_0005 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0006 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 6);
ALTER TABLE NewEvents_0006 SET (fillfactor = 70);
ALTER TABLE NewEvents_0006 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0007 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 7);
ALTER TABLE NewEvents_0007 SET (fillfactor = 70);
ALTER TABLE NewEvents_0007 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0008 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 8);
ALTER TABLE NewEvents_0008 SET (fillfactor = 70);
ALTER TABLE NewEvents_0008 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0009 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 9);
ALTER TABLE NewEvents_0009 SET (fillfactor = 70);
ALTER TABLE NewEvents_0009 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0010 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 10);
ALTER TABLE NewEvents_0010 SET (fillfactor = 70);
ALTER TABLE NewEvents_0010 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0011 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 11);
ALTER TABLE NewEvents_0011 SET (fillfactor = 70);
ALTER TABLE NewEvents_0011 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0012 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 12);
ALTER TABLE NewEvents_0012 SET (fillfactor = 70);
ALTER TABLE NewEvents_0012 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0013 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 13);
ALTER TABLE NewEvents_0013 SET (fillfactor = 70);
ALTER TABLE NewEvents_0013 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0014 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 14);
ALTER TABLE NewEvents_0014 SET (fillfactor = 70);
ALTER TABLE NewEvents_0014 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0015 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 15);
ALTER TABLE NewEvents_0015 SET (fillfactor = 70);
ALTER TABLE NewEvents_0015 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0016 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 16);
ALTER TABLE NewEvents_0016 SET (fillfactor = 70);
ALTER TABLE NewEvents_0016 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0017 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 17);
ALTER TABLE NewEvents_0017 SET (fillfactor = 70);
ALTER TABLE NewEvents_0017 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0018 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 18);
ALTER TABLE NewEvents_0018 SET (fillfactor = 70);
ALTER TABLE NewEvents_0018 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0019 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 19);
ALTER TABLE NewEvents_0019 SET (fillfactor = 70);
ALTER TABLE NewEvents_0019 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0020 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 20);
ALTER TABLE NewEvents_0020 SET (fillfactor = 70);
ALTER TABLE NewEvents_0020 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0021 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 21);
ALTER TABLE NewEvents_0021 SET (fillfactor = 70);
ALTER TABLE NewEvents_0021 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0022 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 22);
ALTER TABLE NewEvents_0022 SET (fillfactor = 70);
ALTER TABLE NewEvents_0022 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0023 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 23);
ALTER TABLE NewEvents_0023 SET (fillfactor = 70);
ALTER TABLE NewEvents_0023 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0024 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 24);
ALTER TABLE NewEvents_0024 SET (fillfactor = 70);
ALTER TABLE NewEvents_0024 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0025 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 25);
ALTER TABLE NewEvents_0025 SET (fillfactor = 70);
ALTER TABLE NewEvents_0025 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0026 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 26);
ALTER TABLE NewEvents_0026 SET (fillfactor = 70);
ALTER TABLE NewEvents_0026 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0027 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 27);
ALTER TABLE NewEvents_0027 SET (fillfactor = 70);
ALTER TABLE NewEvents_0027 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0028 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 28);
ALTER TABLE NewEvents_0028 SET (fillfactor = 70);
ALTER TABLE NewEvents_0028 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0029 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 29);
ALTER TABLE NewEvents_0029 SET (fillfactor = 70);
ALTER TABLE NewEvents_0029 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0030 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 30);
ALTER TABLE NewEvents_0030 SET (fillfactor = 70);
ALTER TABLE NewEvents_0030 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0031 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 31);
ALTER TABLE NewEvents_0031 SET (fillfactor = 70);
ALTER TABLE NewEvents_0031 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0032 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 32);
ALTER TABLE NewEvents_0032 SET (fillfactor = 70);
ALTER TABLE NewEvents_0032 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0033 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 33);
ALTER TABLE NewEvents_0033 SET (fillfactor = 70);
ALTER TABLE NewEvents_0033 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0034 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 34);
ALTER TABLE NewEvents_0034 SET (fillfactor = 70);
ALTER TABLE NewEvents_0034 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0035 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 35);
ALTER TABLE NewEvents_0035 SET (fillfactor = 70);
ALTER TABLE NewEvents_0035 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0036 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 36);
ALTER TABLE NewEvents_0036 SET (fillfactor = 70);
ALTER TABLE NewEvents_0036 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0037 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 37);
ALTER TABLE NewEvents_0037 SET (fillfactor = 70);
ALTER TABLE NewEvents_0037 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0038 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 38);
ALTER TABLE NewEvents_0038 SET (fillfactor = 70);
ALTER TABLE NewEvents_0038 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0039 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 39);
ALTER TABLE NewEvents_0039 SET (fillfactor = 70);
ALTER TABLE NewEvents_0039 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0040 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 40);
ALTER TABLE NewEvents_0040 SET (fillfactor = 70);
ALTER TABLE NewEvents_0040 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0041 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 41);
ALTER TABLE NewEvents_0041 SET (fillfactor = 70);
ALTER TABLE NewEvents_0041 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0042 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 42);
ALTER TABLE NewEvents_0042 SET (fillfactor = 70);
ALTER TABLE NewEvents_0042 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0043 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 43);
ALTER TABLE NewEvents_0043 SET (fillfactor = 70);
ALTER TABLE NewEvents_0043 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0044 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 44);
ALTER TABLE NewEvents_0044 SET (fillfactor = 70);
ALTER TABLE NewEvents_0044 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0045 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 45);
ALTER TABLE NewEvents_0045 SET (fillfactor = 70);
ALTER TABLE NewEvents_0045 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0046 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 46);
ALTER TABLE NewEvents_0046 SET (fillfactor = 70);
ALTER TABLE NewEvents_0046 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0047 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 47);
ALTER TABLE NewEvents_0047 SET (fillfactor = 70);
ALTER TABLE NewEvents_0047 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0048 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 48);
ALTER TABLE NewEvents_0048 SET (fillfactor = 70);
ALTER TABLE NewEvents_0048 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0049 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 49);
ALTER TABLE NewEvents_0049 SET (fillfactor = 70);
ALTER TABLE NewEvents_0049 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0050 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 50);
ALTER TABLE NewEvents_0050 SET (fillfactor = 70);
ALTER TABLE NewEvents_0050 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0051 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 51);
ALTER TABLE NewEvents_0051 SET (fillfactor = 70);
ALTER TABLE NewEvents_0051 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0052 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 52);
ALTER TABLE NewEvents_0052 SET (fillfactor = 70);
ALTER TABLE NewEvents_0052 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0053 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 53);
ALTER TABLE NewEvents_0053 SET (fillfactor = 70);
ALTER TABLE NewEvents_0053 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0054 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 54);
ALTER TABLE NewEvents_0054 SET (fillfactor = 70);
ALTER TABLE NewEvents_0054 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0055 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 55);
ALTER TABLE NewEvents_0055 SET (fillfactor = 70);
ALTER TABLE NewEvents_0055 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0056 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 56);
ALTER TABLE NewEvents_0056 SET (fillfactor = 70);
ALTER TABLE NewEvents_0056 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0057 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 57);
ALTER TABLE NewEvents_0057 SET (fillfactor = 70);
ALTER TABLE NewEvents_0057 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0058 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 58);
ALTER TABLE NewEvents_0058 SET (fillfactor = 70);
ALTER TABLE NewEvents_0058 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0059 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 59);
ALTER TABLE NewEvents_0059 SET (fillfactor = 70);
ALTER TABLE NewEvents_0059 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0060 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 60);
ALTER TABLE NewEvents_0060 SET (fillfactor = 70);
ALTER TABLE NewEvents_0060 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0061 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 61);
ALTER TABLE NewEvents_0061 SET (fillfactor = 70);
ALTER TABLE NewEvents_0061 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0062 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 62);
ALTER TABLE NewEvents_0062 SET (fillfactor = 70);
ALTER TABLE NewEvents_0062 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0063 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 63);
ALTER TABLE NewEvents_0063 SET (fillfactor = 70);
ALTER TABLE NewEvents_0063 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0064 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 64);
ALTER TABLE NewEvents_0064 SET (fillfactor = 70);
ALTER TABLE NewEvents_0064 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0065 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 65);
ALTER TABLE NewEvents_0065 SET (fillfactor = 70);
ALTER TABLE NewEvents_0065 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0066 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 66);
ALTER TABLE NewEvents_0066 SET (fillfactor = 70);
ALTER TABLE NewEvents_0066 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0067 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 67);
ALTER TABLE NewEvents_0067 SET (fillfactor = 70);
ALTER TABLE NewEvents_0067 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0068 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 68);
ALTER TABLE NewEvents_0068 SET (fillfactor = 70);
ALTER TABLE NewEvents_0068 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0069 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 69);
ALTER TABLE NewEvents_0069 SET (fillfactor = 70);
ALTER TABLE NewEvents_0069 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0070 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 70);
ALTER TABLE NewEvents_0070 SET (fillfactor = 70);
ALTER TABLE NewEvents_0070 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0071 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 71);
ALTER TABLE NewEvents_0071 SET (fillfactor = 70);
ALTER TABLE NewEvents_0071 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0072 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 72);
ALTER TABLE NewEvents_0072 SET (fillfactor = 70);
ALTER TABLE NewEvents_0072 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0073 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 73);
ALTER TABLE NewEvents_0073 SET (fillfactor = 70);
ALTER TABLE NewEvents_0073 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0074 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 74);
ALTER TABLE NewEvents_0074 SET (fillfactor = 70);
ALTER TABLE NewEvents_0074 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0075 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 75);
ALTER TABLE NewEvents_0075 SET (fillfactor = 70);
ALTER TABLE NewEvents_0075 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0076 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 76);
ALTER TABLE NewEvents_0076 SET (fillfactor = 70);
ALTER TABLE NewEvents_0076 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0077 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 77);
ALTER TABLE NewEvents_0077 SET (fillfactor = 70);
ALTER TABLE NewEvents_0077 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0078 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 78);
ALTER TABLE NewEvents_0078 SET (fillfactor = 70);
ALTER TABLE NewEvents_0078 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0079 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 79);
ALTER TABLE NewEvents_0079 SET (fillfactor = 70);
ALTER TABLE NewEvents_0079 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0080 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 80);
ALTER TABLE NewEvents_0080 SET (fillfactor = 70);
ALTER TABLE NewEvents_0080 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0081 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 81);
ALTER TABLE NewEvents_0081 SET (fillfactor = 70);
ALTER TABLE NewEvents_0081 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0082 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 82);
ALTER TABLE NewEvents_0082 SET (fillfactor = 70);
ALTER TABLE NewEvents_0082 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0083 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 83);
ALTER TABLE NewEvents_0083 SET (fillfactor = 70);
ALTER TABLE NewEvents_0083 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0084 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 84);
ALTER TABLE NewEvents_0084 SET (fillfactor = 70);
ALTER TABLE NewEvents_0084 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0085 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 85);
ALTER TABLE NewEvents_0085 SET (fillfactor = 70);
ALTER TABLE NewEvents_0085 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0086 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 86);
ALTER TABLE NewEvents_0086 SET (fillfactor = 70);
ALTER TABLE NewEvents_0086 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0087 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 87);
ALTER TABLE NewEvents_0087 SET (fillfactor = 70);
ALTER TABLE NewEvents_0087 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0088 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 88);
ALTER TABLE NewEvents_0088 SET (fillfactor = 70);
ALTER TABLE NewEvents_0088 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0089 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 89);
ALTER TABLE NewEvents_0089 SET (fillfactor = 70);
ALTER TABLE NewEvents_0089 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0090 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 90);
ALTER TABLE NewEvents_0090 SET (fillfactor = 70);
ALTER TABLE NewEvents_0090 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0091 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 91);
ALTER TABLE NewEvents_0091 SET (fillfactor = 70);
ALTER TABLE NewEvents_0091 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0092 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 92);
ALTER TABLE NewEvents_0092 SET (fillfactor = 70);
ALTER TABLE NewEvents_0092 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0093 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 93);
ALTER TABLE NewEvents_0093 SET (fillfactor = 70);
ALTER TABLE NewEvents_0093 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0094 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 94);
ALTER TABLE NewEvents_0094 SET (fillfactor = 70);
ALTER TABLE NewEvents_0094 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0095 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 95);
ALTER TABLE NewEvents_0095 SET (fillfactor = 70);
ALTER TABLE NewEvents_0095 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0096 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 96);
ALTER TABLE NewEvents_0096 SET (fillfactor = 70);
ALTER TABLE NewEvents_0096 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0097 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 97);
ALTER TABLE NewEvents_0097 SET (fillfactor = 70);
ALTER TABLE NewEvents_0097 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0098 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 98);
ALTER TABLE NewEvents_0098 SET (fillfactor = 70);
ALTER TABLE NewEvents_0098 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0099 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 99);
ALTER TABLE NewEvents_0099 SET (fillfactor = 70);
ALTER TABLE NewEvents_0099 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0100 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 100);
ALTER TABLE NewEvents_0100 SET (fillfactor = 70);
ALTER TABLE NewEvents_0100 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0101 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 101);
ALTER TABLE NewEvents_0101 SET (fillfactor = 70);
ALTER TABLE NewEvents_0101 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0102 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 102);
ALTER TABLE NewEvents_0102 SET (fillfactor = 70);
ALTER TABLE NewEvents_0102 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0103 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 103);
ALTER TABLE NewEvents_0103 SET (fillfactor = 70);
ALTER TABLE NewEvents_0103 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0104 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 104);
ALTER TABLE NewEvents_0104 SET (fillfactor = 70);
ALTER TABLE NewEvents_0104 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0105 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 105);
ALTER TABLE NewEvents_0105 SET (fillfactor = 70);
ALTER TABLE NewEvents_0105 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0106 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 106);
ALTER TABLE NewEvents_0106 SET (fillfactor = 70);
ALTER TABLE NewEvents_0106 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0107 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 107);
ALTER TABLE NewEvents_0107 SET (fillfactor = 70);
ALTER TABLE NewEvents_0107 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0108 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 108);
ALTER TABLE NewEvents_0108 SET (fillfactor = 70);
ALTER TABLE NewEvents_0108 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0109 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 109);
ALTER TABLE NewEvents_0109 SET (fillfactor = 70);
ALTER TABLE NewEvents_0109 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0110 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 110);
ALTER TABLE NewEvents_0110 SET (fillfactor = 70);
ALTER TABLE NewEvents_0110 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0111 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 111);
ALTER TABLE NewEvents_0111 SET (fillfactor = 70);
ALTER TABLE NewEvents_0111 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0112 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 112);
ALTER TABLE NewEvents_0112 SET (fillfactor = 70);
ALTER TABLE NewEvents_0112 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0113 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 113);
ALTER TABLE NewEvents_0113 SET (fillfactor = 70);
ALTER TABLE NewEvents_0113 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0114 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 114);
ALTER TABLE NewEvents_0114 SET (fillfactor = 70);
ALTER TABLE NewEvents_0114 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0115 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 115);
ALTER TABLE NewEvents_0115 SET (fillfactor = 70);
ALTER TABLE NewEvents_0115 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0116 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 116);
ALTER TABLE NewEvents_0116 SET (fillfactor = 70);
ALTER TABLE NewEvents_0116 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0117 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 117);
ALTER TABLE NewEvents_0117 SET (fillfactor = 70);
ALTER TABLE NewEvents_0117 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0118 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 118);
ALTER TABLE NewEvents_0118 SET (fillfactor = 70);
ALTER TABLE NewEvents_0118 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0119 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 119);
ALTER TABLE NewEvents_0119 SET (fillfactor = 70);
ALTER TABLE NewEvents_0119 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0120 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 120);
ALTER TABLE NewEvents_0120 SET (fillfactor = 70);
ALTER TABLE NewEvents_0120 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0121 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 121);
ALTER TABLE NewEvents_0121 SET (fillfactor = 70);
ALTER TABLE NewEvents_0121 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0122 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 122);
ALTER TABLE NewEvents_0122 SET (fillfactor = 70);
ALTER TABLE NewEvents_0122 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0123 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 123);
ALTER TABLE NewEvents_0123 SET (fillfactor = 70);
ALTER TABLE NewEvents_0123 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0124 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 124);
ALTER TABLE NewEvents_0124 SET (fillfactor = 70);
ALTER TABLE NewEvents_0124 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0125 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 125);
ALTER TABLE NewEvents_0125 SET (fillfactor = 70);
ALTER TABLE NewEvents_0125 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0126 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 126);
ALTER TABLE NewEvents_0126 SET (fillfactor = 70);
ALTER TABLE NewEvents_0126 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

CREATE TABLE IF NOT EXISTS NewEvents_0127 PARTITION OF NewEvents FOR VALUES WITH (MODULUS 128, REMAINDER 127);
ALTER TABLE NewEvents_0127 SET (fillfactor = 70);
ALTER TABLE NewEvents_0127 SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

-- Performance optimization indexes for each partition
CREATE INDEX IF NOT EXISTS IX_NewEvents_0000_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0000(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0001_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0001(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0002_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0002(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0003_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0003(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0004_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0004(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0005_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0005(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0006_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0006(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0007_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0007(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0008_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0008(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0009_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0009(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0010_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0010(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0011_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0011(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0012_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0012(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0013_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0013(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0014_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0014(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0015_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0015(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0016_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0016(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0017_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0017(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0018_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0018(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0019_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0019(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0020_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0020(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0021_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0021(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0022_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0022(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0023_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0023(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0024_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0024(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0025_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0025(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0026_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0026(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0027_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0027(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0028_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0028(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0029_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0029(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0030_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0030(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0031_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0031(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0032_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0032(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0033_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0033(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0034_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0034(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0035_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0035(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0036_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0036(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0037_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0037(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0038_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0038(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0039_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0039(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0040_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0040(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0041_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0041(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0042_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0042(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0043_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0043(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0044_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0044(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0045_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0045(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0046_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0046(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0047_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0047(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0048_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0048(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0049_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0049(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0050_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0050(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0051_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0051(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0052_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0052(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0053_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0053(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0054_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0054(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0055_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0055(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0056_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0056(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0057_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0057(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0058_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0058(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0059_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0059(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0060_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0060(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0061_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0061(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0062_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0062(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0063_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0063(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0064_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0064(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0065_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0065(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0066_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0066(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0067_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0067(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0068_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0068(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0069_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0069(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0070_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0070(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0071_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0071(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0072_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0072(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0073_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0073(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0074_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0074(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0075_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0075(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0076_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0076(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0077_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0077(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0078_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0078(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0079_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0079(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0080_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0080(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0081_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0081(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0082_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0082(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0083_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0083(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0084_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0084(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0085_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0085(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0086_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0086(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0087_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0087(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0088_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0088(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0089_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0089(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0090_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0090(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0091_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0091(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0092_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0092(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0093_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0093(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0094_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0094(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0095_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0095(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0096_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0096(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0097_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0097(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0098_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0098(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0099_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0099(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0100_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0100(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0101_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0101(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0102_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0102(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0103_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0103(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0104_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0104(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0105_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0105(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0106_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0106(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0107_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0107(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0108_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0108(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0109_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0109(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0110_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0110(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0111_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0111(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0112_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0112(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0113_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0113(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0114_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0114(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0115_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0115(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0116_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0116(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0117_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0117(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0118_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0118(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0119_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0119(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0120_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0120(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0121_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0121(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0122_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0122(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0123_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0123(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0124_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0124(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0125_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0125(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0126_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0126(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0127_ID_SeqNum_WHERE_LockedBy_NULL ON NewEvents_0127(InstanceID, SequenceNumber) WHERE LockedBy IS NULL;

CREATE INDEX IF NOT EXISTS IX_NewEvents_0000_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0000(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0001_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0001(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0002_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0002(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0003_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0003(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0004_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0004(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0005_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0005(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0006_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0006(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0007_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0007(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0008_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0008(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0009_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0009(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0010_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0010(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0011_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0011(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0012_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0012(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0013_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0013(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0014_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0014(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0015_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0015(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0016_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0016(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0017_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0017(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0018_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0018(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0019_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0019(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0020_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0020(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0021_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0021(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0022_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0022(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0023_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0023(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0024_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0024(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0025_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0025(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0026_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0026(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0027_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0027(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0028_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0028(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0029_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0029(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0030_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0030(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0031_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0031(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0032_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0032(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0033_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0033(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0034_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0034(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0035_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0035(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0036_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0036(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0037_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0037(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0038_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0038(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0039_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0039(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0040_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0040(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0041_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0041(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0042_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0042(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0043_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0043(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0044_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0044(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0045_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0045(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0046_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0046(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0047_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0047(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0048_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0048(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0049_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0049(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0050_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0050(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0051_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0051(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0052_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0052(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0053_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0053(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0054_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0054(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0055_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0055(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0056_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0056(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0057_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0057(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0058_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0058(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0059_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0059(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0060_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0060(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0061_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0061(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0062_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0062(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0063_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0063(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0064_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0064(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0065_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0065(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0066_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0066(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0067_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0067(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0068_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0068(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0069_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0069(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0070_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0070(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0071_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0071(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0072_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0072(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0073_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0073(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0074_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0074(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0075_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0075(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0076_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0076(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0077_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0077(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0078_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0078(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0079_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0079(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0080_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0080(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0081_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0081(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0082_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0082(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0083_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0083(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0084_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0084(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0085_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0085(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0086_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0086(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0087_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0087(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0088_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0088(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0089_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0089(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0090_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0090(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0091_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0091(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0092_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0092(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0093_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0093(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0094_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0094(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0095_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0095(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0096_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0096(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0097_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0097(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0098_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0098(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0099_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0099(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0100_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0100(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0101_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0101(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0102_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0102(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0103_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0103(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0104_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0104(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0105_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0105(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0106_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0106(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0107_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0107(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0108_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0108(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0109_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0109(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0110_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0110(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0111_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0111(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0112_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0112(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0113_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0113(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0114_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0114(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0115_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0115(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0116_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0116(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0117_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0117(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0118_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0118(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0119_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0119(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0120_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0120(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0121_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0121(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0122_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0122(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0123_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0123(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0124_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0124(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0125_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0125(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0126_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0126(VisibleTime) WHERE VisibleTime IS NOT NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0127_VisibleTime_WHERE_VisibleTime_IS_NOT_NULL ON NewEvents_0127(VisibleTime) WHERE VisibleTime IS NOT NULL;

-- Composite index for EXISTS subquery: (InstanceID, VisibleTime)
CREATE INDEX IF NOT EXISTS IX_NewEvents_0000_ID_VisTime ON NewEvents_0000(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0001_ID_VisTime ON NewEvents_0001(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0002_ID_VisTime ON NewEvents_0002(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0003_ID_VisTime ON NewEvents_0003(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0004_ID_VisTime ON NewEvents_0004(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0005_ID_VisTime ON NewEvents_0005(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0006_ID_VisTime ON NewEvents_0006(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0007_ID_VisTime ON NewEvents_0007(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0008_ID_VisTime ON NewEvents_0008(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0009_ID_VisTime ON NewEvents_0009(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0010_ID_VisTime ON NewEvents_0010(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0011_ID_VisTime ON NewEvents_0011(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0012_ID_VisTime ON NewEvents_0012(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0013_ID_VisTime ON NewEvents_0013(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0014_ID_VisTime ON NewEvents_0014(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0015_ID_VisTime ON NewEvents_0015(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0016_ID_VisTime ON NewEvents_0016(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0017_ID_VisTime ON NewEvents_0017(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0018_ID_VisTime ON NewEvents_0018(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0019_ID_VisTime ON NewEvents_0019(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0020_ID_VisTime ON NewEvents_0020(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0021_ID_VisTime ON NewEvents_0021(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0022_ID_VisTime ON NewEvents_0022(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0023_ID_VisTime ON NewEvents_0023(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0024_ID_VisTime ON NewEvents_0024(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0025_ID_VisTime ON NewEvents_0025(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0026_ID_VisTime ON NewEvents_0026(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0027_ID_VisTime ON NewEvents_0027(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0028_ID_VisTime ON NewEvents_0028(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0029_ID_VisTime ON NewEvents_0029(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0030_ID_VisTime ON NewEvents_0030(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0031_ID_VisTime ON NewEvents_0031(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0032_ID_VisTime ON NewEvents_0032(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0033_ID_VisTime ON NewEvents_0033(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0034_ID_VisTime ON NewEvents_0034(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0035_ID_VisTime ON NewEvents_0035(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0036_ID_VisTime ON NewEvents_0036(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0037_ID_VisTime ON NewEvents_0037(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0038_ID_VisTime ON NewEvents_0038(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0039_ID_VisTime ON NewEvents_0039(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0040_ID_VisTime ON NewEvents_0040(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0041_ID_VisTime ON NewEvents_0041(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0042_ID_VisTime ON NewEvents_0042(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0043_ID_VisTime ON NewEvents_0043(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0044_ID_VisTime ON NewEvents_0044(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0045_ID_VisTime ON NewEvents_0045(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0046_ID_VisTime ON NewEvents_0046(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0047_ID_VisTime ON NewEvents_0047(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0048_ID_VisTime ON NewEvents_0048(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0049_ID_VisTime ON NewEvents_0049(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0050_ID_VisTime ON NewEvents_0050(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0051_ID_VisTime ON NewEvents_0051(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0052_ID_VisTime ON NewEvents_0052(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0053_ID_VisTime ON NewEvents_0053(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0054_ID_VisTime ON NewEvents_0054(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0055_ID_VisTime ON NewEvents_0055(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0056_ID_VisTime ON NewEvents_0056(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0057_ID_VisTime ON NewEvents_0057(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0058_ID_VisTime ON NewEvents_0058(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0059_ID_VisTime ON NewEvents_0059(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0060_ID_VisTime ON NewEvents_0060(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0061_ID_VisTime ON NewEvents_0061(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0062_ID_VisTime ON NewEvents_0062(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0063_ID_VisTime ON NewEvents_0063(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0064_ID_VisTime ON NewEvents_0064(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0065_ID_VisTime ON NewEvents_0065(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0066_ID_VisTime ON NewEvents_0066(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0067_ID_VisTime ON NewEvents_0067(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0068_ID_VisTime ON NewEvents_0068(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0069_ID_VisTime ON NewEvents_0069(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0070_ID_VisTime ON NewEvents_0070(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0071_ID_VisTime ON NewEvents_0071(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0072_ID_VisTime ON NewEvents_0072(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0073_ID_VisTime ON NewEvents_0073(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0074_ID_VisTime ON NewEvents_0074(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0075_ID_VisTime ON NewEvents_0075(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0076_ID_VisTime ON NewEvents_0076(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0077_ID_VisTime ON NewEvents_0077(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0078_ID_VisTime ON NewEvents_0078(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0079_ID_VisTime ON NewEvents_0079(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0080_ID_VisTime ON NewEvents_0080(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0081_ID_VisTime ON NewEvents_0081(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0082_ID_VisTime ON NewEvents_0082(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0083_ID_VisTime ON NewEvents_0083(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0084_ID_VisTime ON NewEvents_0084(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0085_ID_VisTime ON NewEvents_0085(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0086_ID_VisTime ON NewEvents_0086(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0087_ID_VisTime ON NewEvents_0087(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0088_ID_VisTime ON NewEvents_0088(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0089_ID_VisTime ON NewEvents_0089(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0090_ID_VisTime ON NewEvents_0090(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0091_ID_VisTime ON NewEvents_0091(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0092_ID_VisTime ON NewEvents_0092(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0093_ID_VisTime ON NewEvents_0093(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0094_ID_VisTime ON NewEvents_0094(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0095_ID_VisTime ON NewEvents_0095(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0096_ID_VisTime ON NewEvents_0096(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0097_ID_VisTime ON NewEvents_0097(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0098_ID_VisTime ON NewEvents_0098(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0099_ID_VisTime ON NewEvents_0099(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0100_ID_VisTime ON NewEvents_0100(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0101_ID_VisTime ON NewEvents_0101(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0102_ID_VisTime ON NewEvents_0102(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0103_ID_VisTime ON NewEvents_0103(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0104_ID_VisTime ON NewEvents_0104(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0105_ID_VisTime ON NewEvents_0105(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0106_ID_VisTime ON NewEvents_0106(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0107_ID_VisTime ON NewEvents_0107(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0108_ID_VisTime ON NewEvents_0108(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0109_ID_VisTime ON NewEvents_0109(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0110_ID_VisTime ON NewEvents_0110(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0111_ID_VisTime ON NewEvents_0111(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0112_ID_VisTime ON NewEvents_0112(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0113_ID_VisTime ON NewEvents_0113(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0114_ID_VisTime ON NewEvents_0114(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0115_ID_VisTime ON NewEvents_0115(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0116_ID_VisTime ON NewEvents_0116(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0117_ID_VisTime ON NewEvents_0117(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0118_ID_VisTime ON NewEvents_0118(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0119_ID_VisTime ON NewEvents_0119(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0120_ID_VisTime ON NewEvents_0120(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0121_ID_VisTime ON NewEvents_0121(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0122_ID_VisTime ON NewEvents_0122(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0123_ID_VisTime ON NewEvents_0123(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0124_ID_VisTime ON NewEvents_0124(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0125_ID_VisTime ON NewEvents_0125(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0126_ID_VisTime ON NewEvents_0126(InstanceID, VisibleTime);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0127_ID_VisTime ON NewEvents_0127(InstanceID, VisibleTime);

CREATE INDEX IF NOT EXISTS IX_NewEvents_0000_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0000(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0001_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0001(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0002_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0002(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0003_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0003(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0004_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0004(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0005_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0005(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0006_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0006(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0007_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0007(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0008_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0008(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0009_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0009(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0010_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0010(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0011_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0011(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0012_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0012(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0013_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0013(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0014_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0014(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0015_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0015(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0016_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0016(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0017_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0017(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0018_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0018(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0019_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0019(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0020_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0020(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0021_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0021(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0022_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0022(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0023_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0023(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0024_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0024(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0025_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0025(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0026_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0026(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0027_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0027(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0028_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0028(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0029_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0029(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0030_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0030(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0031_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0031(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0032_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0032(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0033_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0033(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0034_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0034(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0035_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0035(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0036_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0036(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0037_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0037(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0038_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0038(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0039_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0039(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0040_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0040(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0041_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0041(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0042_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0042(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0043_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0043(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0044_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0044(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0045_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0045(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0046_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0046(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0047_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0047(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0048_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0048(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0049_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0049(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0050_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0050(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0051_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0051(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0052_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0052(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0053_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0053(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0054_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0054(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0055_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0055(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0056_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0056(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0057_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0057(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0058_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0058(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0059_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0059(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0060_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0060(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0061_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0061(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0062_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0062(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0063_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0063(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0064_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0064(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0065_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0065(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0066_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0066(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0067_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0067(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0068_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0068(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0069_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0069(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0070_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0070(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0071_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0071(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0072_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0072(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0073_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0073(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0074_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0074(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0075_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0075(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0076_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0076(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0077_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0077(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0078_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0078(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0079_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0079(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0080_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0080(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0081_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0081(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0082_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0082(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0083_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0083(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0084_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0084(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0085_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0085(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0086_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0086(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0087_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0087(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0088_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0088(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0089_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0089(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0090_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0090(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0091_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0091(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0092_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0092(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0093_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0093(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0094_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0094(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0095_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0095(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0096_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0096(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0097_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0097(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0098_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0098(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0099_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0099(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0100_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0100(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0101_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0101(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0102_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0102(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0103_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0103(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0104_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0104(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0105_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0105(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0106_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0106(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0107_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0107(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0108_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0108(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0109_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0109(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0110_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0110(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0111_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0111(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0112_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0112(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0113_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0113(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0114_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0114(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0115_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0115(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0116_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0116(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0117_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0117(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0118_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0118(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0119_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0119(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0120_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0120(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0121_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0121(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0122_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0122(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0123_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0123(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0124_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0124(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0125_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0125(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0126_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0126(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0127_ID_VisTime_SeqNum_WHERE_VisTime_NULL ON NewEvents_0127(InstanceID, VisibleTime, SequenceNumber) WHERE VisibleTime IS NULL;

CREATE INDEX IF NOT EXISTS IX_NewEvents_0000_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0000(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0001_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0001(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0002_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0002(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0003_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0003(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0004_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0004(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0005_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0005(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0006_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0006(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0007_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0007(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0008_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0008(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0009_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0009(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0010_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0010(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0011_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0011(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0012_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0012(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0013_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0013(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0014_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0014(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0015_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0015(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0016_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0016(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0017_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0017(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0018_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0018(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0019_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0019(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0020_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0020(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0021_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0021(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0022_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0022(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0023_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0023(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0024_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0024(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0025_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0025(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0026_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0026(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0027_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0027(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0028_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0028(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0029_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0029(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0030_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0030(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0031_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0031(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0032_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0032(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0033_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0033(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0034_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0034(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0035_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0035(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0036_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0036(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0037_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0037(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0038_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0038(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0039_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0039(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0040_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0040(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0041_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0041(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0042_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0042(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0043_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0043(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0044_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0044(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0045_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0045(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0046_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0046(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0047_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0047(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0048_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0048(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0049_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0049(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0050_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0050(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0051_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0051(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0052_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0052(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0053_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0053(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0054_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0054(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0055_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0055(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0056_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0056(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0057_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0057(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0058_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0058(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0059_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0059(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0060_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0060(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0061_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0061(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0062_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0062(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0063_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0063(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0064_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0064(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0065_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0065(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0066_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0066(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0067_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0067(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0068_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0068(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0069_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0069(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0070_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0070(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0071_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0071(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0072_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0072(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0073_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0073(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0074_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0074(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0075_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0075(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0076_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0076(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0077_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0077(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0078_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0078(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0079_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0079(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0080_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0080(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0081_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0081(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0082_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0082(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0083_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0083(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0084_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0084(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0085_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0085(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0086_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0086(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0087_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0087(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0088_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0088(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0089_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0089(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0090_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0090(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0091_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0091(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0092_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0092(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0093_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0093(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0094_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0094(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0095_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0095(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0096_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0096(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0097_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0097(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0098_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0098(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0099_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0099(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0100_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0100(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0101_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0101(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0102_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0102(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0103_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0103(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0104_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0104(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0105_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0105(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0106_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0106(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0107_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0107(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0108_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0108(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0109_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0109(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0110_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0110(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0111_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0111(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0112_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0112(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0113_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0113(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0114_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0114(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0115_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0115(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0116_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0116(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0117_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0117(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0118_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0118(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0119_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0119(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0120_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0120(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0121_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0121(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0122_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0122(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0123_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0123(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0124_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0124(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0125_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0125(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0126_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0126(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;
CREATE INDEX IF NOT EXISTS IX_NewEvents_0127_ID_VisTime_Dequeue_WHERE_LockedBy_NULL ON NewEvents_0127(InstanceID, VisibleTime, DequeueCount) WHERE LockedBy IS NULL;

-- Index for abandon operations (WHERE InstanceID = $1 AND LockedBy = $2)
CREATE INDEX IF NOT EXISTS IX_NewEvents_0000_InstanceID_LockedBy ON NewEvents_0000(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0001_InstanceID_LockedBy ON NewEvents_0001(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0002_InstanceID_LockedBy ON NewEvents_0002(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0003_InstanceID_LockedBy ON NewEvents_0003(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0004_InstanceID_LockedBy ON NewEvents_0004(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0005_InstanceID_LockedBy ON NewEvents_0005(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0006_InstanceID_LockedBy ON NewEvents_0006(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0007_InstanceID_LockedBy ON NewEvents_0007(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0008_InstanceID_LockedBy ON NewEvents_0008(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0009_InstanceID_LockedBy ON NewEvents_0009(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0010_InstanceID_LockedBy ON NewEvents_0010(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0011_InstanceID_LockedBy ON NewEvents_0011(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0012_InstanceID_LockedBy ON NewEvents_0012(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0013_InstanceID_LockedBy ON NewEvents_0013(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0014_InstanceID_LockedBy ON NewEvents_0014(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0015_InstanceID_LockedBy ON NewEvents_0015(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0016_InstanceID_LockedBy ON NewEvents_0016(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0017_InstanceID_LockedBy ON NewEvents_0017(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0018_InstanceID_LockedBy ON NewEvents_0018(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0019_InstanceID_LockedBy ON NewEvents_0019(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0020_InstanceID_LockedBy ON NewEvents_0020(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0021_InstanceID_LockedBy ON NewEvents_0021(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0022_InstanceID_LockedBy ON NewEvents_0022(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0023_InstanceID_LockedBy ON NewEvents_0023(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0024_InstanceID_LockedBy ON NewEvents_0024(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0025_InstanceID_LockedBy ON NewEvents_0025(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0026_InstanceID_LockedBy ON NewEvents_0026(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0027_InstanceID_LockedBy ON NewEvents_0027(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0028_InstanceID_LockedBy ON NewEvents_0028(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0029_InstanceID_LockedBy ON NewEvents_0029(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0030_InstanceID_LockedBy ON NewEvents_0030(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0031_InstanceID_LockedBy ON NewEvents_0031(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0032_InstanceID_LockedBy ON NewEvents_0032(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0033_InstanceID_LockedBy ON NewEvents_0033(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0034_InstanceID_LockedBy ON NewEvents_0034(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0035_InstanceID_LockedBy ON NewEvents_0035(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0036_InstanceID_LockedBy ON NewEvents_0036(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0037_InstanceID_LockedBy ON NewEvents_0037(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0038_InstanceID_LockedBy ON NewEvents_0038(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0039_InstanceID_LockedBy ON NewEvents_0039(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0040_InstanceID_LockedBy ON NewEvents_0040(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0041_InstanceID_LockedBy ON NewEvents_0041(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0042_InstanceID_LockedBy ON NewEvents_0042(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0043_InstanceID_LockedBy ON NewEvents_0043(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0044_InstanceID_LockedBy ON NewEvents_0044(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0045_InstanceID_LockedBy ON NewEvents_0045(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0046_InstanceID_LockedBy ON NewEvents_0046(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0047_InstanceID_LockedBy ON NewEvents_0047(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0048_InstanceID_LockedBy ON NewEvents_0048(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0049_InstanceID_LockedBy ON NewEvents_0049(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0050_InstanceID_LockedBy ON NewEvents_0050(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0051_InstanceID_LockedBy ON NewEvents_0051(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0052_InstanceID_LockedBy ON NewEvents_0052(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0053_InstanceID_LockedBy ON NewEvents_0053(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0054_InstanceID_LockedBy ON NewEvents_0054(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0055_InstanceID_LockedBy ON NewEvents_0055(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0056_InstanceID_LockedBy ON NewEvents_0056(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0057_InstanceID_LockedBy ON NewEvents_0057(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0058_InstanceID_LockedBy ON NewEvents_0058(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0059_InstanceID_LockedBy ON NewEvents_0059(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0060_InstanceID_LockedBy ON NewEvents_0060(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0061_InstanceID_LockedBy ON NewEvents_0061(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0062_InstanceID_LockedBy ON NewEvents_0062(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0063_InstanceID_LockedBy ON NewEvents_0063(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0064_InstanceID_LockedBy ON NewEvents_0064(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0065_InstanceID_LockedBy ON NewEvents_0065(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0066_InstanceID_LockedBy ON NewEvents_0066(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0067_InstanceID_LockedBy ON NewEvents_0067(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0068_InstanceID_LockedBy ON NewEvents_0068(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0069_InstanceID_LockedBy ON NewEvents_0069(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0070_InstanceID_LockedBy ON NewEvents_0070(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0071_InstanceID_LockedBy ON NewEvents_0071(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0072_InstanceID_LockedBy ON NewEvents_0072(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0073_InstanceID_LockedBy ON NewEvents_0073(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0074_InstanceID_LockedBy ON NewEvents_0074(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0075_InstanceID_LockedBy ON NewEvents_0075(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0076_InstanceID_LockedBy ON NewEvents_0076(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0077_InstanceID_LockedBy ON NewEvents_0077(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0078_InstanceID_LockedBy ON NewEvents_0078(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0079_InstanceID_LockedBy ON NewEvents_0079(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0080_InstanceID_LockedBy ON NewEvents_0080(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0081_InstanceID_LockedBy ON NewEvents_0081(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0082_InstanceID_LockedBy ON NewEvents_0082(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0083_InstanceID_LockedBy ON NewEvents_0083(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0084_InstanceID_LockedBy ON NewEvents_0084(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0085_InstanceID_LockedBy ON NewEvents_0085(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0086_InstanceID_LockedBy ON NewEvents_0086(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0087_InstanceID_LockedBy ON NewEvents_0087(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0088_InstanceID_LockedBy ON NewEvents_0088(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0089_InstanceID_LockedBy ON NewEvents_0089(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0090_InstanceID_LockedBy ON NewEvents_0090(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0091_InstanceID_LockedBy ON NewEvents_0091(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0092_InstanceID_LockedBy ON NewEvents_0092(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0093_InstanceID_LockedBy ON NewEvents_0093(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0094_InstanceID_LockedBy ON NewEvents_0094(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0095_InstanceID_LockedBy ON NewEvents_0095(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0096_InstanceID_LockedBy ON NewEvents_0096(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0097_InstanceID_LockedBy ON NewEvents_0097(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0098_InstanceID_LockedBy ON NewEvents_0098(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0099_InstanceID_LockedBy ON NewEvents_0099(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0100_InstanceID_LockedBy ON NewEvents_0100(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0101_InstanceID_LockedBy ON NewEvents_0101(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0102_InstanceID_LockedBy ON NewEvents_0102(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0103_InstanceID_LockedBy ON NewEvents_0103(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0104_InstanceID_LockedBy ON NewEvents_0104(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0105_InstanceID_LockedBy ON NewEvents_0105(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0106_InstanceID_LockedBy ON NewEvents_0106(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0107_InstanceID_LockedBy ON NewEvents_0107(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0108_InstanceID_LockedBy ON NewEvents_0108(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0109_InstanceID_LockedBy ON NewEvents_0109(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0110_InstanceID_LockedBy ON NewEvents_0110(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0111_InstanceID_LockedBy ON NewEvents_0111(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0112_InstanceID_LockedBy ON NewEvents_0112(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0113_InstanceID_LockedBy ON NewEvents_0113(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0114_InstanceID_LockedBy ON NewEvents_0114(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0115_InstanceID_LockedBy ON NewEvents_0115(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0116_InstanceID_LockedBy ON NewEvents_0116(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0117_InstanceID_LockedBy ON NewEvents_0117(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0118_InstanceID_LockedBy ON NewEvents_0118(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0119_InstanceID_LockedBy ON NewEvents_0119(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0120_InstanceID_LockedBy ON NewEvents_0120(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0121_InstanceID_LockedBy ON NewEvents_0121(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0122_InstanceID_LockedBy ON NewEvents_0122(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0123_InstanceID_LockedBy ON NewEvents_0123(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0124_InstanceID_LockedBy ON NewEvents_0124(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0125_InstanceID_LockedBy ON NewEvents_0125(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0126_InstanceID_LockedBy ON NewEvents_0126(InstanceID, LockedBy);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0127_InstanceID_LockedBy ON NewEvents_0127(InstanceID, LockedBy);

-- Full index for ORDER BY InstanceID, SequenceNumber (supports all rows)
CREATE INDEX IF NOT EXISTS IX_NewEvents_0000_InstanceID_SequenceNumber ON NewEvents_0000(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0001_InstanceID_SequenceNumber ON NewEvents_0001(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0002_InstanceID_SequenceNumber ON NewEvents_0002(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0003_InstanceID_SequenceNumber ON NewEvents_0003(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0004_InstanceID_SequenceNumber ON NewEvents_0004(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0005_InstanceID_SequenceNumber ON NewEvents_0005(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0006_InstanceID_SequenceNumber ON NewEvents_0006(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0007_InstanceID_SequenceNumber ON NewEvents_0007(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0008_InstanceID_SequenceNumber ON NewEvents_0008(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0009_InstanceID_SequenceNumber ON NewEvents_0009(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0010_InstanceID_SequenceNumber ON NewEvents_0010(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0011_InstanceID_SequenceNumber ON NewEvents_0011(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0012_InstanceID_SequenceNumber ON NewEvents_0012(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0013_InstanceID_SequenceNumber ON NewEvents_0013(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0014_InstanceID_SequenceNumber ON NewEvents_0014(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0015_InstanceID_SequenceNumber ON NewEvents_0015(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0016_InstanceID_SequenceNumber ON NewEvents_0016(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0017_InstanceID_SequenceNumber ON NewEvents_0017(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0018_InstanceID_SequenceNumber ON NewEvents_0018(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0019_InstanceID_SequenceNumber ON NewEvents_0019(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0020_InstanceID_SequenceNumber ON NewEvents_0020(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0021_InstanceID_SequenceNumber ON NewEvents_0021(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0022_InstanceID_SequenceNumber ON NewEvents_0022(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0023_InstanceID_SequenceNumber ON NewEvents_0023(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0024_InstanceID_SequenceNumber ON NewEvents_0024(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0025_InstanceID_SequenceNumber ON NewEvents_0025(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0026_InstanceID_SequenceNumber ON NewEvents_0026(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0027_InstanceID_SequenceNumber ON NewEvents_0027(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0028_InstanceID_SequenceNumber ON NewEvents_0028(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0029_InstanceID_SequenceNumber ON NewEvents_0029(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0030_InstanceID_SequenceNumber ON NewEvents_0030(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0031_InstanceID_SequenceNumber ON NewEvents_0031(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0032_InstanceID_SequenceNumber ON NewEvents_0032(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0033_InstanceID_SequenceNumber ON NewEvents_0033(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0034_InstanceID_SequenceNumber ON NewEvents_0034(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0035_InstanceID_SequenceNumber ON NewEvents_0035(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0036_InstanceID_SequenceNumber ON NewEvents_0036(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0037_InstanceID_SequenceNumber ON NewEvents_0037(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0038_InstanceID_SequenceNumber ON NewEvents_0038(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0039_InstanceID_SequenceNumber ON NewEvents_0039(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0040_InstanceID_SequenceNumber ON NewEvents_0040(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0041_InstanceID_SequenceNumber ON NewEvents_0041(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0042_InstanceID_SequenceNumber ON NewEvents_0042(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0043_InstanceID_SequenceNumber ON NewEvents_0043(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0044_InstanceID_SequenceNumber ON NewEvents_0044(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0045_InstanceID_SequenceNumber ON NewEvents_0045(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0046_InstanceID_SequenceNumber ON NewEvents_0046(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0047_InstanceID_SequenceNumber ON NewEvents_0047(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0048_InstanceID_SequenceNumber ON NewEvents_0048(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0049_InstanceID_SequenceNumber ON NewEvents_0049(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0050_InstanceID_SequenceNumber ON NewEvents_0050(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0051_InstanceID_SequenceNumber ON NewEvents_0051(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0052_InstanceID_SequenceNumber ON NewEvents_0052(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0053_InstanceID_SequenceNumber ON NewEvents_0053(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0054_InstanceID_SequenceNumber ON NewEvents_0054(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0055_InstanceID_SequenceNumber ON NewEvents_0055(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0056_InstanceID_SequenceNumber ON NewEvents_0056(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0057_InstanceID_SequenceNumber ON NewEvents_0057(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0058_InstanceID_SequenceNumber ON NewEvents_0058(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0059_InstanceID_SequenceNumber ON NewEvents_0059(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0060_InstanceID_SequenceNumber ON NewEvents_0060(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0061_InstanceID_SequenceNumber ON NewEvents_0061(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0062_InstanceID_SequenceNumber ON NewEvents_0062(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0063_InstanceID_SequenceNumber ON NewEvents_0063(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0064_InstanceID_SequenceNumber ON NewEvents_0064(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0065_InstanceID_SequenceNumber ON NewEvents_0065(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0066_InstanceID_SequenceNumber ON NewEvents_0066(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0067_InstanceID_SequenceNumber ON NewEvents_0067(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0068_InstanceID_SequenceNumber ON NewEvents_0068(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0069_InstanceID_SequenceNumber ON NewEvents_0069(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0070_InstanceID_SequenceNumber ON NewEvents_0070(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0071_InstanceID_SequenceNumber ON NewEvents_0071(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0072_InstanceID_SequenceNumber ON NewEvents_0072(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0073_InstanceID_SequenceNumber ON NewEvents_0073(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0074_InstanceID_SequenceNumber ON NewEvents_0074(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0075_InstanceID_SequenceNumber ON NewEvents_0075(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0076_InstanceID_SequenceNumber ON NewEvents_0076(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0077_InstanceID_SequenceNumber ON NewEvents_0077(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0078_InstanceID_SequenceNumber ON NewEvents_0078(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0079_InstanceID_SequenceNumber ON NewEvents_0079(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0080_InstanceID_SequenceNumber ON NewEvents_0080(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0081_InstanceID_SequenceNumber ON NewEvents_0081(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0082_InstanceID_SequenceNumber ON NewEvents_0082(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0083_InstanceID_SequenceNumber ON NewEvents_0083(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0084_InstanceID_SequenceNumber ON NewEvents_0084(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0085_InstanceID_SequenceNumber ON NewEvents_0085(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0086_InstanceID_SequenceNumber ON NewEvents_0086(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0087_InstanceID_SequenceNumber ON NewEvents_0087(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0088_InstanceID_SequenceNumber ON NewEvents_0088(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0089_InstanceID_SequenceNumber ON NewEvents_0089(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0090_InstanceID_SequenceNumber ON NewEvents_0090(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0091_InstanceID_SequenceNumber ON NewEvents_0091(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0092_InstanceID_SequenceNumber ON NewEvents_0092(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0093_InstanceID_SequenceNumber ON NewEvents_0093(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0094_InstanceID_SequenceNumber ON NewEvents_0094(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0095_InstanceID_SequenceNumber ON NewEvents_0095(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0096_InstanceID_SequenceNumber ON NewEvents_0096(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0097_InstanceID_SequenceNumber ON NewEvents_0097(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0098_InstanceID_SequenceNumber ON NewEvents_0098(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0099_InstanceID_SequenceNumber ON NewEvents_0099(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0100_InstanceID_SequenceNumber ON NewEvents_0100(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0101_InstanceID_SequenceNumber ON NewEvents_0101(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0102_InstanceID_SequenceNumber ON NewEvents_0102(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0103_InstanceID_SequenceNumber ON NewEvents_0103(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0104_InstanceID_SequenceNumber ON NewEvents_0104(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0105_InstanceID_SequenceNumber ON NewEvents_0105(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0106_InstanceID_SequenceNumber ON NewEvents_0106(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0107_InstanceID_SequenceNumber ON NewEvents_0107(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0108_InstanceID_SequenceNumber ON NewEvents_0108(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0109_InstanceID_SequenceNumber ON NewEvents_0109(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0110_InstanceID_SequenceNumber ON NewEvents_0110(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0111_InstanceID_SequenceNumber ON NewEvents_0111(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0112_InstanceID_SequenceNumber ON NewEvents_0112(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0113_InstanceID_SequenceNumber ON NewEvents_0113(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0114_InstanceID_SequenceNumber ON NewEvents_0114(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0115_InstanceID_SequenceNumber ON NewEvents_0115(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0116_InstanceID_SequenceNumber ON NewEvents_0116(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0117_InstanceID_SequenceNumber ON NewEvents_0117(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0118_InstanceID_SequenceNumber ON NewEvents_0118(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0119_InstanceID_SequenceNumber ON NewEvents_0119(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0120_InstanceID_SequenceNumber ON NewEvents_0120(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0121_InstanceID_SequenceNumber ON NewEvents_0121(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0122_InstanceID_SequenceNumber ON NewEvents_0122(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0123_InstanceID_SequenceNumber ON NewEvents_0123(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0124_InstanceID_SequenceNumber ON NewEvents_0124(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0125_InstanceID_SequenceNumber ON NewEvents_0125(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0126_InstanceID_SequenceNumber ON NewEvents_0126(InstanceID, SequenceNumber);
CREATE INDEX IF NOT EXISTS IX_NewEvents_0127_InstanceID_SequenceNumber ON NewEvents_0127(InstanceID, SequenceNumber);

-- ============================================================================
-- NewTasks Table (single table, no partitioning)
-- ============================================================================
-- NOTE: Unlike NewEvents/History, NewTasks is intentionally NOT hash-partitioned by
-- InstanceID. Its hottest query patterns (poll, complete, abandon) do not filter by
-- InstanceID at all, so hash partitioning on InstanceID cannot prune and instead forces
-- every poll/complete/abandon call to fan out across all partitions. Only INSERT and the
-- per-instance purge DELETE benefit from InstanceID-based partitioning, so a single table
-- with targeted indexes performs better overall.
CREATE TABLE IF NOT EXISTS NewTasks (
    SequenceNumber BIGSERIAL PRIMARY KEY,
    InstanceID TEXT NOT NULL,
    ExecutionID TEXT NULL,
    Timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    DequeueCount INTEGER NOT NULL DEFAULT 0,
    LockedBy TEXT NULL,
    LockExpiration TIMESTAMP NULL,
    EventPayload BYTEA NOT NULL
);

ALTER TABLE NewTasks SET (fillfactor = 70);
ALTER TABLE NewTasks SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_threshold = 5000, autovacuum_analyze_scale_factor = 0.05, autovacuum_analyze_threshold = 2000);

-- Poll query coverage: WHERE (LockExpiration IS NULL OR LockExpiration < $3) ORDER BY InstanceID, SequenceNumber
CREATE INDEX IF NOT EXISTS IX_NewTasks_LockExp_Null_ID_SeqNum ON NewTasks(LockExpiration, InstanceID, SequenceNumber)
WHERE LockExpiration IS NULL;

CREATE INDEX IF NOT EXISTS IX_NewTasks_LockExp_NotNull_ID_SeqNum ON NewTasks(LockExpiration, InstanceID, SequenceNumber)
WHERE LockExpiration IS NOT NULL;

-- Cleanup path: WHERE InstanceID = $1
CREATE INDEX IF NOT EXISTS IX_NewTasks_InstanceID ON NewTasks(InstanceID);


-- ============================================================================
-- Summary of Performance Improvements
-- ============================================================================
-- 1. BIGSERIAL instead of SERIAL - Prevents sequence exhaustion at high scale
-- 2. Hash partitioning for NewEvents (128 partitions) - Better parallelism and reduced contention
-- 3. Partial indexes - 80-90% size reduction for active rows only
-- 4. Fillfactor settings - Reduces page splits for HOT updates (applied to partitions)
-- 5. Autovacuum tuning - Aggressive cleanup for high-churn tables (applied to partitions)
-- 6. Composite indexes - Optimized for complex locking query patterns
-- ============================================================================
-- ============================================================================
