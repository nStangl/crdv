-- Store the data of all CRDTs in the database
CREATE TABLE IF NOT EXISTS Local (
    id varchar COLLATE "C",
    key varchar COLLATE "C",
    type "char",
    data varchar,
    site int,
    lts vclock,
    pts hlc,
    op "char",
    merged_at bigint
);
CREATE INDEX IF NOT EXISTS Local_idx ON Local (id, key );

-- Shared table to publish to and subscribe from remote servers
CREATE TABLE Shared (
    id varchar COLLATE "C",
    key varchar COLLATE "C",
    type "char",
    data varchar,
    site int,
    lts vclock,
    pts hlc,
    op "char",
    seq serial,
    hops int DEFAULT 0
);
CREATE INDEX IF NOT EXISTS Shared_idx ON Shared (id, key);
-- Index for fast deduplication lookups in BEFORE INSERT trigger (do we really need this?)
CREATE INDEX IF NOT EXISTS Shared_dedup_idx ON Shared (id, key, lts);

-- Stores information about the cluster:
CREATE TABLE IF NOT EXISTS ClusterInfo (
    site_id integer,
    is_local boolean, -- whether this is the local site (true) or a remote one (false)
    addr varchar -- site address
);

-- BEFORE INSERT trigger to prevent duplicate operations from entering Shared table
CREATE OR REPLACE FUNCTION Shared_before_insert_dedup_function() RETURNS trigger AS $$
BEGIN
    -- Check if operation already exists in Local or is currently in Shared
    IF _is_operation_in_local_or_shared(new.id, new.key, new.lts) THEN
        -- Return NULL to cancel the insert
        RETURN NULL;
    END IF;

    -- Operation is new and not currently being processed, proceed with insert
    RETURN new;
END;
$$ LANGUAGE plpgsql;

-- AFTER INSERT trigger to increment hop counter (can't do in BEFORE because logical replication overrides)
CREATE OR REPLACE FUNCTION Shared_after_insert_increment_hops_function() RETURNS trigger AS $$
BEGIN
    -- Only increment for replicated operations (site != siteId())
    -- Local writes have site = siteId(), should stay at hops = 0
    IF new.site != siteId() THEN
        UPDATE Shared
        SET hops = hops + 1
        WHERE seq = new.seq;
    END IF;

    RETURN new;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER Shared_before_insert_dedup_trigger
BEFORE INSERT ON Shared
FOR EACH ROW
EXECUTE FUNCTION Shared_before_insert_dedup_function();

-- Trigger to increment hop counter after insert (must be AFTER because logical replication overrides BEFORE changes)
CREATE TRIGGER Shared_after_insert_increment_hops_trigger
AFTER INSERT ON Shared
FOR EACH ROW
EXECUTE FUNCTION Shared_after_insert_increment_hops_function();

-- Trigger to process local Shared inserts under the sync mode
CREATE OR REPLACE FUNCTION Shared_insert_local_sync_function() RETURNS trigger AS $$
BEGIN
    -- merge op
    PERFORM merge(new.id, new.key, new.type, new.data, new.site, new.lts, new.pts, new.op);

    -- delete op from the Shared table
   -- don't delete immediately, let merge_daemon hanlde cleanup every 1 second
   -- DELETE
   -- FROM Shared
   -- WHERE id = new.id
    --     AND key = new.key
    --     AND site = new.site
    --     AND seq = new.seq;

    RETURN new;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER Shared_insert_local_sync_trigger
AFTER INSERT ON Shared
FOR each row
EXECUTE FUNCTION Shared_insert_local_sync_function();


-- To be used to update the wall clock when receiving a remote insert and the read mode is 'all'.
CREATE OR REPLACE FUNCTION Shared_wall_clock_function() RETURNS trigger AS $$
BEGIN
    SET search_path TO 'public';

    -- update the wall clock
    PERFORM setval('WallClockSeq', greatest((new.pts).physical_time, (SELECT last_value FROM WallClockSeq)) , true);

    RETURN new;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER Shared_wall_clock_trigger
AFTER INSERT ON Shared
FOR each row
EXECUTE FUNCTION Shared_wall_clock_function();

-- Disable by default
ALTER TABLE Shared DISABLE TRIGGER Shared_wall_clock_trigger;


-- Used to control concurrent accesses to the same element
CREATE UNLOGGED TABLE StructureControl (
    id varchar PRIMARY KEY, -- structure identifier
    count bigint -- number of accesses
);
