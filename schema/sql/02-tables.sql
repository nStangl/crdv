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
    hops int DEFAULT 0,
    arrival_time bigint DEFAULT currentTimeMillis()
);
CREATE INDEX IF NOT EXISTS Shared_idx ON Shared (id, key);
-- Index for fast deduplication lookups in BEFORE INSERT trigger (do we really need this?)
-- CREATE INDEX IF NOT EXISTS Shared_dedup_idx ON Shared (id, key, lts);

-- Stores information about the cluster:
CREATE TABLE IF NOT EXISTS ClusterInfo (
    site_id integer,
    is_local boolean, -- whether this is the local site (true) or a remote one (false)
    addr varchar -- site address
);

-- BEFORE INSERT trigger to prevent duplicate operations from entering Shared table
-- CREATE OR REPLACE FUNCTION Shared_before_insert_dedup_function() RETURNS trigger AS $$
-- BEGIN
--     -- Check if operation already exists in Local or is currently in Shared
--     -- IMPORTANT: Use schema-qualified function name for logical replication compatibility
--     -- replication worker will complain otherwise
--     IF public._is_operation_in_local_or_shared(new.id, new.key, new.lts) THEN
--         -- Return NULL to cancel the insert
--         RETURN NULL;
--     END IF;

--     -- Operation is new and not currently being processed, proceed with insert
--     RETURN new;
-- END;
-- $$ LANGUAGE plpgsql;

-- CREATE TRIGGER Shared_before_insert_dedup_trigger
-- BEFORE INSERT ON Shared
-- FOR EACH ROW
-- EXECUTE FUNCTION Shared_before_insert_dedup_function();

-- enable as ALWAYS trigger so it fires during both local inserts AND replication
-- this is critical for preventing duplicates from multiple replication paths
-- See(!) https://www.postgresql.org/docs/current/logical-replication-architecture.html
-- ALTER TABLE Shared ENABLE ALWAYS TRIGGER Shared_before_insert_dedup_trigger;

-- BEFORE INSERT REPLICA trigger to increment hop counter during replication
-- This is the KEY to making hops TTL work with logical replication:
-- - REPLICA triggers fire during logical replication (session_replication_role = 'replica')
-- - BEFORE INSERT can modify NEW row before it's written
-- - The incremented hops value is stored and seen by publication WHERE clause
-- CREATE OR REPLACE FUNCTION Shared_before_insert_increment_hops_function() RETURNS trigger AS $$
-- BEGIN
--     -- Only increment for operations from remote sites
--     -- Local writes (site = siteId()) should stay at hops = 0
--     -- IMPORTANT: Use schema-qualified function name for logical replication compatibility
--     IF NEW.site != public.siteId() THEN
--         NEW.hops := NEW.hops + 1;
--     END IF;

--     RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;

-- CREATE TRIGGER Shared_before_insert_increment_hops_trigger
-- BEFORE INSERT ON Shared
-- FOR EACH ROW
-- EXECUTE FUNCTION Shared_before_insert_increment_hops_function();

-- enable as REPLICA trigger so it ONLY fires during replication, not local inserts
-- ALTER TABLE Shared ENABLE REPLICA TRIGGER Shared_before_insert_increment_hops_trigger;

-- Trigger to set arrival_time to current time on receiving node during replication
-- This ensures each node records when the operation actually arrived, not when it was created
CREATE OR REPLACE FUNCTION Shared_before_insert_set_arrival_time_function() RETURNS trigger AS $$
BEGIN
    -- Override the replicated arrival_time with current time on this node
    -- IMPORTANT: Use schema-qualified function name for logical replication compatibility
    NEW.arrival_time := public.currentTimeMillis();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER Shared_before_insert_set_arrival_time_trigger
BEFORE INSERT ON Shared
FOR EACH ROW
EXECUTE FUNCTION Shared_before_insert_set_arrival_time_function();

-- enable as REPLICA trigger so it ONLY fires during replication, not local inserts
ALTER TABLE Shared ENABLE REPLICA TRIGGER Shared_before_insert_set_arrival_time_trigger;

-- Trigger to process local Shared inserts under the sync mode
CREATE OR REPLACE FUNCTION Shared_insert_local_sync_function() RETURNS trigger AS $$
BEGIN
    -- merge op
    PERFORM merge(new.id, new.key, new.type, new.data, new.site, new.lts, new.pts, new.op, new.arrival_time);

    -- delete op from the Shared table
   -- don't delete immediately, let merge_daemon hanlde cleanup every 1 second
    DELETE
    FROM Shared
    WHERE id = new.id
        AND key = new.key
        AND site = new.site
        AND seq = new.seq;

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
