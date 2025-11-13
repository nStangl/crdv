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
    seq serial
);
CREATE INDEX IF NOT EXISTS Shared_idx ON Shared (id, key);

-- Stores information about the cluster:
CREATE TABLE IF NOT EXISTS ClusterInfo (
    site_id integer,
    is_local boolean, -- whether this is the local site (true) or a remote one (false)
    addr varchar -- site address
);

-- BEFORE INSERT trigger to prevent duplicate operations from entering Shared table
-- Breaks infinite replication loop
CREATE OR REPLACE FUNCTION Shared_before_insert_dedup_function() RETURNS trigger AS $$
BEGIN
    -- check if an operation already exists in Local
    IF _is_operation_already_in_local(new.id, new.key, new.lts) THEN
        -- return null to cancel the insert
        RETURN NULL;
    END IF;
    -- Operation is new, proceed with insert
    RETURN new;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER Shared_before_insert_dedup_trigger
BEFORE INSERT ON Shared
FOR EACH ROW
EXECUTE FUNCTION Shared_before_insert_dedup_function();

-- Trigger to process local Shared inserts under the sync mode
CREATE OR REPLACE FUNCTION Shared_insert_local_sync_function() RETURNS trigger AS $$
BEGIN
    -- merge op
    PERFORM merge(new.id, new.key, new.type, new.data, new.site, new.lts, new.pts, new.op);

    -- delete op from the Shared table
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
