-- Returns the site's identifier
CREATE OR REPLACE FUNCTION siteId() RETURNS int AS $$
BEGIN
    RETURN site_id
    FROM ClusterInfo
    WHERE is_local;
END;
$$ LANGUAGE PLPGSQL;


-- Returns the current number of sites
CREATE OR REPLACE FUNCTION nSites() RETURNS int AS $$
BEGIN
    SELECT count(*)
    FROM ClusterInfo;
END;
$$ LANGUAGE PLPGSQL;


-- Returns the initial logical timestamp (all zeros)
CREATE OR REPLACE FUNCTION initialLogicalTime() RETURNS vclock AS $$
BEGIN
    SELECT array_agg(0)
    FROM (
        SELECT generate_series(1, (SELECT nSites()))
    ) t;
END;
$$ LANGUAGE PLPGSQL;


-- Initializes the site's information
CREATE OR REPLACE FUNCTION initSite(site_id_ integer) RETURNS boolean AS $$
    BEGIN
        PERFORM *
        FROM ClusterInfo
        WHERE is_local;

        IF NOT FOUND THEN
            INSERT INTO ClusterInfo
            VALUES (site_id_, true, format('dbname=%s', current_database()));

            EXECUTE format(
                'CREATE PUBLICATION Shared_Pub '
                'FOR TABLE shared '
                'WITH (publish = ''insert'');'
            );

            CREATE INDEX ON Local ((lts[1]));

            PERFORM schedule_merge_daemon(1, 1, 100);

            RETURN true;
        ELSE
            RAISE EXCEPTION 'Site already initialized.';
            RETURN false;
        END IF;
    END
$$ LANGUAGE PLPGSQL;


CREATE EXTENSION IF NOT EXISTS dblink;

-- Adds a remote site to the cluster.
CREATE OR REPLACE FUNCTION addRemoteSite(site_id_ integer, host_ varchar, port_ varchar, dbname_ varchar,
                                         user_ varchar, password_ varchar, replicate_ boolean) RETURNS boolean AS $$
    DECLARE next_lts varchar[];
            max_lts varchar[];
            match_lts varchar[];
            n_sites integer;
    BEGIN
        PERFORM *
        FROM ClusterInfo
        WHERE is_local;

        IF NOT FOUND THEN
            RAISE EXCEPTION 'Site not initialized.';
        END IF;

        PERFORM *
        FROM ClusterInfo
        WHERE site_id = site_id_;

        IF FOUND THEN
            RAISE EXCEPTION 'Site already exists.';
        END IF;

        IF replicate_ THEN
            -- logical replication in the remote site
            EXECUTE format(
                'SELECT * '
                'FROM dblink(''host=%s port=%s dbname=%s user=%s password=%s'', '
                            '''SELECT pg_create_logical_replication_slot(''''sub_%s_%s'''', ''''pgoutput'''');'') as T(x text)',
                    host_, port_, dbname_, user_, password_, (SELECT siteId()), site_id_
            );

            -- subscription for this site
            EXECUTE format(
                'CREATE SUBSCRIPTION sub_%s_%s '
                'CONNECTION ''host=%s port=%s dbname=%s user=%s password=%s'' '
                'PUBLICATION Shared_Pub '
                'WITH (create_slot = false)',
                (SELECT siteId()), site_id_, host_, port_, dbname_, user_, password_
            );
        END IF;

        INSERT INTO ClusterInfo
        VALUES (site_id_, false, format('host=%s port=%s dbname=%s user=%s password=%s', host_, port_, dbname_, user_, password_));

        -- update views

        SELECT array_agg(format('coalesce(max(lts[%s]), 0)' || (CASE WHEN site_id = siteId() THEN '+ 1' ELSE '' END), site_id) ORDER BY site_id) INTO next_lts
        FROM ClusterInfo;

        SELECT array_agg(format('max(coalesce(lts[%s], 0))', site_id) ORDER BY site_id) INTO max_lts
        FROM ClusterInfo;

        EXECUTE format(
            'CREATE OR REPLACE FUNCTION nextTimestamp(id_ varchar) RETURNS vclock_and_hlc AS $D$
            BEGIN
                RETURN (
                    WITH T AS (
                        SELECT array [%s] as lts,
                            coalesce((SELECT (last_value, 0)::hlc FROM WallClockSeq), (0, 0)::hlc) as pts,
                            round(extract(epoch FROM clock_timestamp()) * 1000) as curr_time
                        FROM AllRows
                        where id = id_
                    )
                    SELECT (lts,
                        CASE WHEN curr_time > (T.pts).physical_time
                        THEN (curr_time, (SELECT setval(''SiteHybridLogicalTime'', 1)))::hlc
                        ELSE ((T.pts).physical_time, (SELECT nextval(''SiteHybridLogicalTime'')))::hlc END)::vclock_and_hlc
                    FROM T
                );
            END;
            $D$ LANGUAGE PLPGSQL;
            ', array_to_string(next_lts, ', ')
        );

        SELECT array_agg(format(
            'lts[%1$s] = maxes.m[%1$s]', site_id)
            ORDER BY site_id
        ) INTO match_lts
        FROM ClusterInfo;

        EXECUTE format(
            'CREATE OR REPLACE VIEW DataAll AS
                SELECT id, key, type, data, site, lts, pts, op, merged_at, ctid
                FROM (
                    WITH potential_max AS (
                        WITH maxes AS (
                            SELECT id, key, (
                                SELECT array [%s]
                                FROM LocalAndShared
                                WHERE id = t_.id AND key = t_.key
                            ) m
                            FROM (
                                SELECT DISTINCT id, key
                                FROM LocalAndShared
                            ) t_
                        )
                        SELECT maxes.id, maxes.key, type, data, site, lts, pts, op, merged_at, ctid
                        FROM LocalAndShared, maxes
                        WHERE LocalAndShared.id = maxes.id AND LocalAndShared.key = maxes.key
                            AND (%s)
                    )
                    SELECT t1.*, NOT vclock_lte(t1.lts, t2.lts) OR t1.lts = t2.lts lte
                    FROM potential_max t1
                    JOIN LocalAndShared t2 ON t1.id = t2.id AND t1.key = t2.key
                ) t
                GROUP BY id, key, type, data, site, lts, pts, op, merged_at, ctid
                HAVING bool_and(lte) = true;
        ', array_to_string(max_lts, ', '), array_to_string(match_lts, ' OR '));

        SELECT count(*) INTO n_sites
        FROM ClusterInfo;

        EXECUTE format('CREATE INDEX ON Local ((lts[%s]));', n_sites);

        RETURN true;
    END
$$ LANGUAGE PLPGSQL;


-- Computes the next next timestamp, or a zeroed clock if it is the first one
-- (when new sites are added, this function will be replaced by another which considers the extra ones)
CREATE OR REPLACE FUNCTION nextTimestamp(id_ varchar) RETURNS vclock_and_hlc AS $$
BEGIN
    RETURN (
        WITH T AS (
            SELECT array [coalesce(max(lts[1]), 0) + 1] as lts,
                coalesce((SELECT (last_value, 0)::hlc FROM WallClockSeq), (0, 0)::hlc) as pts,
                round(extract(epoch FROM clock_timestamp()) * 1000) as curr_time
            FROM AllRows
            WHERE id = id_
        )
        SELECT (lts,
            CASE WHEN curr_time > (T.pts).physical_time
            THEN (curr_time, (SELECT setval('SiteHybridLogicalTime', 1)))::hlc
            ELSE ((T.pts).physical_time, (SELECT nextval('SiteHybridLogicalTime')))::hlc END)::vclock_and_hlc
        FROM T
    );
END;
$$ LANGUAGE PLPGSQL;


-- Adds a trigger to ensure the referential integrity of some structure. This is used for nested
-- structures when we want to ensure that when we add a value to an inner structure, the outer
-- element remains even with concurrent removes. E.g., map of sets where we add to some set in that
-- map but concurrently remove the respective kv entry. In this case, the kv entry can be forced to
-- stay if we also perform an add to the map in the same transaction.
-- This function adds the trigger in all sites of the cluster.
-- src represents the identifiers of the source element (id and key for maps, or id for sets);
-- dst represents the identifier of the destination structure (id);
-- addFunc is the name of the function to add the src element.
CREATE OR REPLACE FUNCTION add_referential_integrity(src varchar[], dst varchar, addFunc varchar) RETURNS void AS $$
    BEGIN
        PERFORM dblink_exec(addr, format(
            'CREATE OR REPLACE FUNCTION referential_integrity_%s_%s_f() RETURNS TRIGGER AS $d$
            BEGIN
                PERFORM %s(%s, ''%s'');
                RETURN new;
            END;
            $d$ LANGUAGE PLPGSQL;
            ', array_to_string(src, '_'), dst, addFunc, (SELECT string_agg(quote_literal(x), ',') FROM unnest(src) AS x), dst))
        FROM ClusterInfo;

        PERFORM dblink_exec(addr, format(
            'CREATE OR REPLACE TRIGGER referential_integrity_%s_%s
            AFTER INSERT ON SHARED
            FOR EACH ROW
            WHEN (new.id = ''%s'')
            EXECUTE FUNCTION referential_integrity_%s_%s_f();
            ', array_to_string(src, '_'), dst, dst, array_to_string(src, '_'), dst))
        FROM ClusterInfo;
    END;
$$ LANGUAGE PLPGSQL;


-- Removes a referential integrity trigger.
-- This function removes the trigger in all sites of the cluster.
-- src represents the identifiers of the source element (id and key for maps, or id for sets);
-- dst represents the identifier of the destination structure (id);
CREATE OR REPLACE FUNCTION rmv_referential_integrity(src varchar[], dst varchar) RETURNS void AS $$
    BEGIN
        PERFORM dblink_exec(addr, format(
            'DROP FUNCTION referential_integrity_%s_%s_f CASCADE', array_to_string(src, '_'), dst))
        FROM ClusterInfo;
    END;
$$ LANGUAGE PLPGSQL;


-- Locks the respective row in the StructureControl table by a given id.
-- Used to avoid conflicts in the merge procedure.
CREATE OR REPLACE FUNCTION _access_elem_(id_ varchar) RETURNS void AS $$
BEGIN
    INSERT INTO StructureControl VALUES (id_, 1)
    ON CONFLICT (id)
    DO UPDATE
    SET count = StructureControl.count + 1;
END;
$$ LANGUAGE PLPGSQL;

-- Uses Postgres' advisory lock function to lock an item. Alternative version to the one above.
-- Since no table is updated, it should in theory be slightly faster.
-- Used to avoid conflicts in the merge procedure.
CREATE OR REPLACE FUNCTION _access_elem(id_ varchar) RETURNS void AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(id_, 0));
END;
$$ LANGUAGE PLPGSQL;

-- Deletes operations in the causal past of the respective CRDT
-- (used by the merge function)
CREATE OR REPLACE FUNCTION _delete_past_ops(id_ varchar, key_ varchar, lts_ vclock) RETURNS void AS $$
BEGIN
    DELETE
    FROM Local
    WHERE id = id_
        AND key = key_
        AND vclock_lte(lts, lts_)
        AND lts <> lts_;
END;
$$ LANGUAGE PLPGSQL;

-- Checks if an operation already exists in Local table
-- Returns true if operation exists (exact match or superseded by newer operation)
CREATE OR REPLACE FUNCTION _is_operation_already_in_local(id_ varchar, key_ varchar, lts_ vclock) RETURNS boolean AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1
        FROM Local
        WHERE id = id_
            AND key = key_
            AND vclock_lte(lts_, lts)
    );
END;
$$ LANGUAGE PLPGSQL;

-- computes whether this operation is already obsolete in the context of the CRDT;
-- (used by the merge function)
CREATE OR REPLACE FUNCTION _is_operation_obsolete(id_ varchar, key_ varchar, lts_ vclock) RETURNS boolean AS $$
BEGIN
    -- check if this operation was replaced by a future operation
    RETURN count(*) > 0
    FROM Local
    WHERE id = id_
        AND key = key_
        AND vclock_lte(lts_, lts);
END;
$$ LANGUAGE PLPGSQL;


-- Merges a new operation with the existing data
CREATE OR REPLACE FUNCTION merge(id_ varchar, key_ varchar, type_ "char", data_ varchar, site_ int, lts_ vclock, pts_ hlc, op_ "char")
RETURNS void AS $$
    BEGIN
        -- acquire a lock to the element
        -- (to avoid conflicts, e.g., deadlocks when deleting past versions)
        PERFORM _access_elem(id_ || '.' || key_);

        -- find if this operation was already applied or if it is an obsolete operation
        IF _is_operation_obsolete(id_, key_, lts_) THEN
            RETURN;
        END IF;

        -- remove obsolete entries
        -- i.e., operations on the same element in the causal past
        PERFORM _delete_past_ops(id_, key_, lts_);

        INSERT INTO Local
        VALUES (id_, key_, type_, data_, site_, lts_, pts_, op_, currentTimeMillis());

        -- update the wall clock
        PERFORM setval('WallClockSeq', greatest((pts_).physical_time, (SELECT last_value FROM WallClockSeq)) , true);
    END
$$ LANGUAGE PLPGSQL;


-- Adds the operation into the Shared table
CREATE OR REPLACE FUNCTION handleOp(id_ varchar, key_ varchar, type_ "char", data_ varchar, site_ int, lts_ vclock, pts_ hlc, op_ "char") RETURNS void AS $$
    BEGIN
        INSERT INTO Shared VALUES (id_, key_, type_, data_, site_, lts_, pts_, op_, default);
    END;
$$ LANGUAGE PLPGSQL;


-- Merges a batch of a partition of the Shared table.
CREATE OR REPLACE FUNCTION merge_batch(batch xid[]) RETURNS bool AS $$
BEGIN
    PERFORM merge(id, key, type, data, site, lts, pts, op)
    FROM Shared
    WHERE xmin IN (
        SELECT unnest(batch)
    )
    ORDER BY id, key;

    DELETE
    FROM Shared
    WHERE xmin = ANY(batch);

    RETURN true;
END
$$ LANGUAGE PLPGSQL;


-- Continuously merges batches of a partition until the entire partition has been merged.
-- Partitions are computed by hashing the key and performing the modulo function over 'num_partitions'.
-- Each partition is divided into batches of at most 'max_batch_size' rows. Batching is done to
-- separate merges into multiple transactions, reducing the amount of time each lock is held.
CREATE OR REPLACE PROCEDURE merge_partition(partition integer, num_partitions integer, max_batch_size integer) AS $$
DECLARE
    batches xid[][];
    i integer;
    j integer;
BEGIN
    -- build an array with the transaction id of all rows in this partition
    SELECT array_agg(xmin ORDER BY xmin::text::bigint DESC) INTO batches
    FROM Shared
    -- ensures that all data from the same transaction ends up in the same partition
    WHERE xmin::text::bigint % num_partitions = partition;

    i := 1;
    WHILE i <= array_length(batches, 1) LOOP
        COMMIT;

        BEGIN
            j := i + max_batch_size;
            -- ensure that all data from the same transaction is merged in the same transaction
            WHILE j <= array_length(batches, 1) AND batches[j] = batches[j - 1] LOOP
                j := j + 1;
            END LOOP;

            PERFORM merge_batch(batches[i:j-1]);

            i := j;
        EXCEPTION WHEN others THEN
            RAISE WARNING 'Exception when merging.';
            ROLLBACK;
        END;
    END LOOP;
END
$$ LANGUAGE PLPGSQL;


-- To schedule the merge daemon process
CREATE EXTENSION IF NOT EXISTS pg_background;


-- Manually merges all operations in the Shared table
CREATE OR REPLACE FUNCTION merge() RETURNS void AS $$
BEGIN
    PERFORM *
    FROM pg_background_result(pg_background_launch(format('CALL merge_partition(0, 1, 1000)'))) as (x text);
END;
$$ LANGUAGE PLPGSQL;


-- Continuously calls the merge functions for each partition, for at most 'workers' partitions.
-- Also receives the delta between merges, in seconds.
CREATE OR REPLACE PROCEDURE merge_daemon(workers int, delta float, max_batch_size integer) AS $$
    BEGIN
        FOR i in 1..workers LOOP
            PERFORM dblink_connect('c' || i, 'dbname=' || current_database());
        END LOOP;

        LOOP
            COMMIT;

            -- the row count to determine if is it necessary to call the merge workers is also
            -- done in a separate process, to not acquire a shared lock forever
            PERFORM * FROM pg_background_result(pg_background_launch('SELECT 1 FROM Shared LIMIT 1')) as (x int);

            IF FOUND THEN
                -- constantly inserting and cleaning the Shared table leads to stale statistics;
                -- sometimes the planner would choose a nested loop for the merge_partition function,
                -- as it thought that the table had no rows, resulting in considerably slow plans.
                PERFORM * FROM pg_background_result(pg_background_launch('ANALYZE Shared')) as (x text);

                FOR i in 1..workers LOOP
                    PERFORM dblink_send_query('c' || i, format('CALL merge_partition(%s, %s, %s)',  (i - 1), workers, max_batch_size));
                END LOOP;

                FOR i in 1..workers LOOP
                    COMMIT;

                    -- wait until the partition has been merged. the code is split into multiple
                    -- transactions to avoid long chains of dead tuples that cannot be vacuumed due
                    -- to long-running transactions.
                    WHILE dblink_is_busy('c' || i) = 1 LOOP
                        PERFORM pg_sleep(0.1);
                        COMMIT;
                    END LOOP;

                    PERFORM * FROM dblink_get_result('c' || i) as (x text);
                    PERFORM * FROM dblink_get_result('c' || i) as (x text);
                END LOOP;
            END IF;

            COMMIT;

            PERFORM pg_sleep(delta);
        END LOOP;
    END;
$$ LANGUAGE PLPGSQL;


-- Stop the merge daemon process
CREATE OR REPLACE FUNCTION unschedule_merge_daemon() RETURNS void AS $$
BEGIN
    PERFORM pg_terminate_backend(pid)
    FROM pg_stat_activity
    WHERE (query LIKE 'CALL merge_daemon%' OR query LIKE 'CALL merge_partition%')
        AND datname = current_database();
END;
$$ LANGUAGE PLPGSQL;


-- Start the merge daemon process with 'workers' partitions, stopping the previous if it exists.
CREATE OR REPLACE FUNCTION schedule_merge_daemon(workers int, delta float, max_batch_size integer) RETURNS void AS $$
BEGIN
    PERFORM unschedule_merge_daemon();
    PERFORM pg_background_launch(format('CALL merge_daemon(%s, %s, %s)', workers, delta, max_batch_size));
END;
$$ LANGUAGE PLPGSQL;


-- Manual replication function (currently a noop)
CREATE OR REPLACE FUNCTION replicate() RETURNS void AS $$
BEGIN
    RETURN;
END;
$$ LANGUAGE PLPGSQL;


-- Switches the read mode to 'local' or 'all'.
-- local - considers only the data on the local table
-- all - considers the data in the local and shared tables
CREATE OR REPLACE FUNCTION switch_read_mode(mode varchar) RETURNS void AS $$
    BEGIN
        IF mode = 'local' THEN
            CREATE OR REPLACE VIEW Data AS
            SELECT *
            FROM DataLocal;

            CREATE OR REPLACE VIEW AllRows AS
            SELECT *
            FROM DataLocal;

            ALTER TABLE Shared DISABLE TRIGGER Shared_wall_clock_trigger;

            FOR i in 1..(SELECT count(*) FROM ClusterInfo) LOOP
                EXECUTE format('DROP INDEX IF EXISTS Shared_lts_%s;', i);
            END LOOP;
        ELSEIF mode = 'all' THEN
            CREATE OR REPLACE VIEW Data AS
            SELECT *
            FROM DataAll;

            CREATE OR REPLACE VIEW AllRows AS
            SELECT *
            FROM LocalAndShared;

            ALTER TABLE Shared ENABLE REPLICA TRIGGER Shared_wall_clock_trigger;

            FOR i in 1..(SELECT count(*) FROM ClusterInfo) LOOP
                EXECUTE format('CREATE INDEX IF NOT EXISTS Shared_lts_%s ON Shared ((lts[%s]));', i, i);
            END LOOP;
        ELSE
            RAISE EXCEPTION 'Mode ''%'' does not exist. The supported modes are ''local'' and ''all''.', mode;
        END IF;
    END;
$$ LANGUAGE PLPGSQL;


-- Switches the write mode to 'sync' or 'async'.
-- sync - writes are immediately merged
-- async - writes must be merged manually with the merge() function
CREATE OR REPLACE FUNCTION switch_write_mode(mode varchar) RETURNS void AS $$
    BEGIN
        IF mode = 'sync' THEN
            ALTER TABLE Shared ENABLE TRIGGER Shared_insert_local_sync_trigger;
        ELSEIF mode = 'async' THEN
            ALTER TABLE Shared DISABLE TRIGGER Shared_insert_local_sync_trigger;
        ELSE
            RAISE EXCEPTION 'Mode ''%'' does not exist. The supported modes are ''sync'' and ''async''.', mode;
        END IF;
    END;
$$ LANGUAGE PLPGSQL;


-- Waits until the time of last message received by a subscription remains unchanged during 1s,
-- and there isn't a merge in process or the shared table is empty .
CREATE OR REPLACE FUNCTION wait_for_replication() RETURNS void AS $$
    DECLARE last_send_time timestamp;
            curr_send_time timestamp;
            merge_count integer;
            shared_count integer;
    BEGIN
        LOOP
            -- the coalesce is used in case there are no replications, i.e., there is only this site
            -- in the cluster; the now() is always the same inside a transaction, so in that case
            -- this now() will return the same value as the now() below, ending the loop.
            SELECT coalesce(max(last_msg_send_time), now()) INTO last_send_time
            FROM pg_stat_subscription
            WHERE subname like 'sub_' || (SELECT siteid()) || '_%';

            PERFORM pg_sleep(1);

            SELECT coalesce(max(last_msg_send_time), now()) INTO curr_send_time
            FROM pg_stat_subscription
            WHERE subname like 'sub_' || (SELECT siteid()) || '_%';

            SELECT count(*) INTO merge_count
            FROM pg_stat_activity
            WHERE query LIKE 'CALL merge_daemon(%'
                AND state <> 'idle'
                AND datname = current_database();

            SELECT count(*) INTO shared_count
            FROM Shared;

            EXIT WHEN last_send_time = curr_send_time AND (merge_count = 0 OR shared_count = 0);
        END LOOP;
    END;
$$ LANGUAGE PLPGSQL;


-- Clears the tables and sequences created by this schema, except for the cluster info)
CREATE OR REPLACE FUNCTION reset_data() RETURNS void AS $$
    BEGIN
        -- wait for all data to be replicated, to avoid deadlocks
        PERFORM wait_for_replication();

        TRUNCATE Local;

        TRUNCATE Shared;

        ALTER SEQUENCE SiteHybridLogicalTime RESTART;
    END;
$$ LANGUAGE PLPGSQL;
