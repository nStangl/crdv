-- Map utility functions


-- Get a map by id (add wins + mvr)
CREATE OR REPLACE FUNCTION mapAwMvrGet(id_ varchar) RETURNS mEntryMvr[] AS $$
BEGIN
    RETURN data
    FROM MapAwMvrTuple
    WHERE id = id_;
END;
$$ LANGUAGE PLPGSQL;

-- Get a map by id (add wins + lww)
CREATE OR REPLACE FUNCTION mapAwLwwGet(id_ varchar) RETURNS mEntry[] AS $$
BEGIN
    RETURN data
    FROM MapAwLwwTuple
    WHERE id = id_;
END;
$$ LANGUAGE PLPGSQL;

-- Get a map by id (remove wins)
CREATE OR REPLACE FUNCTION mapRwMvrGet(id_ varchar) RETURNS mEntryMvr[] AS $$
BEGIN
    RETURN data
    FROM MapRwMvrTuple
    WHERE id = id_;
END;
$$ LANGUAGE PLPGSQL;

-- Get a map by id (lww)
CREATE OR REPLACE FUNCTION mapLwwGet(id_ varchar) RETURNS mEntry[] AS $$
BEGIN
    RETURN data
    FROM MapLwwTuple
    WHERE id = id_;
END;
$$ LANGUAGE PLPGSQL;


-- Get the value of a map by key (add wins + mvr)
CREATE OR REPLACE FUNCTION mapAwMvrValue(id_ varchar, key_ varchar) RETURNS varchar[] AS $$
BEGIN
    RETURN (data).value
    FROM MapAwMvr
    WHERE id = id_
        AND (data).key = key_;
END;
$$ LANGUAGE PLPGSQL;

-- Get the value of a map by key (add wins + lww)
CREATE OR REPLACE FUNCTION mapAwLwwValue(id_ varchar, key_ varchar) RETURNS varchar AS $$
BEGIN
    RETURN (data).value
    FROM MapAwLww
    WHERE id = id_
        AND (data).key = key_;
END;
$$ LANGUAGE PLPGSQL;

-- Get the value of a map by key (remove wins)
CREATE OR REPLACE FUNCTION mapRwMvrValue(id_ varchar, key_ varchar) RETURNS varchar[] AS $$
BEGIN
    RETURN (data).value
    FROM MapRwMvr
    WHERE id = id_
        AND (data).key = key_;
END;
$$ LANGUAGE PLPGSQL;

-- Get the value of a map by key (lww)
CREATE OR REPLACE FUNCTION mapLwwValue(id_ varchar, key_ varchar) RETURNS varchar AS $$
BEGIN
    RETURN (data).value
    FROM MapLww
    WHERE id = id_
        AND (data).key = key_;
END;
$$ LANGUAGE PLPGSQL;


-- Check if a key is in a map (add wins + mvr)
CREATE OR REPLACE FUNCTION mapAwMvrContains(id_ varchar, key_ varchar) RETURNS bool AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1
        FROM MapAwMvr
        WHERE id = id_
            AND (data).key = key_
    );
END;
$$ LANGUAGE PLPGSQL;

-- Check if a key is in a map (add wins + lww)
CREATE OR REPLACE FUNCTION mapAwLwwContains(id_ varchar, key_ varchar) RETURNS bool AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1
        FROM MapAwLww
        WHERE id = id_
            AND (data).key = key_
    );
END;
$$ LANGUAGE PLPGSQL;

-- Check if a key is in a map (remove wins)
CREATE OR REPLACE FUNCTION mapRwMvrContains(id_ varchar, key_ varchar) RETURNS bool AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1
        FROM MapRwMvr
        WHERE id = id_
            AND (data).key = key_
    );
END;
$$ LANGUAGE PLPGSQL;

-- Check if a key is in a map (lww)
CREATE OR REPLACE FUNCTION mapLwwContains(id_ varchar, key_ varchar) RETURNS bool AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1
        FROM MapLww
        WHERE id = id_
            AND (data).key = key_
    );
END;
$$ LANGUAGE PLPGSQL;


-- Add an entry to a map
CREATE OR REPLACE FUNCTION mapAdd(id_ varchar, key_ varchar, value_ varchar) RETURNS void AS $$
BEGIN
    INSERT INTO Data (id, key, type, data, site, lts, pts, op)
    SELECT id_, key_, 'm', value_, siteId(), (t).lts, (t).pts, 'a'
    FROM nextTimestamp(id_) AS t;
END
$$ LANGUAGE PLPGSQL;

-- Remove a map entry by key
CREATE OR REPLACE FUNCTION mapRmv(id_ varchar, key_ varchar) RETURNS void AS $$
BEGIN
    INSERT INTO Data (id, key, type, data, site, lts, pts, op)
    SELECT id_, key_, 'm', null, siteId(), (t).lts, (t).pts, 'r'
    FROM nextTimestamp(id_) AS t;
END
$$ LANGUAGE PLPGSQL;

-- Clear a map, i.e., remove all entries
CREATE OR REPLACE FUNCTION mapClear(id_ varchar) RETURNS void AS $$
BEGIN
    INSERT INTO Data (id, key, type, data, site, lts, pts, op)
    SELECT id_, (data).key, 'm', null, siteId(), (t).lts, (t).pts, 'r'
    FROM MapAwMvr
    JOIN nextTimestamp(id_) AS t ON true
    WHERE id = id_
    ORDER BY id_, (data).key;
END
$$ LANGUAGE PLPGSQL;
