-- Set utility functions


-- Get a set by id (add wins)
CREATE OR REPLACE FUNCTION setAwGet(id_ varchar) RETURNS varchar[] AS $$
BEGIN
    RETURN data
    FROM SetAwTuple
    WHERE id = id_;
END;
$$ LANGUAGE PLPGSQL;

-- Get a set by id (remove wins)
CREATE OR REPLACE FUNCTION setRwGet(id_ varchar) RETURNS varchar[] AS $$
BEGIN
    RETURN data
    FROM SetRwTuple
    WHERE id = id_;
END;
$$ LANGUAGE PLPGSQL;

-- Get a set by id (lww)
CREATE OR REPLACE FUNCTION setLwwGet(id_ varchar) RETURNS varchar[] AS $$
BEGIN
    RETURN data
    FROM SetLwwTuple
    WHERE id = id_;
END;
$$ LANGUAGE PLPGSQL;


-- Check if an element is in a set (add wins)
CREATE OR REPLACE FUNCTION setAwContains(id_ varchar, elem_ varchar) RETURNS bool AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1
        FROM SetAw
        WHERE id = id_
            AND data = elem_
    );
END;
$$ LANGUAGE PLPGSQL;

-- Check if an element is in a set (remove wins)
CREATE OR REPLACE FUNCTION setRwContains(id_ varchar, elem_ varchar) RETURNS bool AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1
        FROM SetRw
        WHERE id = id_
            AND data = elem_
    );
END;
$$ LANGUAGE PLPGSQL;

-- Check if an element is in a set (lww)
CREATE OR REPLACE FUNCTION setLwwContains(id_ varchar, elem_ varchar) RETURNS bool AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1
        FROM SetLww
        WHERE id = id_
            AND data = elem_
    );
END;
$$ LANGUAGE PLPGSQL;


-- Add a value to a set
CREATE OR REPLACE FUNCTION setAdd(id_ varchar, value_ varchar) RETURNS void AS $$
BEGIN
    INSERT INTO Data (id, key, type, data, site, lts, pts, op)
    SELECT id_, value_, 's', null, siteId(), (t).lts, (t).pts, 'a'
    FROM nextTimestamp(id_) AS t;
END;
$$ LANGUAGE PLPGSQL;


-- Remove a value from a set
CREATE OR REPLACE FUNCTION setRmv(id_ varchar, value_ varchar) RETURNS void AS $$
BEGIN
    INSERT INTO Data (id, key, type, data, site, lts, pts, op)
    SELECT id_, value_, 's', null, siteId(), (t).lts, (t).pts, 'r'
    FROM nextTimestamp(id_) AS t;
END;
$$ LANGUAGE PLPGSQL;

-- Clear set, i.e., remove all elements
CREATE OR REPLACE FUNCTION setClear(id_ varchar) RETURNS void AS $$
BEGIN
    INSERT INTO Data (id, key, type, data, site, lts, pts, op)
    SELECT id_, data, 's', null, siteId(), (t).lts, (t).pts, 'r'
    FROM SetAw
    JOIN nextTimestamp(id_) AS t ON true
    WHERE id = id_
    ORDER BY id_, data;
END;
$$ LANGUAGE PLPGSQL;
