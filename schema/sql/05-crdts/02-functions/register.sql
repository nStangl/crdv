-- Register utility functions


-- Get a register by id (mvr)
CREATE OR REPLACE FUNCTION registerMvrGet(id_ varchar) RETURNS varchar[] AS $$
BEGIN
    RETURN data
    FROM RegisterMvr
    WHERE id = id_;
END;
$$ LANGUAGE PLPGSQL;

-- Get a register by id (mvr)
CREATE OR REPLACE FUNCTION registerLwwGet(id_ varchar) RETURNS varchar AS $$
BEGIN
    RETURN data
    FROM RegisterLww
    WHERE id = id_;
END;
$$ LANGUAGE PLPGSQL;


-- Set a register value
CREATE OR REPLACE FUNCTION registerSet(id_ varchar, value_ varchar) RETURNS void AS $$
BEGIN
    INSERT INTO Data (id, key, type, data, site, lts, pts, op)
    SELECT id_, '', 'r', value_, siteId(), (t).lts, (t).pts, 'a'
    FROM nextTimestamp(id_) AS t;
END;
$$ LANGUAGE PLPGSQL;
