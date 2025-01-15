-- Counter utility functions

-- Get a counter by id
CREATE OR REPLACE FUNCTION counterGet(id_ varchar) RETURNS bigint AS $$
BEGIN
    RETURN data
    FROM Counter
    WHERE id = id_;
END;
$$ LANGUAGE PLPGSQL;


-- Increment a delta to a counter
CREATE OR REPLACE FUNCTION counterInc(id_ varchar, delta_ bigint) RETURNS void AS $$
BEGIN
    -- unlike other structures, all operations affect the final result, so same-site concurrency
    -- must be avoided
    PERFORM _access_elem(id_);

    INSERT INTO Data (id, key, type, data, site, lts, pts, op)
    SELECT id_, siteId(), 'c', 
        (SELECT coalesce((SELECT data::bigint FROM Data WHERE id = id_ AND key::integer = siteId()), 0)) + delta_, 
        siteId(), lts, pts, 'a'
    FROM nextTimestamp(id_);
END;
$$ LANGUAGE PLPGSQL;

-- Decrement a delta to a counter
CREATE OR REPLACE FUNCTION counterDec(id_ varchar, delta_ bigint) RETURNS void AS $$
BEGIN
    -- unlike other structures, all operations affect the final result, so same-site concurrency
    -- must be avoided
    PERFORM _access_elem(id_);

    INSERT INTO Data (id, key, type, data, site, lts, pts, op)
    SELECT id_, siteId(), 'c', 
        (SELECT coalesce((SELECT data::bigint FROM Data WHERE id = id_ AND key::integer = siteId()), 0)) - delta_, 
        siteId(), lts, pts, 'a'
    FROM nextTimestamp(id_);
END;
$$ LANGUAGE PLPGSQL;
