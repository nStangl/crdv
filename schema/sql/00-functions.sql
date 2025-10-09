-- Returns the current time in milliseconds since epoch
CREATE OR REPLACE FUNCTION currentTimeMillis() RETURNS bigint AS $$
BEGIN
    RETURN round(extract(epoch FROM clock_timestamp()) * 1000);
END;
$$ LANGUAGE PLPGSQL;
