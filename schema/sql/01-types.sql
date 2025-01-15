-- Hybrid logical clock type
CREATE TYPE hlc AS (
    physical_time bigint,
    logical_time int
);


-- Vector clock type
CREATE DOMAIN vclock AS bigint[];


-- Vector + HLC clocks
CREATE TYPE vclock_and_hlc AS (
    lts vclock,
    pts hlc
);


-- Create clock functions
DO $x$
BEGIN
    -- C code with helper functions over vclock and hlc clocks
    CREATE EXTENSION clocks;

    EXCEPTION
        WHEN feature_not_supported THEN
            -- Computes whether the vclock v1 happens before v2
            -- (i.e., each element of v1 is <= the corresponding element of v2)
            CREATE OR REPLACE FUNCTION vclock_lte(v1 vclock, v2 vclock) RETURNS bool AS $$
            BEGIN
                RETURN bool_and(coalesce(u1, 0) <= coalesce(u2, 0))
                FROM (
                    SELECT unnest(v1) AS u1, unnest(v2) AS u2
                ) t;
            END;
            $$ LANGUAGE PLPGSQL;

            -- Computes the pointwise max vclock of two vclocks
            CREATE OR REPLACE FUNCTION vclock_max(v1 vclock, v2 vclock) RETURNS vclock AS $$
            BEGIN
                RETURN array_agg(u)
                FROM (
                    SELECT greatest(unnest(v1), unnest(v2)) AS u
                ) t;
            END;
            $$ LANGUAGE PLPGSQL;

            -- Computes the next hybrid logical clock
            CREATE OR REPLACE FUNCTION next_hlc(curr hlc)
            RETURNS hlc AS $$
                DECLARE current_time_ms bigint;
                BEGIN
                    -- first get the max between the old and the current
                    SELECT currentTimeMillis() INTO current_time_ms;

                    IF current_time_ms > (curr).physical_time THEN
                        RETURN (current_time_ms, 1)::hlc;
                    ELSE
                        RETURN ((curr).physical_time, (curr).logical_time + 1)::hlc;
                    END IF;
                END
            $$ LANGUAGE PLPGSQL;
END $x$;


-- Represents a map entry
CREATE TYPE mEntry AS (
    key varchar COLLATE "C",
    value varchar
);


-- Represents a map entry with multi-value register values
CREATE TYPE mEntryMvr AS (
    key varchar COLLATE "C",
    value varchar[]
);
