-- List utility functions (also works for Stacks and Queues)


-- Get a list by id
CREATE OR REPLACE FUNCTION listGet(id_ varchar) RETURNS varchar[] AS $$
BEGIN
    RETURN data
    FROM ListTuple
    WHERE id = id_;
END;
$$ LANGUAGE PLPGSQL;


-- Get the element from a list at some index
CREATE OR REPLACE FUNCTION listGetAt(id_ varchar, index_ int) RETURNS varchar AS $$
BEGIN
    RETURN data
    FROM List
    WHERE id = id_
    OFFSET index_
    LIMIT 1;
END;
$$ LANGUAGE PLPGSQL;


-- Get the first element in a list
CREATE OR REPLACE FUNCTION listGetFirst(id_ varchar) RETURNS varchar AS $$
BEGIN
    RETURN data
    FROM _ListUnsorted
    WHERE id = id_
        AND pos = (
            SELECT min(pos)
            FROM _ListUnsorted
            WHERE id = id_
        );
    END;
$$ LANGUAGE PLPGSQL;


-- Get the last element in a list
CREATE OR REPLACE FUNCTION listGetLast(id_ varchar) RETURNS varchar AS $$
BEGIN
    RETURN data
    FROM _ListUnsorted
    WHERE id = id_
        AND pos = (
            SELECT max(pos)
            FROM _ListUnsorted
            WHERE id = id_
        );
    END;
$$ LANGUAGE PLPGSQL;


-- Id generation functions
DO $x$
BEGIN
    -- C functions with list id generation
    CREATE EXTENSION list_ids;

    CREATE OR REPLACE FUNCTION _generateVirtualIndexBetween(p1 varchar, p2 varchar) RETURNS varchar AS $$
    BEGIN
        RETURN generateVirtualIndexBetweenRegular(p1, p2);
    END;
    $$ LANGUAGE PLPGSQL;

    CREATE OR REPLACE FUNCTION switch_list_id_generation(mode varchar) RETURNS void AS $$
    BEGIN
        IF mode NOT IN ('regular', 'appends', 'prepends') THEN
            RAISE EXCEPTION 'Mode ''%'' does not exist. The supported modes are ''regular'', ''appends'', and ''prepends''.', mode;
        END IF;

        EXECUTE format(
            'CREATE OR REPLACE FUNCTION _generateVirtualIndexBetween(p1 varchar, p2 varchar) RETURNS varchar AS $D$
            BEGIN
                RETURN generateVirtualIndexBetween%s(p1, p2);
            END;
            $D$ LANGUAGE PLPGSQL;',
        initCap(mode));
    END;
    $$ LANGUAGE plpgsql;

    -- extension is not installed, default to SQL code to generate ids
    EXCEPTION
        WHEN feature_not_supported THEN
            -- performs the best in random inserts (default)
            CREATE OR REPLACE FUNCTION _char_between_regular(c1 char, c2 char) RETURNS char AS $$
            BEGIN
                RETURN chr((ascii(c1) + ascii(c2)) / 2);
            END;
            $$ LANGUAGE PLPGSQL;

            -- performs the best for appends
            CREATE OR REPLACE FUNCTION _char_between_appends(c1 char, c2 char) RETURNS char AS $$
            BEGIN
                RETURN chr(ascii(c1) + 1);
            END;
            $$ LANGUAGE PLPGSQL;

            -- performs the best for prepends
            CREATE OR REPLACE FUNCTION _char_between_prepends(c1 char, c2 char) RETURNS char AS $$
            BEGIN
                RETURN chr(ascii(c2) - 1);
            END;
            $$ LANGUAGE PLPGSQL;

            -- generation function used by _generateVirtualIndexBetween
            CREATE OR REPLACE FUNCTION _char_between(c1 char, c2 char) RETURNS char AS $$
            BEGIN
                RETURN _char_between_regular(c1, c2);
            END;
            $$ LANGUAGE PLPGSQL;

            CREATE OR REPLACE FUNCTION switch_list_id_generation(mode varchar) RETURNS void AS $$
            BEGIN
                IF mode NOT IN ('regular', 'appends', 'prepends') THEN
                    RAISE EXCEPTION 'Mode ''%'' does not exist. The supported modes are ''regular'', ''appends'', and ''prepends''.', mode;
                END IF;

                EXECUTE format(
                    'CREATE OR REPLACE FUNCTION _char_between(c1 char, c2 char) RETURNS char AS $D$
                    BEGIN
                        RETURN _char_between_%s(c1, c2);
                    END;
                    $D$ LANGUAGE PLPGSQL;',
                mode);
            END;
            $$ LANGUAGE PLPGSQL;

            -- Generate a new virtual index between two virtual indexes
            -- (e.g., (a, c) -> b, (a, b) -> aP)
            CREATE OR REPLACE FUNCTION _generateVirtualIndexBetween(p1_ varchar, p2_ varchar) RETURNS varchar AS $$
            BEGIN
                -- build the string from the chars
                RETURN string_agg(c, '' ORDER BY r)
                FROM (
                    -- get the resulting chars:
                    -- if the difference between c1 and c2 is zero or one, the resulting char is c1;
                    -- otherwise, the (final) char is dictated by the _char_between function
                    SELECT coalesce(nullif(ascii(c2) - ascii(c1), 1), 0) AS diff,
                        CASE WHEN ascii(c2) - ascii(c1) <= 1  THEN c1 ELSE _char_between(c1, c2) END AS c,
                        -- rolling sum of all previous rows, excluding the current one
                        sum(coalesce(nullif(ascii(c2) - ascii(c1), 1), 0)) over (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
                        row_number() over() AS r
                    FROM (
                        -- add padding (first char for p1, last char for p2) and split each char to rows
                        SELECT regexp_split_to_table(rpad(p1, greatest(length(p1), length(p2)) + 1, '!'), '') c1,
                            regexp_split_to_table(rpad(p2, greatest(length(p1), length(p2)) + 1, chr(127)), '') c2
                        FROM (
                            SELECT coalesce(p1_, '') as p1, coalesce(p2_, '') as p2
                        )
                    ) t
                ) t
                -- with this, we keep all chars until the first with diff != 0 or 1
                WHERE sum IS NULL OR sum = 0;
            END;
            $$ LANGUAGE PLPGSQL;
END $x$;


-- Given a physical index (e.g., 0, 1, 2, ...), return the new virtual index that when inserted
-- to the list will be placed in the specified physical location
-- (index_ >= 0)
CREATE OR REPLACE FUNCTION _physicalToVirtualIndex(id_ varchar, index_ bigint) RETURNS varchar AS $$
BEGIN
    RETURN (
        WITH T AS (
            SELECT pos
            FROM List
            WHERE id = id_
            OFFSET greatest(index_ - 1, 0)
            LIMIT (CASE WHEN index_ = 0 THEN 1 ELSE 2 END)
        )
        SELECT _generateVirtualIndexBetween(
            -- position before the new insert
            (SELECT (CASE WHEN index_ = 0 THEN '' ELSE pos END) FROM T LIMIT 1),
            -- position after the new insert
            (SELECT pos FROM T OFFSET (CASE WHEN index_ = 0 THEN 0 ELSE 1 END) LIMIT 1)
        ) || siteId() -- append the site id to ensure uniqueness
    );
END;
$$ LANGUAGE PLPGSQL;


-- Return the virtual index to point to the last position of some list
CREATE OR REPLACE FUNCTION _lastVirtualIndex(id_ varchar) RETURNS varchar AS $$
BEGIN
    RETURN _generateVirtualIndexBetween(
        -- last position
        (SELECT max(pos) FROM _ListUnsorted WHERE id = id_),
        ''
    ) || siteId(); -- append the site id to ensure uniqueness
END;
$$ LANGUAGE PLPGSQL;


-- Return the virtual index to point to the first position of some list
CREATE OR REPLACE FUNCTION _firstVirtualIndex(id_ varchar) RETURNS varchar AS $$    
BEGIN
    RETURN _generateVirtualIndexBetween(
        '',
        -- first position
        (SELECT min(pos) FROM _ListUnsorted WHERE id = id_)
    ) || siteId(); -- append the site id to ensure uniqueness
END;
$$ LANGUAGE PLPGSQL;


-- Add an element to a list at some index
CREATE OR REPLACE FUNCTION listAdd(id_ varchar, index_ bigint, elem_ varchar) RETURNS void AS $$
BEGIN
    INSERT INTO Data (id, key, type, data, site, lts, pts, op)
    SELECT id_, (SELECT _physicalToVirtualIndex(id_, index_)), 'l', elem_, siteId(), (t).lts, (t).pts, 'a'
    FROM nextTimestamp(id_) AS t;
END;
$$ LANGUAGE PLPGSQL;


-- Add an element to the end of some list
CREATE OR REPLACE FUNCTION listAppend(id_ varchar, elem_ varchar) RETURNS void AS $$
BEGIN
    INSERT INTO Data (id, key, type, data, site, lts, pts, op)
    SELECT id_, (SELECT _lastVirtualIndex(id_)), 'l', elem_, siteId(), (t).lts, (t).pts, 'a'
    FROM nextTimestamp(id_) AS t;
END;
$$ LANGUAGE PLPGSQL;


-- Add an element to the beginning of some list
CREATE OR REPLACE FUNCTION listPrepend(id_ varchar, elem_ varchar) RETURNS void AS $$
BEGIN
    INSERT INTO Data (id, key, type, data, site, lts, pts, op)
    SELECT id_, (SELECT _firstVirtualIndex(id_)), 'l', elem_, siteId(), (t).lts, (t).pts, 'a'
    FROM nextTimestamp(id_) AS t;
END;
$$ LANGUAGE PLPGSQL;


-- Remove an element from a list at some index
CREATE OR REPLACE FUNCTION listRmv(id_ varchar, index_ bigint) RETURNS void AS $$
BEGIN
    INSERT INTO Data (id, key, type, data, site, lts, pts, op)
    SELECT id_, P.pos, 'l', null, siteId(), (t).lts, (t).pts, 'r'
    FROM nextTimestamp(id_) AS t
    JOIN (
        SELECT pos FROM List WHERE id = id_ OFFSET index_ LIMIT 1
    ) P ON true;
END;
$$ LANGUAGE PLPGSQL;


-- Clear a list, i.e., remove all elements
CREATE OR REPLACE FUNCTION listClear(id_ varchar) RETURNS void AS $$
BEGIN
    INSERT INTO Data (id, key, type, data, site, lts, pts, op)
    SELECT id_, pos, 'l', null, siteId(), (t).lts, (t).pts, 'r'
    FROM List
    JOIN nextTimestamp(id_) AS t ON true
    WHERE id = id_
    ORDER BY id_, pos;
END;
$$ LANGUAGE PLPGSQL;


-- Removes and returns the first element in a list
CREATE OR REPLACE FUNCTION listPopFirst(id_ varchar) RETURNS varchar AS $$
BEGIN
    WITH select_cte AS (
        SELECT listGetFirst(id_)
    ), insert_cte AS (
        INSERT INTO Data (id, key, type, data, site, lts, pts, op)
        SELECT id_, (SELECT min(pos) FROM _ListUnsorted WHERE id = id_), 'l', null, siteId(), (t).lts, (t).pts, 'r'
        FROM nextTimestamp(id_) AS t
    )
    SELECT *
    FROM select_cte;
END;
$$ LANGUAGE PLPGSQL;


-- Removes and returns the last element in a list
CREATE OR REPLACE FUNCTION listPopLast(id_ varchar) RETURNS varchar AS $$
BEGIN
    WITH select_cte AS (
        SELECT listGetLast(id_)
    ), insert_cte AS (
        INSERT INTO Data (id, key, type, data, site, lts, pts, op)
        SELECT id_, (SELECT max(pos) FROM _ListUnsorted WHERE id = id_), 'l', null, siteId(), (t).lts, (t).pts, 'r'
        FROM nextTimestamp(id_) AS t
    )
    SELECT *
    FROM select_cte;
END;
$$ LANGUAGE PLPGSQL;
