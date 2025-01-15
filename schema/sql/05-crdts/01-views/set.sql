-- Set views

-- add wins
CREATE OR REPLACE VIEW SetAw AS
    SELECT id AS id, key AS data
    FROM (
        SELECT id, key, op, 
            rank() OVER (PARTITION BY id, key ORDER BY lts, site) AS rank
        FROM Data
        WHERE type = 's'
            AND op = 'a'
    ) t 
    WHERE rank = 1;

-- add wins - entire set in the same tuple
CREATE OR REPLACE VIEW SetAwTuple AS
    SELECT id, array_agg(data) AS data
    FROM SetAw
    GROUP BY id;

-- remove wins
CREATE OR REPLACE VIEW SetRw AS
    SELECT id AS id, key AS data
    FROM (
        SELECT id, key, op, 
            rank() OVER (
                PARTITION BY id, key 
                ORDER BY array_position('{r, a}', op), lts, site
            ) AS rank
        FROM Data
        WHERE type = 's'
    ) t 
    WHERE rank = 1
        AND op != 'r';

-- remove wins - entire set in the same tuple
CREATE OR REPLACE VIEW SetRwTuple AS
    SELECT id, array_agg(data) AS data
    FROM SetRw
    GROUP BY id;

-- lww
CREATE OR REPLACE VIEW SetLww AS
    SELECT id AS id, key AS data
    FROM (
        SELECT id, key, op,
            rank() OVER (
                PARTITION BY id, key 
                ORDER BY pts DESC, site, ctid
            ) AS rank
        FROM Data
        WHERE type = 's'
    ) t
    WHERE rank = 1
        AND op != 'r';

-- lww - entire set in the same tuple
CREATE OR REPLACE VIEW SetLwwTuple AS
    SELECT id, array_agg(data) AS data
    FROM SetLww
    GROUP BY id;
