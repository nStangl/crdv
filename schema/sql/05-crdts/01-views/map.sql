-- Map views

-- add wins + mvr for concurrent adds
CREATE OR REPLACE VIEW MapAwMvr AS
    SELECT id as id, (key, array_agg(data ORDER BY site))::mEntryMvr AS data
    FROM Data
    WHERE type = 'm'
        AND op = 'a'
    GROUP BY id, key;

-- add wins + mvr for concurrent adds - entire map in the same tuple
CREATE OR REPLACE VIEW MapAwMvrTuple AS
    SELECT id, array_agg(data) AS data
    FROM MapAwMvr
    GROUP by id;

-- add wins + lww for concurrent adds
CREATE OR REPLACE VIEW MapAwLww AS
    SELECT id as id, (key, data)::mEntry AS data
    FROM (
        SELECT id, key, data, op, 
            rank() OVER (
                PARTITION BY id, key 
                ORDER BY array_position('{a, r}', op), pts DESC, site, data, ctid
            ) AS rank
        FROM Data
        WHERE type = 'm'
    ) t 
    WHERE rank = 1 
        AND op != 'r';

-- add wins + lww for concurrent adds - entire map in the same tuple
CREATE OR REPLACE VIEW MapAwLwwTuple AS
    SELECT id, array_agg(data) AS data
    FROM MapAwLww
    GROUP BY id;

-- remove wins + mvr
CREATE OR REPLACE VIEW MapRwMvr AS
    SELECT id as id, (key, array_agg(data ORDER BY site))::mEntryMvr AS data
    FROM (
        SELECT id, key, data, op, site,
            rank() over (
                PARTITION BY id, key 
                ORDER BY array_position('{r, a}', op)
            ) AS rank
        FROM Data
        WHERE type = 'm'
    ) t 
    WHERE rank = 1 
        AND op != 'r'
    GROUP BY id, key;

-- remove wins - entire map in the same tuple
CREATE OR REPLACE VIEW MapRwMvrTuple AS
    SELECT id, array_agg(data) AS data
    FROM MapRwMvr
    GROUP BY id;

-- lww
CREATE OR REPLACE VIEW MapLww AS
    SELECT id as id, (key, data)::mEntry AS data
    FROM (
        SELECT id, key, data, op,
            rank() over (
                PARTITION BY id, key 
                ORDER BY pts DESC, site, data, ctid
            ) AS rank
        FROM Data
        WHERE type = 'm'
    ) t
    WHERE rank = 1
        AND op != 'r';

-- lww - entire map in the same tuple
CREATE OR REPLACE VIEW MapLwwTuple AS
    SELECT id, array_agg(data) AS data
    FROM MapLww
    GROUP BY id;
