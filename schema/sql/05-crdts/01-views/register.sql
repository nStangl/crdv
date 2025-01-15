-- Register views

-- mvr
CREATE OR REPLACE VIEW RegisterMvr AS
    SELECT id AS id, array_agg(data ORDER BY site) AS data
    FROM Data
    WHERE type = 'r'
    GROUP BY id;

-- lww
CREATE OR REPLACE VIEW RegisterLww AS
    SELECT id AS id, data
    FROM (
        SELECT id, data,
            rank() OVER (PARTITION BY id ORDER BY pts DESC, site, data, ctid
            ) AS rank
        FROM Data
        WHERE type = 'r'
    ) t
    WHERE rank = 1;
