-- Counter views

--- add multiple values
CREATE OR REPLACE VIEW Counter AS
    SELECT id AS id, sum(data::bigint) AS data
    FROM Data
    WHERE type = 'c'
    GROUP BY id;
