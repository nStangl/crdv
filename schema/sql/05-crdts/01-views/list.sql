-- List views (also works for Stacks and Queues)

CREATE OR REPLACE VIEW List AS
    SELECT id as id, key as pos, data
    FROM Data
    WHERE type = 'l'
        AND op != 'r'
    ORDER BY id, key, site, pts desc;


-- unsorted list (to be used by the functions)
CREATE OR REPLACE VIEW _ListUnsorted AS
    SELECT id as id, key as pos, data
    FROM Data
    WHERE type = 'l'
        AND op != 'r';


-- entire list in the same tuple
CREATE OR REPLACE VIEW ListTuple AS
    SELECT id, array_agg(data) AS data
    FROM List
    GROUP BY id;
