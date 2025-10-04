-- View that only considers the data in the Local table
CREATE VIEW DataLocal AS
    SELECT *, ctid
    FROM Local;


-- Auxiliary view that unions the Local and Shared rows
CREATE VIEW LocalAndShared AS
    SELECT id, key, type, data, site, lts, pts, op, merged_at, ctid
    FROM Local
    UNION ALL
    SELECT id, key, type, data, site, lts, pts, op, NULL::bigint AS merged_at, ctid
    FROM Shared;


-- View that shows the most recent data from the Local and Shared tables.
-- As rows in the Shared table get merged into the Local table and then deleted inside a transaction,
-- both tables always hold different data.
-- (when new sites are added, this view will be replaced by another which considers the extra timestamps)
CREATE VIEW DataAll AS
    SELECT id, key, type, data, site, lts, pts, op, ctid
    FROM (
        WITH potential_max AS (
            WITH maxes AS not materialized (
                SELECT id, key, (
                    SELECT array [max(lts[1])]
                    FROM LocalAndShared
                    WHERE id = t_.id AND key = t_.key
                ) m
                FROM (
                    SELECT DISTINCT id, key
                    FROM LocalAndShared
                ) t_
            )
            SELECT maxes.id, maxes.key, type, data, site, lts, pts, op, ctid
            FROM LocalAndShared, maxes
            WHERE LocalAndShared.id = maxes.id AND LocalAndShared.key = maxes.key
                AND lts[1] = maxes.m[1]
        )
        SELECT t1.*, NOT vclock_lte(t1.lts, t2.lts) OR t1.lts = t2.lts lte
        FROM potential_max t1
        JOIN LocalAndShared t2
            ON t1.id = t2.id AND t1.key = t2.key
    ) t
    GROUP BY id, key, type, data, site, lts, pts, op, ctid
    HAVING bool_and(lte) = true;


-- View with the current visible data (set to read from Data_Local by default)
CREATE VIEW Data AS
    SELECT *
    FROM DataLocal;


-- Rule to redirect inserts to the Data table to the Shared table
CREATE RULE Data_insert_rule AS
    ON INSERT TO Data
    DO INSTEAD INSERT INTO Shared VALUES(new.id, new.key, new.type, new.data, new.site, new.lts, new.pts, new.op, default);


-- View with all readable rows, to aid the timestamp computation
-- (defaults to only the Local table from sync writes; with async writes it considers both Local and Shared)
CREATE VIEW AllRows AS
    SELECT *
    FROM Local;
