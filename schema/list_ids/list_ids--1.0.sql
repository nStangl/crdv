\echo Use "CREATE EXTENSION list_ids" to load this file. \quit


CREATE OR REPLACE FUNCTION generateVirtualIndexBetweenRegular(p1 varchar, p2 varchar) RETURNS varchar
    AS '$libdir/list_ids', 'generateVirtualIndexBetweenRegular'
    LANGUAGE C;

CREATE OR REPLACE FUNCTION generateVirtualIndexBetweenAppends(p1 varchar, p2 varchar) RETURNS varchar
    AS '$libdir/list_ids', 'generateVirtualIndexBetweenAppends'
    LANGUAGE C;

CREATE OR REPLACE FUNCTION generateVirtualIndexBetweenPrepends(p1 varchar, p2 varchar) RETURNS varchar
    AS '$libdir/list_ids', 'generateVirtualIndexBetweenPrepends'
    LANGUAGE C;
