\echo Use "CREATE EXTENSION clocks" to load this file. \quit


CREATE FUNCTION vclock_lte(v1 vclock, v2 vclock) RETURNS boolean
    AS '$libdir/clocks', 'vclock_lte'
    LANGUAGE C STRICT;


CREATE FUNCTION vclock_max(v1 vclock, v2 vclock) RETURNS vclock
    AS '$libdir/clocks', 'vclock_max'
    LANGUAGE C STRICT;


CREATE FUNCTION next_hlc(curr hlc) RETURNS hlc
    AS '$libdir/clocks', 'next_hlc'
    LANGUAGE C STRICT;
