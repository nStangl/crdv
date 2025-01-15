-- Current logical time of the hybrid logical clock of this site
CREATE UNLOGGED SEQUENCE IF NOT EXISTS SiteHybridLogicalTime;

-- Last wall clock time seen
CREATE UNLOGGED SEQUENCE WallClockSeq;
