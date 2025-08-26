-- Simple marker to indicate schema initialization is complete
CREATE TABLE SchemaReady (
    ready BOOLEAN DEFAULT TRUE
);

-- Insert marker record
INSERT INTO SchemaReady (ready) VALUES (TRUE);

-- Simple function to check if schema is ready
CREATE OR REPLACE FUNCTION is_schema_ready() RETURNS BOOLEAN AS $$
BEGIN
    RETURN EXISTS (SELECT 1 FROM SchemaReady WHERE ready = TRUE);
EXCEPTION
    WHEN OTHERS THEN
        RETURN FALSE;
END;
$$ LANGUAGE plpgsql;
