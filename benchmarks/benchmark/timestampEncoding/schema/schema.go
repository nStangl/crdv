package schema

import (
	"benchmarks/benchmark/timestampEncoding/row"
	"database/sql"
)

type Schema interface {
	Populate(*sql.DB, []row.Row)       // Populate the database with initial data
	Prepare(*sql.DB, int) *SchemaStmts // Prepare the statements
	Drop(*sql.DB)                      // Drops the previously created tables
}

type SchemaStmts struct {
	ReadKey  *sql.Stmt // Get a row by key
	ReadAll  *sql.Stmt // Get all rows
	NextTime *sql.Stmt // Get the next timestamp to write
	Write    *sql.Stmt // Write a new row, given a key, value, and time
	CurrTime *sql.Stmt // Current time for a given key
	Size     *sql.Stmt // Return the size of the tables used, including indexes
}
