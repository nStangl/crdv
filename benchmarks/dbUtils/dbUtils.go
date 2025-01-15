package dbutils

import (
	"benchmarks/util"
	"database/sql"
	"sync"
	"time"
)

// Sets the database read mode: 'local' or 'all'
func SetReadMode(db *sql.DB, mode string) {
	util.Try(db.Exec("select switch_read_mode($1)", mode))
}

// Sets the database write mode: 'sync' or 'async'
func SetWriteMode(db *sql.DB, mode string) {
	util.Try(db.Exec("select switch_write_mode($1)", mode))
}

// Resets the data in all sites
func InitDb(dbs []*sql.DB, mergeParallelism int, mergeDelta float64, mergeBatchSize int) {
	// deletes are not propagated, so we need to clean each database individually
	var wg sync.WaitGroup
	for _, db := range dbs {
		wg.Add(1)
		go func(db *sql.DB) {
			defer wg.Done()
			util.Try(db.Exec("select unschedule_merge_daemon()"))
			util.Try(db.Exec("select reset_data()"))
			SetWriteMode(db, "sync")
			SetReadMode(db, "local")
			util.Try(db.Exec("select schedule_merge_daemon($1, $2, $3)",
				mergeParallelism, mergeDelta, mergeBatchSize))
		}(db)
	}
	wg.Wait()
}

// Wait until all databases have replicated and applied the data
func WaitForSyncAllDBs(dbs []*sql.DB) {
	var wg sync.WaitGroup
	for i, db := range dbs {
		wg.Add(1)
		go func(db *sql.DB, i int) {
			defer wg.Done()
			util.Try(db.Exec("select wait_for_replication()"))
		}(db, i)
	}
	wg.Wait()
}

// Sets the parallelism of the merge daemon to a high number to speed up the merge of any remaining
// rows at the end of the benchmark
func ScaleMergeDaemon(dbs []*sql.DB) {
	var wg sync.WaitGroup
	for _, db := range dbs {
		wg.Add(1)
		go func(db *sql.DB) {
			defer wg.Done()
			util.Try(db.Exec("select schedule_merge_daemon(8, 1, 10000)"))
		}(db)
	}
	wg.Wait()
}

// Delete any unmerged rows from the Shared table, so the benchmark can finish faster
func DiscardUnmergedRows(dbs []*sql.DB) {
	var wg sync.WaitGroup
	for _, db := range dbs {
		wg.Add(1)
		go func(db *sql.DB) {
			defer wg.Done()
			nRows := 1
			for nRows > 0 {
				util.Try(db.Exec("truncate shared"))
				time.Sleep(1 * time.Second)
				row := db.QueryRow("select count(*) from shared")
				row.Scan(&nRows)
			}
		}(db)
	}
	wg.Wait()
}

// Bypasses the regular API to make copies of some structure in all sites; this is faster than
// building each one separately.
func CopyStructure(dbs []*sql.DB, id string, prefix string, numCopies int) {
	WaitForSyncAllDBs(dbs)
	var wg sync.WaitGroup
	for _, db := range dbs {
		wg.Add(1)
		go func(db *sql.DB) {
			defer wg.Done()
			util.Try(db.Exec(`
				insert into local
				select $1 || i, key, type, data, site, lts, pts, op
				from local, generate_series(1, $2) as i
				where id = $3
			`, prefix, numCopies, id))
		}(db)
	}
	wg.Wait()
}

// Vacuums and checkpoints all databases
func VacuumAndCheckpointAllDBs(dbs []*sql.DB) {
	var wg sync.WaitGroup
	for _, db := range dbs {
		wg.Add(1)
		go func(db *sql.DB) {
			defer wg.Done()
			util.Try(db.Exec("vacuum analyze"))
			util.Try(db.Exec("checkpoint"))
		}(db)
	}
	wg.Wait()
}

// Log query plans (for debug)
func EnableAutoExplain(db *sql.DB) {
	util.Try(db.Exec("LOAD 'auto_explain'"))
	util.Try(db.Exec("SET auto_explain.log_min_duration = 10;"))
	util.Try(db.Exec("SET auto_explain.log_analyze = true;"))
	util.Try(db.Exec("SET auto_explain.log_nested_statements = true;"))
}

// Returns the database size, in bytes
func DbSize(db *sql.DB, vacuumFull bool) int64 {
	if vacuumFull {
		util.Try(db.Exec("vacuum full analyze"))
	} else {
		util.Try(db.Exec("vacuum analyze"))
	}
	row := db.QueryRow(`
	select pg_total_relation_size('local') +
		pg_total_relation_size('shared')
	`)
	var s int64
	row.Scan(&s)
	return s
}
