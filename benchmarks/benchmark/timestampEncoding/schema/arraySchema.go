package schema

import (
	"benchmarks/benchmark/timestampEncoding/row"
	"benchmarks/util"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/lib/pq"
)

type ArraySchema struct{}

func (e *ArraySchema) Populate(db *sql.DB, rows []row.Row) {
	// create table
	util.Try(db.Exec("DROP TABLE IF EXISTS ArrayTimestamps"))
	util.Try(db.Exec("CREATE TABLE ArrayTimestamps (k bigint, v bigint, lts vclock)"))

	// add data
	txn := util.Try(db.Begin())
	stmt := util.Try(txn.Prepare(pq.CopyIn("arraytimestamps", "k", "v", "lts")))
	for _, r := range rows {
		util.Try(stmt.Exec(r.K, r.V, pq.Array(r.Lts)))
	}
	util.CheckErr(stmt.Close())
	util.CheckErr(txn.Commit())

	// indexes
	for i := 0; i < len(rows[0].Lts); i++ {
		util.Try(db.Exec("CREATE INDEX ON ArrayTimestamps (k, (lts[" + strconv.Itoa(i+1) + "]))"))
	}

	// current timestamps on this site (1) for each key
	util.Try(db.Exec("DROP TABLE IF EXISTS CurrentTime"))
	util.Try(db.Exec(`
		create table CurrentTime as
		select distinct on (t1.k) t1.k, t1.lts
		from ArrayTimestamps t1
		join (
			select k, max(lts[1])
			from ArrayTimestamps
			group by k
		) t2 on t1.k = t2.k and t1.lts[1] = t2.max;
	`))
	util.Try(db.Exec("ALTER TABLE CurrentTime ADD PRIMARY KEY (k)"))
}

func (e *ArraySchema) Prepare(db *sql.DB, sites int) *SchemaStmts {
	stmts := SchemaStmts{}

	var maxKs []string
	for i := 0; i < sites; i++ {
		maxKs = append(maxKs, fmt.Sprintf("max(lts[%d])", i+1))
	}

	var getMaxRows []string
	for i := 0; i < sites; i++ {
		getMaxRows = append(getMaxRows, fmt.Sprintf(`
			select ArrayTimestamps.*
			from ArrayTimestamps, maxes
			where ArrayTimestamps.k = maxes.k and lts[%d] = maxes.m[%d]
		`, i+1, i+1))
	}

	readQuery := `
		select k, v, lts
		from (
			with potential_max as (
				with maxes as (
					select k, (
						select array[` + strings.Join(maxKs, ",") + `] 
						from ArrayTimestamps 
						where k = t_.k) m
					from (
						%s
					) t_
				)
				` + strings.Join(getMaxRows, "union") + `
			)
			select t1.*, not vclock_lte(t1.lts, t2.lts) lte
			from potential_max t1
			join potential_max t2 on t1.k = t2.k and t1.lts <> t2.lts
		) t
		group by k, v, lts
		having bool_and(lte) = true;
	`

	currTimeQuery := `
		select array[` + strings.Join(maxKs, ",") + `]
		from ArrayTimestamps
		where k = %s
	`

	stmts.ReadKey = util.Try(db.Prepare(fmt.Sprintf(readQuery, "select $1::bigint as k")))

	stmts.ReadAll = util.Try(db.Prepare(fmt.Sprintf(readQuery, "select k from CurrentTime")))

	stmts.NextTime = util.Try(db.Prepare("UPDATE CurrentTime SET lts[1] = lts[1] + 1 WHERE k = $1 RETURNING lts"))

	stmts.Write = util.Try(db.Prepare("INSERT INTO ArrayTimestamps VALUES ($1, $2, $3)"))

	stmts.CurrTime = util.Try(db.Prepare(fmt.Sprintf(currTimeQuery, "$1::bigint")))

	stmts.Size = util.Try(db.Prepare("SELECT pg_total_relation_size('ArrayTimestamps')"))

	return &stmts
}

func (e *ArraySchema) Drop(db *sql.DB) {
	util.Try(db.Exec("drop table ArrayTimestamps"))
	util.Try(db.Exec("drop table CurrentTime"))
}
