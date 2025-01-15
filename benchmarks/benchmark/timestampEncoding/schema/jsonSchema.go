package schema

import (
	"benchmarks/benchmark/timestampEncoding/row"
	"benchmarks/util"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/lib/pq"
)

type JsonSchema struct{}

func (e *JsonSchema) Populate(db *sql.DB, rows []row.Row) {
	// create table
	util.Try(db.Exec("DROP TABLE IF EXISTS JsonTimestamps"))
	util.Try(db.Exec("CREATE TABLE JsonTimestamps (k bigint, v bigint, lts jsonb)"))

	// add data
	txn := util.Try(db.Begin())
	stmt := util.Try(txn.Prepare(pq.CopyIn("jsontimestamps", "k", "v", "lts")))
	for _, r := range rows {
		ltsMap := map[string]int64{}
		for site, v := range r.Lts {
			ltsMap[strconv.Itoa(site+1)] = v
		}
		ltsJson, _ := json.Marshal(ltsMap)
		util.Try(stmt.Exec(r.K, r.V, string(ltsJson)))
	}
	util.CheckErr(stmt.Close())
	util.CheckErr(txn.Commit())

	// indexes
	for i := 0; i < len(rows[0].Lts); i++ {
		util.Try(db.Exec("CREATE INDEX ON JsonTimestamps (k, ((lts->'" + strconv.Itoa(i+1) + "')::bigint))"))
	}

	// current timestamps on this site (1) for each key
	util.Try(db.Exec("DROP TABLE IF EXISTS CurrentTime"))
	util.Try(db.Exec(`
		create table CurrentTime as
		select distinct on (t1.k) t1.k, t1.lts
		from JsonTimestamps t1
		join (
			select k, max((lts->'1')::bigint)
			from JsonTimestamps
			group by k
		) t2 on t1.k = t2.k and (t1.lts->'1')::bigint = t2.max;
	`))
	util.Try(db.Exec("ALTER TABLE CurrentTime ADD PRIMARY KEY (k)"))
}

func (e *JsonSchema) Prepare(db *sql.DB, sites int) *SchemaStmts {
	stmts := SchemaStmts{}

	var jsonElements []string
	for i := 0; i < sites; i++ {
		jsonElements = append(jsonElements, fmt.Sprintf("lts->'%d'", i+1))
	}

	var maxK []string
	for _, elem := range jsonElements {
		maxK = append(maxK, fmt.Sprintf("max((%s)::bigint)", elem))
	}

	var getMaxRows []string
	for i := 0; i < sites; i++ {
		getMaxRows = append(getMaxRows, fmt.Sprintf(`
			select JsonTimestamps.k, JsonTimestamps.v, array[`+strings.Join(jsonElements, ",")+`]::vclock as lts
			from JsonTimestamps, maxes
			where JsonTimestamps.k = maxes.k and (lts->'%d')::bigint = maxes.m[%d]
		`, i+1, i+1))
	}

	readQuery := `
		select k, v, lts
		from (
			with potential_max as (
				with maxes as (
					select k, (
						select array[` + strings.Join(maxK, ",") + `] 
						from JsonTimestamps 
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
		select array[` + strings.Join(maxK, ",") + `] 
		from JsonTimestamps 
		where k = %s
	`

	stmts.ReadKey = util.Try(db.Prepare(fmt.Sprintf(readQuery, "select $1::bigint as k")))

	stmts.ReadAll = util.Try(db.Prepare(fmt.Sprintf(readQuery, "select k from CurrentTime")))

	stmts.NextTime = util.Try(db.Prepare(`
		UPDATE CurrentTime 
		SET lts = lts || ('{"1": ' || ((lts->'1')::bigint + 1) || '}')::jsonb
		WHERE k = $1 
		RETURNING lts
	`))

	stmts.Write = util.Try(db.Prepare("INSERT INTO JsonTimestamps VALUES ($1, $2, $3)"))

	stmts.CurrTime = util.Try(db.Prepare(fmt.Sprintf(currTimeQuery, "$1::bigint")))

	stmts.Size = util.Try(db.Prepare("SELECT pg_total_relation_size('JsonTimestamps')"))

	return &stmts
}

func (e *JsonSchema) Drop(db *sql.DB) {
	util.Try(db.Exec("drop table JsonTimestamps"))
	util.Try(db.Exec("drop table CurrentTime"))
}
