package schema

import (
	"benchmarks/benchmark/timestampEncoding/row"
	"benchmarks/util"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/lib/pq"
)

type RowSchema struct{}

func (e *RowSchema) Populate(db *sql.DB, rows []row.Row) {
	// create table
	util.Try(db.Exec("DROP TABLE IF EXISTS RowData"))
	util.Try(db.Exec("DROP TABLE IF EXISTS RowTimestamps"))
	util.Try(db.Exec("CREATE TABLE RowData (k bigint, v bigint, tid serial)"))
	util.Try(db.Exec("CREATE TABLE RowTimestamps (k bigint, tid bigint, site smallint, lts bigint)"))

	// add data
	txn := util.Try(db.Begin())
	stmtData := util.Try(txn.Prepare(pq.CopyIn("rowdata", "k", "v", "tid")))
	var tid int64
	for _, r := range rows {
		util.Try(stmtData.Exec(r.K, r.V, tid))
		tid += 1
	}
	util.CheckErr(stmtData.Close())

	stmtTime := util.Try(txn.Prepare(pq.CopyIn("rowtimestamps", "k", "tid", "site", "lts")))
	tid = 0
	for _, r := range rows {
		for site, v := range r.Lts {
			util.Try(stmtTime.Exec(r.K, tid, site+1, v))
		}
		tid += 1
	}
	util.CheckErr(stmtTime.Close())

	util.CheckErr(txn.Commit())

	// indexes
	util.Try(db.Exec("CREATE INDEX ON RowData (k, tid)"))
	util.Try(db.Exec("CREATE INDEX ON RowTimestamps (k, tid)"))
	util.Try(db.Exec("CREATE INDEX ON RowTimestamps (k, site, lts)"))

	// tid sequence
	util.Try(db.Exec("SELECT pg_catalog.setval(pg_get_serial_sequence('rowdata', 'tid'), (SELECT MAX(tid) FROM rowdata) + 1);"))

	// current timestamps on this site (1) for each key
	util.Try(db.Exec("DROP TABLE IF EXISTS CurrentTime"))
	util.Try(db.Exec(`
		create table CurrentTime as
		select distinct on (t1.k, t1.site) t1.k, t1.site, t1.lts
		from RowTimestamps t1
		join (
			select t1.k, t1.tid
			from RowTimestamps t1
			join (
				select k, max(lts)
				from RowTimestamps
				where site = 1
				group by k
			) t2 on t1.k = t2.k and t1.lts = t2.max
			where t1.site = 1
		) t2 on t2.k = t1.k and t2.tid = t1.tid
	`))
	util.Try(db.Exec("ALTER TABLE CurrentTime ADD PRIMARY KEY (k, site)"))
}

func (e *RowSchema) Prepare(db *sql.DB, sites int) *SchemaStmts {
	stmts := SchemaStmts{}

	readQuery := `
		select t.k, v, lts
		from (
			with potential_max as (
				select k, tid, array_agg(lts order by site) as lts
				from RowTimestamps
				where (k, tid) in (
					select k, tid
					from RowTimestamps
					where (k, site, lts) in (
						select k, site, (
							select max(lts) from RowTimestamps where k = t.k and site = t.site) as max_ts
						from (
							select k, generate_series(1, ` + strconv.Itoa(sites) + `) as site
							from (
								select k from CurrentTime
							)
						) t
					) %s
				)
				group by k, tid
			)
			select t1.*, not vclock_lte(t1.lts, t2.lts) lte
			from potential_max t1
			join potential_max t2 on t1.k = t2.k and t1.lts <> t2.lts
		) t
		join RowData on RowData.k = t.k and RowData.tid = t.tid
		group by t.k, v, lts
		having bool_and(lte) = true;
	`

	currTimeQuery := `
		select array_agg(lts order by site) 
		from (
			select site, max(lts) as lts 
			from RowTimestamps 
			where k = %s
			group by site
		) t
	`

	stmts.ReadKey = util.Try(db.Prepare(fmt.Sprintf(readQuery, "and k = $1::bigint")))

	stmts.ReadAll = util.Try(db.Prepare(fmt.Sprintf(readQuery, "")))

	stmts.NextTime = util.Try(db.Prepare("UPDATE CurrentTime SET lts = lts + 1 WHERE k = $1 and site = 1 RETURNING lts"))

	util.Try(db.Exec(`
		create or replace function insertRow(k_ bigint, v_ bigint, lts_ bigint) returns void as $$
		declare
			tid_ bigint;
		begin
			insert into rowdata values (k_, v_) returning tid into tid_;

			insert into rowtimestamps
			select k, tid_, site, lts
			from CurrentTime
			where k = k_;
		end
		$$ language plpgsql;
	`))

	stmts.CurrTime = util.Try(db.Prepare(fmt.Sprintf(currTimeQuery, "$1::bigint")))

	stmts.Write = util.Try(db.Prepare("select insertRow($1, $2, $3)"))

	stmts.Size = util.Try(db.Prepare("SELECT pg_total_relation_size('RowData') + pg_total_relation_size('RowTimestamps')"))

	return &stmts
}

func (e *RowSchema) Drop(db *sql.DB) {
	util.Try(db.Exec("drop table RowData"))
	util.Try(db.Exec("drop table RowTimestamps"))
	util.Try(db.Exec("drop table CurrentTime"))
}
