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

type CubeSchema struct{}

func (e *CubeSchema) Populate(db *sql.DB, rows []row.Row) {
	nSites := len(rows[0].Lts)
	currSiteIdx := strconv.Itoa(nSites + 3)

	// cube extension
	util.Try(db.Exec("CREATE EXTENSION IF NOT EXISTS Cube"))

	// create table
	util.Try(db.Exec("DROP TABLE IF EXISTS CubeTimestamps"))
	util.Try(db.Exec("CREATE TABLE CubeTimestamps (k bigint, v bigint, lts cube)"))

	// add data
	txn := util.Try(db.Begin())
	stmt := util.Try(txn.Prepare(pq.CopyIn("cubetimestamps", "k", "v", "lts")))
	for _, r := range rows {
		llPoint := make([]string, nSites)
		for i := range llPoint {
			llPoint[i] = "0"
		}
		llPoint = append([]string{strconv.FormatInt(r.K, 10)}, llPoint...)

		urPoint := make([]string, nSites)
		for i := range urPoint {
			urPoint[i] = strconv.FormatInt(r.Lts[i], 10)
		}
		urPoint = append([]string{strconv.FormatInt(r.K, 10)}, urPoint...)

		cubeStr := "(" + strings.Join(llPoint, ",") + "),"
		cubeStr += "(" + strings.Join(urPoint, ",") + ")"

		util.Try(stmt.Exec(r.K, r.V, cubeStr))
	}
	util.CheckErr(stmt.Close())
	util.CheckErr(txn.Commit())

	// indexes
	util.Try(db.Exec("CREATE INDEX ON CubeTimestamps USING GIST (lts)"))
	util.Try(db.Exec("CREATE INDEX ON CubeTimestamps (k)"))

	// current timestamps on this site (1) for each key
	util.Try(db.Exec("DROP TABLE IF EXISTS CurrentTime"))
	util.Try(db.Exec(`
		create table CurrentTime as
		select distinct on (t1.k) t1.k, t1.lts
		from CubeTimestamps t1
		join (
			select k, max(lts->` + currSiteIdx + `)
			from CubeTimestamps
			group by k
		) t2 on t1.k = t2.k and t1.lts->` + currSiteIdx + ` = t2.max;
	`))
	util.Try(db.Exec("ALTER TABLE CurrentTime ADD PRIMARY KEY (k)"))

	// materialized max bounds for each key
	llElems := []string{}
	urElems := []string{}
	for i := 0; i < nSites+1; i++ {
		if i == 0 {
			llElems = append(llElems, "k")
		} else {
			llElems = append(llElems, "0")
		}
		urElems = append(urElems, "max(lts->"+strconv.Itoa(nSites+2+i)+")")
	}

	util.Try(db.Exec("DROP TABLE IF EXISTS MaxCubes"))
	util.Try(db.Exec(`
		create table MaxCubes as
		select k, cube(
			array[` + strings.Join(llElems, ",") + `], 
			array[` + strings.Join(urElems, ",") + `]) as max_cube 
		from CubeTimestamps 
		group by k;
	`))
	util.Try(db.Exec("ALTER TABLE MaxCubes ADD PRIMARY KEY (k)"))

	// trigger to update the max bounds table
	util.Try(db.Exec(`
		create or replace function update_max_cube() returns trigger as $$
		begin
			update MaxCubes
			set max_cube = cube_union(max_cube, new.lts)
			where k = new.k;
			return new;
		end;
		$$ language plpgsql;
	`))
	util.Try(db.Exec(`
		create trigger update_max_cube
		after insert on CubeTimestamps
		for each row
		execute function update_max_cube();
	`))
}

func (e *CubeSchema) Prepare(db *sql.DB, sites int) *SchemaStmts {
	stmts := SchemaStmts{}

	getMaxRows := []string{}
	for i := 0; i < sites; i++ {
		cubePoint := []string{fmt.Sprintf("max_cube->%d", sites+2)}

		for j := 0; j < i; j++ {
			cubePoint = append(cubePoint, "0")
		}

		cubePoint = append(cubePoint, fmt.Sprintf("max_cube->%d", sites+3+i))

		for j := i + 1; j < sites; j++ {
			cubePoint = append(cubePoint, "0")
		}

		getMaxRows = append(getMaxRows, fmt.Sprintf(`
			select maxes.k, ct.v, ct.lts
			from maxes, CubeTimestamps ct
			where lts @> cube(array[%s])
		`, strings.Join(cubePoint, ",")))
	}

	ltsElems := []string{}
	for i := 0; i < sites; i++ {
		ltsElems = append(ltsElems, fmt.Sprintf("lts->%d", sites+3+i))
	}
	cubeToArray := "array[" + strings.Join(ltsElems, ",") + "]"

	readQuery := `
		select k, v, ` + cubeToArray + `
		from (
			with potential_max as (
				with maxes as (
					select *
					from MaxCubes
					%s
				)
				` + strings.Join(getMaxRows, "union") + `
			)
			select t1.*, not t1.lts <@ t2.lts not_contained
			from potential_max t1
			join potential_max t2 on t1.k = t2.k and t1.lts <> t2.lts
		) t
		group by k, v, lts
		having bool_and(not_contained) = true;
	`

	urElems := []string{}
	for i := 0; i < sites+1; i++ {
		urElems = append(urElems, "max(lts->"+strconv.Itoa(sites+2+i)+")")
	}

	currTimeQuery := `
		select array[` + strings.Join(urElems, ",") + `]
		from CubeTimestamps 
		where k = %s
	`

	stmts.ReadKey = util.Try(db.Prepare(fmt.Sprintf(readQuery, "where k = $1::bigint")))

	stmts.ReadAll = util.Try(db.Prepare(fmt.Sprintf(readQuery, "")))

	llElems := []string{}
	urElems = []string{}
	for i := 0; i < sites+1; i++ {
		llElems = append(llElems, fmt.Sprintf("lts->%d", i+1))

		if i == 1 {
			urElems = append(urElems, fmt.Sprintf("(lts->%d) + 1", sites+2+i))
		} else {
			urElems = append(urElems, fmt.Sprintf("lts->%d", sites+2+i))
		}
	}
	llPoint := "array[" + strings.Join(llElems, ",") + "]"
	urPoint := "array[" + strings.Join(urElems, ",") + "]"
	stmts.NextTime = util.Try(db.Prepare(`
		UPDATE CurrentTime SET lts = cube(` + llPoint + "," + urPoint + `) WHERE k = $1 RETURNING lts
	`))

	stmts.CurrTime = util.Try(db.Prepare(fmt.Sprintf(currTimeQuery, "$1::bigint")))

	stmts.Write = util.Try(db.Prepare("INSERT INTO CubeTimestamps VALUES ($1, $2, $3)"))

	stmts.Size = util.Try(db.Prepare("SELECT pg_total_relation_size('CubeTimestamps')"))

	return &stmts
}

func (e *CubeSchema) Drop(db *sql.DB) {
	util.Try(db.Exec("drop table CubeTimestamps"))
	util.Try(db.Exec("drop table CurrentTime"))
	util.Try(db.Exec("drop table MaxCubes"))
}
