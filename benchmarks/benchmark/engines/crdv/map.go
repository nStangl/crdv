package crdv

import (
	dbutils "benchmarks/dbUtils"
	"benchmarks/util"
	"database/sql"
	"sync"
)

type Map struct {
	getStmts      map[mode]*sql.Stmt
	valueStmts    map[mode]*sql.Stmt
	containsStmts map[mode]*sql.Stmt
	addStmt       *sql.Stmt
	rmvStmt       *sql.Stmt
	clearStmt     *sql.Stmt
}

func populateMaps(wg *sync.WaitGroup, dbs []*sql.DB, nMaps int, size int, valueLength int) {
	defer wg.Done()

	// populate the first structure using the regular API and one site
	util.Try(dbs[0].Exec(`
		select mapAdd('m-0', '' || i, $2)
		from (
			select generate_series(0, $1 - 1) as i
		) T
	`, size, util.RandomString(valueLength)))

	// populate the remaining structures by copying first one in each site the (only the id is
	// different); this is faster than populating each one separately and replicating the data
	dbutils.CopyStructure(dbs, "m-0", "m-", nMaps-1)
}

func newMap(db *sql.DB) *Map {
	m := &Map{}
	m.getStmts = map[mode]*sql.Stmt{
		AwMvr: util.Try(db.Prepare("select (data).key, (data).value from MapAwMvr where id = $1")),
		AwLww: util.Try(db.Prepare("select (data).key, (data).value from MapAwLww where id = $1")),
		RwMvr: util.Try(db.Prepare("select (data).key, (data).value from MapRwMvr where id = $1")),
		Lww:   util.Try(db.Prepare("select (data).key, (data).value from MapLww where id = $1")),
	}
	m.valueStmts = map[mode]*sql.Stmt{
		AwMvr: util.Try(db.Prepare("select mapAwMvrValue($1, $2)")),
		AwLww: util.Try(db.Prepare("select mapAwLwwValue($1, $2)")),
		RwMvr: util.Try(db.Prepare("select mapRwMvrValue($1, $2)")),
		Lww:   util.Try(db.Prepare("select mapLwwValue($1, $2)")),
	}
	m.containsStmts = map[mode]*sql.Stmt{
		AwMvr: util.Try(db.Prepare("select mapAwMvrContains($1, $2)")),
		AwLww: util.Try(db.Prepare("select mapAwLwwContains($1, $2)")),
		RwMvr: util.Try(db.Prepare("select mapRwMvrContains($1, $2)")),
		Lww:   util.Try(db.Prepare("select mapLwwContains($1, $2)")),
	}
	m.addStmt = util.Try(db.Prepare("select mapAdd($1, $2, $3)"))
	m.rmvStmt = util.Try(db.Prepare("select mapRmv($1, $2)"))
	m.clearStmt = util.Try(db.Prepare("select mapClear($1)"))
	return m
}

func (m *Map) Get(id string) (map[string]string, error) {
	rs := util.Try(m.getStmts[Lww].Query(id))
	defer rs.Close()

	result := map[string]string{}
	for rs.Next() {
		var key, value string
		rs.Scan(&key, &value)
		result[key] = value
	}

	return result, nil
}

func (m *Map) Value(id string, key string) (string, error) {
	rs := util.Try(m.valueStmts[Lww].Query(id, key))
	rs.Next()
	defer rs.Close()

	var value string
	rs.Scan(&value)

	return value, nil
}

func (m *Map) Contains(id string, key string) (bool, error) {
	rs := util.Try(m.containsStmts[Lww].Query(id, key))
	rs.Next()
	defer rs.Close()

	var value bool
	rs.Scan(&value)

	return value, nil
}

func (m *Map) Add(id string, key string, value string) error {
	_, err := m.addStmt.Exec(id, key, value)
	return err
}

func (m *Map) Rmv(id string, key string) error {
	_, err := m.rmvStmt.Exec(id, key)
	return err
}

func (m *Map) Clear(id string) error {
	_, err := m.clearStmt.Exec(id)
	return err
}
