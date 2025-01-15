package native

import (
	"benchmarks/util"
	"database/sql"
	"sync"
)

type Map struct {
	getStmt      *sql.Stmt
	valueStmt    *sql.Stmt
	containsStmt *sql.Stmt
	addStmt      *sql.Stmt
	rmvStmt      *sql.Stmt
	clearStmt    *sql.Stmt
}

func populateMaps(wg *sync.WaitGroup, db *sql.DB, nMaps int, size int, valueLength int) {
	defer wg.Done()

	util.Try(db.Exec(`
		insert into native_map
		select 'm-' || (i / $1), '' || (i % $2), $4
		from (
			select generate_series(0, $3 - 1) as i
		) T
	`, size, size, nMaps*size, util.RandomString(valueLength)))
}

func newMap(db *sql.DB) *Map {
	m := &Map{}
	m.getStmt = util.Try(db.Prepare("select key, value from native_map where id = $1"))
	m.valueStmt = util.Try(db.Prepare("select value from native_map where id = $1 and key = $2"))
	m.containsStmt = util.Try(db.Prepare("select exists(select 1 from native_map where id = $1 and key = $2)"))
	m.addStmt = util.Try(db.Prepare("insert into native_map values ($1, $2, $3) on conflict (id, key) do update set value = excluded.value"))
	m.rmvStmt = util.Try(db.Prepare("delete from native_map where id = $1 and key = $2"))
	m.clearStmt = util.Try(db.Prepare("delete from native_map where id = $1"))
	return m
}

func (m *Map) Get(id string) (map[string]string, error) {
	rs := util.Try(m.getStmt.Query(id))
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
	rs := util.Try(m.valueStmt.Query(id, key))
	rs.Next()
	defer rs.Close()

	var value string
	rs.Scan(&value)

	return value, nil
}

func (m *Map) Contains(id string, key string) (bool, error) {
	rs := util.Try(m.containsStmt.Query(id, key))
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
