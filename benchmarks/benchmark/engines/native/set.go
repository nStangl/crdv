package native

import (
	"benchmarks/util"
	"database/sql"
	"sync"
)

type Set struct {
	getStmt      *sql.Stmt
	containsStmt *sql.Stmt
	addStmt      *sql.Stmt
	rmvStmt      *sql.Stmt
	clearStmt    *sql.Stmt
}

func populateSets(wg *sync.WaitGroup, db *sql.DB, nSets int, size int) {
	defer wg.Done()

	util.Try(db.Exec(`
		insert into native_set
		select 's-' || (i / $1), '' || (i % $2)
		from (
			select generate_series(0, $3 - 1) as i
		) T
	`, size, size, nSets*size))
}

func newSet(db *sql.DB) *Set {
	s := &Set{}
	s.getStmt = util.Try(db.Prepare("select elem from native_set where id = $1"))
	s.containsStmt = util.Try(db.Prepare("select exists(select 1 from native_set where id = $1 and elem = $2)"))
	s.addStmt = util.Try(db.Prepare("insert into native_set values ($1, $2) on conflict (id, elem) do update set elem = excluded.elem"))
	s.rmvStmt = util.Try(db.Prepare("delete from native_set where id = $1 and elem = $2"))
	s.clearStmt = util.Try(db.Prepare("delete from native_set where id = $1"))
	return s
}

func (s *Set) Get(id string) ([]string, error) {
	rs := util.Try(s.getStmt.Query(id))
	defer rs.Close()

	result := []string{}
	for rs.Next() {
		var value string
		rs.Scan(&value)
		result = append(result, value)
	}

	return result, nil
}

func (s *Set) Contains(id string, value string) (bool, error) {
	rs := util.Try(s.containsStmt.Query(id, value))
	rs.Next()
	defer rs.Close()

	var contains bool
	rs.Scan(&contains)

	return contains, nil
}

func (s *Set) Add(id string, value string) error {
	_, err := s.addStmt.Exec(id, value)
	return err
}

func (s *Set) Rmv(id string, value string) error {
	_, err := s.rmvStmt.Exec(id, value)
	return err
}

func (s *Set) Clear(id string) error {
	_, err := s.clearStmt.Exec(id)
	return err
}
