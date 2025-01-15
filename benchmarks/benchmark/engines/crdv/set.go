package crdv

import (
	dbutils "benchmarks/dbUtils"
	"benchmarks/util"
	"database/sql"
	"sync"

	"github.com/lib/pq"
)

type Set struct {
	getStmts      map[mode]*sql.Stmt
	containsStmts map[mode]*sql.Stmt
	addStmt       *sql.Stmt
	rmvStmt       *sql.Stmt
	clearStmt     *sql.Stmt
}

func populateSets(wg *sync.WaitGroup, dbs []*sql.DB, nSets int, size int) {
	defer wg.Done()

	// populate the first structure using the regular API and one site
	util.Try(dbs[0].Exec(`
		select setAdd('s-0', '' || i)
		from (
			select generate_series(0, $1 - 1) as i
		) T
	`, size))

	// populate the remaining structures by copying first one in each site the (only the id is
	// different); this is faster than populating each one separately and replicating the data
	dbutils.CopyStructure(dbs, "s-0", "s-", nSets-1)
}

func newSet(db *sql.DB) *Set {
	s := &Set{}
	s.getStmts = map[mode]*sql.Stmt{
		Aw:  util.Try(db.Prepare("select setAwGet($1)")),
		Rw:  util.Try(db.Prepare("select setRwGet($1)")),
		Lww: util.Try(db.Prepare("select setLwwGet($1)")),
	}
	s.containsStmts = map[mode]*sql.Stmt{
		Aw:  util.Try(db.Prepare("select setAwContains($1, $2)")),
		Rw:  util.Try(db.Prepare("select setAwContains($1, $2)")),
		Lww: util.Try(db.Prepare("select setLwwContains($1, $2)")),
	}
	s.addStmt = util.Try(db.Prepare("select setAdd($1, $2)"))
	s.rmvStmt = util.Try(db.Prepare("select setRmv($1, $2)"))
	s.clearStmt = util.Try(db.Prepare("select setClear($1)"))
	return s
}

func (s *Set) Get(id string) ([]string, error) {
	rs := util.Try(s.getStmts[Lww].Query(id))
	rs.Next()
	defer rs.Close()

	values := []string{}
	rs.Scan(pq.Array(&values))

	return values, nil
}

func (s *Set) Contains(id string, value string) (bool, error) {
	rs := util.Try(s.containsStmts[Lww].Query(id, value))
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
