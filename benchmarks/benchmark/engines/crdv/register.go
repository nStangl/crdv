package crdv

import (
	dbutils "benchmarks/dbUtils"
	"benchmarks/util"
	"database/sql"
	"sync"
)

type Register struct {
	getStmts map[mode]*sql.Stmt
	setStmt  *sql.Stmt
}

func populateRegisters(wg *sync.WaitGroup, dbs []*sql.DB, nRegisters int, valueLength int) {
	defer wg.Done()

	// populate the first structure using the regular API and one site
	util.Try(dbs[0].Exec("select registerSet('r-0', $1)", util.RandomString(valueLength)))

	// populate the remaining structures by copying first one in each site the (only the id is
	// different); this is faster than populating each one separately and replicating the data
	dbutils.CopyStructure(dbs, "r-0", "r-", nRegisters-1)
}

func newRegister(db *sql.DB) *Register {
	r := &Register{}
	r.getStmts = map[mode]*sql.Stmt{
		Mvr: util.Try(db.Prepare("select registerMvrGet($1)")),
		Lww: util.Try(db.Prepare("select registerLwwGet($1)")),
	}
	r.setStmt = util.Try(db.Prepare("select registerSet($1, $2)"))
	return r
}

func (r *Register) Get(id string) (string, error) {
	rs := util.Try(r.getStmts[Lww].Query(id))
	rs.Next()
	defer rs.Close()

	var value string
	rs.Scan(&value)

	return value, nil
}

func (r *Register) Set(id string, value string) error {
	_, err := r.setStmt.Exec(id, value)
	return err
}
