package electric

import (
	"benchmarks/util"
	"database/sql"
	"sync"
)

type Register struct {
	getStmt *sql.Stmt
	setStmt *sql.Stmt
}

func populateRegisters(wg *sync.WaitGroup, db *sql.DB, size int, valueLength int) {
	defer wg.Done()

	util.Try(db.Exec(`
		insert into electric_register
		select 'r-' || i, $2
		from (
			select generate_series(0, $1 - 1) as i
		) T
	`, size, util.RandomString(valueLength)))
}

func newRegister(db *sql.DB) *Register {
	r := &Register{}
	r.getStmt = util.Try(db.Prepare("select value from electric_register where id = $1"))
	r.setStmt = util.Try(db.Prepare("update electric_register set value = $2 where id = $1"))
	return r
}

func (r *Register) Get(id string) (string, error) {
	rs := util.Try(r.getStmt.Query(id))
	rs.Next()
	defer rs.Close()

	var value string
	rs.Scan(&value)

	return value, nil
}

func (r *Register) Set(id string, value string) error {
	// updates with the same value cause a syntax error on 'electric.shadow__public__electric_register'
	_, err := r.setStmt.Exec(id, value)
	return err
}
