package crdv

import (
	dbutils "benchmarks/dbUtils"
	"benchmarks/util"
	"database/sql"
	"sync"

	"github.com/lib/pq"
)

type List struct {
	getStmt     *sql.Stmt
	getAtStmt   *sql.Stmt
	addStmt     *sql.Stmt
	appendStmt  *sql.Stmt
	prependStmt *sql.Stmt
	rmvStmt     *sql.Stmt
	clearStmt   *sql.Stmt
}

func populateLists(wg *sync.WaitGroup, dbs []*sql.DB, nLists int, size int, valueLength int) {
	defer wg.Done()

	// switch the list id generation mode to improve the populate
	util.Try(dbs[0].Exec("select switch_list_id_generation('appends')"))

	// populate the first structure using the regular API and one site
	util.Try(dbs[0].Exec(`
		select listAppend('l-0', $2)
		from (
			select generate_series(0, $1 - 1) as i
		) T
	`, size, util.RandomString(valueLength)))

	// switch the list generation mode to the default
	util.Try(dbs[0].Exec("select switch_list_id_generation('regular')"))

	// populate the remaining structures by copying first one in each site the (only the id is
	// different); this is faster than populating each one separately and replicating the data
	dbutils.CopyStructure(dbs, "l-0", "l-", nLists-1)
}

func newList(db *sql.DB) *List {
	l := &List{}
	l.getStmt = util.Try(db.Prepare("select listGet($1)"))
	l.getAtStmt = util.Try(db.Prepare("select listGetAt($1, $2)"))
	l.addStmt = util.Try(db.Prepare("select listAdd($1, $2, $3)"))
	l.appendStmt = util.Try(db.Prepare("select listAppend($1, $2)"))
	l.prependStmt = util.Try(db.Prepare("select listPrepend($1, $2)"))
	l.rmvStmt = util.Try(db.Prepare("select listRmv($1, $2)"))
	l.clearStmt = util.Try(db.Prepare("select listClear($1)"))
	return l
}

func (l *List) Get(id string) ([]string, error) {
	rs := util.Try(l.getStmt.Query(id))
	rs.Next()
	defer rs.Close()

	values := []string{}
	rs.Scan(pq.Array(&values))

	return values, nil
}

func (l *List) GetAt(id string, index int) (string, error) {
	rs := util.Try(l.getAtStmt.Query(id, index))
	rs.Next()
	defer rs.Close()

	var value string
	rs.Scan(&value)

	return value, nil
}

func (l *List) Add(id string, index int, value string) error {
	_, err := l.addStmt.Exec(id, index, value)
	return err
}

func (l *List) Append(id string, value string) error {
	_, err := l.appendStmt.Exec(id, value)
	return err
}

func (l *List) Prepend(id string, value string) error {
	_, err := l.prependStmt.Exec(id, value)
	return err
}

func (l *List) Rmv(id string, index int) error {
	_, err := l.rmvStmt.Exec(id, index)
	return err
}

func (l *List) Clear(id string) error {
	_, err := l.clearStmt.Exec(id)
	return err
}
