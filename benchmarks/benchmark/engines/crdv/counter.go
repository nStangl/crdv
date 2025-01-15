package crdv

import (
	dbutils "benchmarks/dbUtils"
	"benchmarks/util"
	"database/sql"
	"sync"

	"github.com/lib/pq"
)

type Counter struct {
	getStmt         *sql.Stmt
	incStmt         *sql.Stmt
	decStmt         *sql.Stmt
	getAllStmt      *sql.Stmt
	getMultipleStmt *sql.Stmt
}

func populateCounters(wg *sync.WaitGroup, dbs []*sql.DB, nCounters int) {
	defer wg.Done()

	// populate the first structure using the regular API and one site
	util.Try(dbs[0].Exec("select counterInc('c-0', 0)"))

	// populate the remaining structures by copying first one in each site the (only the id is
	// different); this is faster than populating each one separately and replicating the data
	dbutils.CopyStructure(dbs, "c-0", "c-", nCounters-1)
}

func newCounter(db *sql.DB) *Counter {
	c := &Counter{}
	c.getStmt = util.Try(db.Prepare("select counterGet($1)"))
	c.incStmt = util.Try(db.Prepare("select counterInc($1, $2)"))
	c.decStmt = util.Try(db.Prepare("select counterDec($1, $2)"))
	c.getAllStmt = util.Try(db.Prepare("select id, data from counter"))
	c.getMultipleStmt = util.Try(db.Prepare("select id, data from counter where id = any($1)"))
	return c
}

func (c *Counter) Get(id string) (int64, error) {
	rs := util.Try(c.getStmt.Query(id))
	rs.Next()
	defer rs.Close()

	var value int64
	rs.Scan(&value)

	return value, nil
}

func (c *Counter) Inc(id string, delta int) error {
	_, err := c.incStmt.Exec(id, delta)
	return err
}

func (c *Counter) Dec(id string, delta int) error {
	_, err := c.decStmt.Exec(id, delta)
	return err
}

func (c *Counter) GetAll() (map[string]int64, error) {
	rs := util.Try(c.getAllStmt.Query())

	result := map[string]int64{}
	for rs.Next() {
		var id string
		var value int64
		rs.Scan(&id, &value)
		result[id] = value
	}

	return result, nil
}

func (c *Counter) GetMultiple(ids []string) (map[string]int64, error) {
	rs := util.Try(c.getMultipleStmt.Query(pq.Array(ids)))

	result := map[string]int64{}
	for rs.Next() {
		var id string
		var value int64
		rs.Scan(&id, &value)
		result[id] = value
	}

	return result, nil
}
