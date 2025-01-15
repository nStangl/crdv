package native

import (
	"benchmarks/util"
	"database/sql"
	"sync"

	"github.com/lib/pq"
)

type Counter struct {
	getStmt         *sql.Stmt
	incStmt         *sql.Stmt
	decStmt         *sql.Stmt
	newStmt         *sql.Stmt
	getAllStmt      *sql.Stmt
	getMultipleStmt *sql.Stmt
}

func populateCounters(wg *sync.WaitGroup, db *sql.DB, size int) {
	defer wg.Done()

	util.Try(db.Exec(`
		insert into native_counter
		select 'c-' || i, 0
		from (
			select generate_series(0, $1 - 1) as i
		) T
	`, size))
}

func newCounter(db *sql.DB) *Counter {
	c := &Counter{}
	c.getStmt = util.Try(db.Prepare("select value from native_counter where id = $1"))
	c.incStmt = util.Try(db.Prepare("update native_counter set value = value + $2 where id = $1"))
	c.decStmt = util.Try(db.Prepare("update native_counter set value = value - $2 where id = $1"))
	c.newStmt = util.Try(db.Prepare("insert into native_counter values ($1, $2)"))
	c.getAllStmt = util.Try(db.Prepare("select id, value from native_counter"))
	c.getMultipleStmt = util.Try(db.Prepare("select id, value from native_counter where id = any($1)"))
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
	rs := util.Try(c.getAllStmt.Query(pq.Array(ids)))

	result := map[string]int64{}
	for rs.Next() {
		var id string
		var value int64
		rs.Scan(&id, &value)
		result[id] = value
	}

	return result, nil
}
