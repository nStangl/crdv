package pg_crdt

import (
	"benchmarks/util"
	"database/sql"
	"sync"

	"github.com/automerge/automerge-go"
)

type Counter struct {
	dm *DataManager
}

func populateCounters(wg *sync.WaitGroup, db *sql.DB, size int) {
	defer wg.Done()

	doc := automerge.New()
	counter := automerge.NewCounter(0)
	doc.Path("c").Set(counter)

	util.Try(db.Exec(`
		insert into data
		select 'c-' || i, crdt.autodoc_from_bytea($2)
		from (
			select generate_series(0, $1 - 1) as i
		) T
	`, size, doc.Save()))
}

func newCounter(dm *DataManager) *Counter {
	return &Counter{dm: dm}
}

func (c *Counter) Get(id string) (int64, error) {
	doc := c.dm.getDoc(id)
	return doc.Path("c").Counter().Get()
}

func (c *Counter) Inc(id string, delta int) error {
	doc := c.dm.getDoc(id)
	doc.Path("c").Counter().Inc(int64(delta))
	return c.dm.applyChange(id, doc)
}

func (c *Counter) Dec(id string, delta int) error {
	doc := c.dm.getDoc(id)
	doc.Path("c").Counter().Inc(int64(-delta))
	return c.dm.applyChange(id, doc)
}

func (c *Counter) GetAll() (map[string]int64, error) {
	result := map[string]int64{}
	for id, doc := range c.dm.getAllByPrefix("c-") {
		result[id], _ = doc.Path("c").Counter().Get()
	}
	return result, nil
}

func (c *Counter) GetMultiple(ids []string) (map[string]int64, error) {
	result := map[string]int64{}
	for id, doc := range c.dm.getMultiple(ids) {
		result[id], _ = doc.Path("c").Counter().Get()
	}
	return result, nil
}
