package pg_crdt

import (
	"benchmarks/util"
	"database/sql"
	"sync"

	"github.com/automerge/automerge-go"
)

type Register struct {
	dm *DataManager
}

func populateRegisters(wg *sync.WaitGroup, db *sql.DB, size int, valueLength int) {
	defer wg.Done()

	doc := automerge.New()
	doc.Path("r").Set(util.RandomString(valueLength))

	util.Try(db.Exec(`
		insert into data
		select 'r-' || i, crdt.autodoc_from_bytea($2)
		from (
			select generate_series(0, $1 - 1) as i
		) T
	`, size, doc.Save()))
}

func newRegister(dm *DataManager) *Register {
	return &Register{dm: dm}
}

func (r *Register) Get(id string) (string, error) {
	doc := r.dm.getDoc(id)
	value := util.Try(doc.Path("r").Get())
	return value.GoString(), nil
}

func (r *Register) Set(id string, value string) error {
	doc := r.dm.getDoc(id)
	doc.Path("r").Set(value)
	return r.dm.applyChange(id, doc)
}
