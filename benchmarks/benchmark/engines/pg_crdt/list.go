package pg_crdt

import (
	"benchmarks/util"
	"database/sql"
	"errors"
	"sync"

	"github.com/automerge/automerge-go"
)

type List struct {
	dm *DataManager
}

func populateLists(wg *sync.WaitGroup, db *sql.DB, nLists int, size int, valueLength int) {
	defer wg.Done()

	doc := automerge.New()
	list := automerge.NewList()
	doc.Path("l").Set(list)

	for i := 0; i < size; i++ {
		list.Append(util.RandomString(valueLength))
	}

	util.Try(db.Exec(`
		insert into data
		select 'l-' || i, crdt.autodoc_from_bytea($2)
		from (
			select generate_series(0, $1 - 1) as i
		) T
	`, nLists, doc.Save()))
}

func newList(dm *DataManager) *List {
	return &List{dm: dm}
}

func (l *List) Get(id string) ([]string, error) {
	doc := l.dm.getDoc(id)
	values := util.Try(doc.Path("l").List().Values())
	result := []string{}

	for _, v := range values {
		result = append(result, v.GoString())
	}

	return result, nil
}

func (l *List) GetAt(id string, index int) (string, error) {
	doc := l.dm.getDoc(id)
	value := util.Try(doc.Path("l").List().Get(index))
	return value.GoString(), nil
}

func (l *List) Add(id string, index int, value string) error {
	doc := l.dm.getDoc(id)
	doc.Path("l").List().Insert(index, value)
	return l.dm.applyChange(id, doc)
}

func (l *List) Append(id string, value string) error {
	doc := l.dm.getDoc(id)
	doc.Path("l").List().Append(value)
	return l.dm.applyChange(id, doc)
}

func (l *List) Prepend(id string, value string) error {
	doc := l.dm.getDoc(id)
	doc.Path("l").List().Insert(0, value)
	return l.dm.applyChange(id, doc)
}

func (l *List) Rmv(id string, index int) error {
	doc := l.dm.getDoc(id)
	// the delete results in signal SIGSEGV: segmentation violation, even without prior deletes
	doc.Path("l").List().Delete(index)
	return l.dm.applyChange(id, doc)
}

func (l *List) Clear(id string) error {
	return errors.New("not implemented")
}
