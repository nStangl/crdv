package pg_crdt

import (
	"benchmarks/util"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/automerge/automerge-go"
)

type Set struct {
	dm *DataManager
}

func populateSets(wg *sync.WaitGroup, db *sql.DB, nSets int, size int) {
	defer wg.Done()

	doc := automerge.New()
	set := doc.RootMap()
	for i := 0; i < size; i++ {
		set.Set(fmt.Sprintf("%d", i), "")
	}

	util.Try(db.Exec(`
		insert into data
		select 's-' || i, crdt.autodoc_from_bytea($2)
		from (
			select generate_series(0, $1 - 1) as i
		) T
	`, nSets, doc.Save()))
}

func newSet(dm *DataManager) *Set {
	return &Set{dm: dm}
}

func (s *Set) Get(id string) ([]string, error) {
	doc := s.dm.getDoc(id)
	return doc.RootMap().Keys()
}

func (s *Set) Contains(id string, value string) (bool, error) {
	doc := s.dm.getDoc(id)
	v, _ := doc.RootMap().Get(value)

	contains := new(bool)
	if v.Kind() == automerge.KindVoid {
		*contains = false
	} else {
		*contains = true
	}

	return *contains, nil
}

func (s *Set) Add(id string, value string) error {
	doc := s.dm.getDoc(id)
	doc.RootMap().Set(value, "")
	return s.dm.applyChange(id, doc)
}

func (s *Set) Rmv(id string, value string) error {
	doc := s.dm.getDoc(id)
	doc.RootMap().Delete(value)
	return s.dm.applyChange(id, doc)
}

func (s *Set) Clear(id string) error {
	return errors.New("not implemented")
}
