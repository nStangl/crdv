package pg_crdt

import (
	"benchmarks/util"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/automerge/automerge-go"
)

type Map struct {
	dm *DataManager
}

func populateMaps(wg *sync.WaitGroup, db *sql.DB, nMaps int, size int, valueLength int) {
	defer wg.Done()

	doc := automerge.New()
	m := doc.RootMap()
	for i := 0; i < size; i++ {
		m.Set(fmt.Sprintf("%d", i), util.RandomString(valueLength))
	}

	util.Try(db.Exec(`
		insert into data
		select 'm-' || i, crdt.autodoc_from_bytea($2)
		from (
			select generate_series(0, $1 - 1) as i
		) T
	`, nMaps, doc.Save()))
}

func newMap(dm *DataManager) *Map {
	return &Map{dm: dm}
}

func (m *Map) Get(id string) (map[string]string, error) {
	doc := m.dm.getDoc(id)
	rootMap := doc.RootMap()
	keys, _ := rootMap.Keys()
	result := map[string]string{}

	for _, k := range keys {
		value := util.Try(rootMap.Get(k))
		result[k] = value.GoString()
	}

	return result, nil
}

func (m *Map) Value(id string, key string) (string, error) {
	doc := m.dm.getDoc(id)
	value := util.Try(doc.RootMap().Get(key))
	return value.GoString(), nil
}

func (m *Map) Contains(id string, key string) (bool, error) {
	doc := m.dm.getDoc(id)
	v, _ := doc.RootMap().Get(key)

	contains := new(bool)
	if v.Kind() == automerge.KindVoid {
		*contains = false
	} else {
		*contains = true
	}

	return *contains, nil
}

func (m *Map) Add(id string, key string, value string) error {
	doc := m.dm.getDoc(id)
	doc.RootMap().Set(key, value)
	return m.dm.applyChange(id, doc)
}

func (m *Map) Rmv(id string, key string) error {
	doc := m.dm.getDoc(id)
	doc.RootMap().Delete(key)
	return m.dm.applyChange(id, doc)
}

func (m *Map) Clear(id string) error {
	return errors.New("not implemented")
}
