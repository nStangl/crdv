package native

import (
	engine "benchmarks/benchmark/engines/abstract"
	dbutils "benchmarks/dbUtils"
	"benchmarks/util"
	"database/sql"
	"slices"
	"strconv"
	"sync"

	"gopkg.in/yaml.v3"
)

type Native struct {
	id                     int
	VacuumFull             bool     `yaml:"vacuumFull"`
	InitialOpsPerStructure int      `yaml:"initialOpsPerStructure"`
	ItemsPerStructure      int      `yaml:"itemsPerStructure"`
	TypesToPopulate        []string `yaml:"typesToPopulate"`
	counter                *Counter
	register               *Register
	set                    *Set
	map_                   *Map
	list                   *List
}

var initialDbSize int64

func New(id int, configData []byte) *Native {
	native := Native{}
	native.id = id
	util.CheckErr(yaml.Unmarshal(configData, &native))
	return &native
}

func (n *Native) Setup(connections []any) {
	dbs := util.CastArray[any, *sql.DB](connections)
	db := dbs[0]
	// create the tables
	util.Try(db.Exec("create table if not exists native_counter(id varchar primary key, value bigint)"))
	util.Try(db.Exec("create table if not exists native_register(id varchar primary key, value varchar)"))
	util.Try(db.Exec("create table if not exists native_set(id varchar, elem varchar, primary key(id, elem))"))
	util.Try(db.Exec("create table if not exists native_map(id varchar, key varchar, value varchar, primary key(id, key))"))
	util.Try(db.Exec("create table if not exists native_list(id varchar, pos varchar collate \"C\", value varchar, primary key(id, pos))"))
}

func (n *Native) Cleanup(connections []any) {
	dbs := util.CastArray[any, *sql.DB](connections)
	db := dbs[0]

	// truncate
	util.Try(db.Exec("truncate native_counter"))
	util.Try(db.Exec("truncate native_register"))
	util.Try(db.Exec("truncate native_set"))
	util.Try(db.Exec("truncate native_map"))
	util.Try(db.Exec("truncate native_list"))

	// vacuum + checkpoint
	dbutils.VacuumAndCheckpointAllDBs(dbs)
}

func (n *Native) Populate(connections []any, typesToPopulate []string, itemsPerStructure int, opsPerItem int, valueLength int) {
	dbs := util.CastArray[any, *sql.DB](connections)
	db := dbs[0]

	// truncate
	util.Try(db.Exec("truncate native_counter"))
	util.Try(db.Exec("truncate native_register"))
	util.Try(db.Exec("truncate native_set"))
	util.Try(db.Exec("truncate native_map"))
	util.Try(db.Exec("truncate native_list"))

	wg := sync.WaitGroup{}
	wg.Add(len(typesToPopulate))
	if slices.Contains(n.TypesToPopulate, "counter") {
		go populateCounters(&wg, db, n.ItemsPerStructure)
	}
	if slices.Contains(n.TypesToPopulate, "register") {
		go populateRegisters(&wg, db, n.ItemsPerStructure, valueLength)
	}
	if slices.Contains(n.TypesToPopulate, "set") {
		go populateSets(&wg, db, n.ItemsPerStructure, n.InitialOpsPerStructure)
	}
	if slices.Contains(n.TypesToPopulate, "map") {
		go populateMaps(&wg, db, n.ItemsPerStructure, n.InitialOpsPerStructure, valueLength)
	}
	if slices.Contains(n.TypesToPopulate, "list") {
		go populateLists(&wg, db, n.ItemsPerStructure, n.InitialOpsPerStructure, valueLength)
	}
	wg.Wait()

	// vacuum + checkpoint
	dbutils.VacuumAndCheckpointAllDBs(dbs)

	initialDbSize = n.dbSize(db, n.VacuumFull)
}

func (n *Native) Prepare(connection any) {
	db := connection.(*sql.DB)
	n.counter = newCounter(db)
	n.register = newRegister(db)
	n.set = newSet(db)
	n.map_ = newMap(db)
	n.list = newList(db)
}

func (n *Native) GetRegister() engine.Register {
	return n.register
}

func (n *Native) GetCounter() engine.Counter {
	return n.counter
}

func (n *Native) GetSet() engine.Set {
	return n.set
}

func (n *Native) GetMap() engine.Map {
	return n.map_
}

func (n *Native) GetList() engine.List {
	return n.list
}

func (n *Native) GetConfigs() map[string]string {
	return map[string]string{
		"initialOpsPerStructure": strconv.Itoa(n.InitialOpsPerStructure),
		"itemsPerStructure":      strconv.Itoa(n.ItemsPerStructure),
		"engine":                 "native",
	}
}

func (n *Native) dbSize(db *sql.DB, vacuumFull bool) int64 {
	if vacuumFull {
		util.Try(db.Exec("vacuum full analyze"))
	} else {
		util.Try(db.Exec("vacuum analyze"))
	}
	row := db.QueryRow(`
		select pg_total_relation_size('native_counter') +
			pg_total_relation_size('native_register') +
			pg_total_relation_size('native_set') +
			pg_total_relation_size('native_map') +
			pg_total_relation_size('native_list')
	`)
	var s int64
	row.Scan(&s)
	return s
}

func (n *Native) GetMetrics(connection any) map[string]string {
	db := connection.(*sql.DB)
	return map[string]string{
		"startSize": strconv.FormatInt(initialDbSize, 10),
		"endSize":   strconv.FormatInt(n.dbSize(db, n.VacuumFull), 10),
	}
}

func (n *Native) Finalize(connections []any) {}
