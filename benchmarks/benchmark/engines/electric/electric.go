package electric

import (
	engine "benchmarks/benchmark/engines/abstract"
	dbutils "benchmarks/dbUtils"
	"benchmarks/util"
	"database/sql"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"sync"

	"gopkg.in/yaml.v3"
)

type Electric struct {
	id                     int
	VacuumFull             bool     `yaml:"vacuumFull"`
	InitialOpsPerStructure int      `yaml:"initialOpsPerStructure"`
	ItemsPerStructure      int      `yaml:"itemsPerStructure"`
	TypesToPopulate        []string `yaml:"typesToPopulate"`
	register               *Register
	set                    *Set
	map_                   *Map
	ResetServerPort        int `yaml:"resetServerPort"`
	Connection             []string
	Reset                  bool `yaml:"reset"`
}

var initialDbSize int64

func New(id int, configData []byte) *Electric {
	electric := Electric{}
	electric.id = id
	util.CheckErr(yaml.Unmarshal(configData, &electric))
	return &electric
}

func (e *Electric) Setup(connections []any) {}

func (e *Electric) Cleanup(connections []any) {
	dbs := util.CastArray[any, *sql.DB](connections)
	db := dbs[0]

	// delete
	util.Try(db.Exec("delete from electric_register"))
	util.Try(db.Exec("delete from electric_set"))
	util.Try(db.Exec("delete from electric_map"))

	// vacuum + checkpoint
	dbutils.VacuumAndCheckpointAllDBs(dbs)
}

func (e *Electric) Populate(connections []any, typesToPopulate []string, itemsPerStructure int, opsPerItem int, valueLength int) {
	dbs := util.CastArray[any, *sql.DB](connections)
	db := dbs[0]

	// tables must be created beforehand with the electric proxy, as that is the only way to execute
	// "alter table ... enable electric". we cannot use the electric proxy here as it is
	// incompatible with lib/pq.

	// delete
	if e.Reset {
		re := regexp.MustCompile(`host=(.*?) `)
		match := re.FindStringSubmatch(e.Connection[0])
		connectionStr := "http://" + match[1] + ":" + strconv.Itoa(e.ResetServerPort)
		util.Try(http.Post(connectionStr, "", nil))
	} else {
		util.Try(db.Exec("delete from electric_register"))
		util.Try(db.Exec("delete from electric_set"))
		util.Try(db.Exec("delete from electric_map"))
	}

	wg := sync.WaitGroup{}
	wg.Add(len(typesToPopulate))

	if slices.Contains(e.TypesToPopulate, "register") {
		go populateRegisters(&wg, db, e.ItemsPerStructure, valueLength)
	}
	if slices.Contains(e.TypesToPopulate, "set") {
		go populateSets(&wg, db, e.ItemsPerStructure, e.InitialOpsPerStructure)
	}
	if slices.Contains(e.TypesToPopulate, "map") {
		go populateMaps(&wg, db, e.ItemsPerStructure, e.InitialOpsPerStructure, valueLength)
	}

	wg.Wait()

	// vacuum + checkpoint
	dbutils.VacuumAndCheckpointAllDBs(dbs)

	initialDbSize = e.dbSize(db, e.VacuumFull)
}

func (e *Electric) Prepare(connection any) {
	db := connection.(*sql.DB)
	e.register = newRegister(db)
	e.set = newSet(db)
	e.map_ = newMap(db)
}

func (e *Electric) GetRegister() engine.Register {
	return e.register
}

func (e *Electric) GetCounter() engine.Counter {
	return nil
}

func (e *Electric) GetSet() engine.Set {
	return e.set
}

func (e *Electric) GetMap() engine.Map {
	return e.map_
}

func (e *Electric) GetList() engine.List {
	return nil
}

func (e *Electric) GetConfigs() map[string]string {
	return map[string]string{
		"initialOpsPerStructure": strconv.Itoa(e.InitialOpsPerStructure),
		"itemsPerStructure":      strconv.Itoa(e.ItemsPerStructure),
		"engine":                 "electric",
	}
}

func (e *Electric) dbSize(db *sql.DB, vacuumFull bool) int64 {
	if vacuumFull {
		util.Try(db.Exec("vacuum full analyze"))
	} else {
		util.Try(db.Exec("vacuum analyze"))
	}
	row := db.QueryRow(`
		select pg_total_relation_size('electric_register') +
			pg_total_relation_size('electric_set') +
			pg_total_relation_size('electric_map') +
			pg_total_relation_size('electric.shadow__public__electric_register') +
			pg_total_relation_size('electric.shadow__public__electric_set') +
			pg_total_relation_size('electric.shadow__public__electric_map') +
			pg_total_relation_size('electric.tombstone__public__electric_register') +
			pg_total_relation_size('electric.tombstone__public__electric_set') +
			pg_total_relation_size('electric.tombstone__public__electric_map') 
	`)
	var s int64
	row.Scan(&s)
	return s
}

func (e *Electric) GetMetrics(connection any) map[string]string {
	db := connection.(*sql.DB)
	return map[string]string{
		"startSize": strconv.FormatInt(initialDbSize, 10),
		"endSize":   strconv.FormatInt(e.dbSize(db, e.VacuumFull), 10),
	}
}

func (e *Electric) Finalize(connections []any) {}
