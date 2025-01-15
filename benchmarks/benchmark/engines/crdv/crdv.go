package crdv

import (
	engine "benchmarks/benchmark/engines/abstract"
	dbutils "benchmarks/dbUtils"
	"benchmarks/util"
	"database/sql"
	"slices"
	"strconv"
	"sync"
	"time"

	zlog "github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

type Crdv struct {
	id                          int
	VacuumFull                  bool              `yaml:"vacuumFull"`
	Modes                       map[string]string `yaml:"modes"`
	MergeParallelism            int               `yaml:"mergeParallelism"`
	MergeDelta                  float64           `yaml:"mergeDelta"`
	MergeBatchSize              int               `yaml:"mergeBatchSize"`
	TrackUnmergedRows           bool              `yaml:"trackUnmergedRows"`
	DiscardUnmergedWhenFinished bool              `yaml:"discardUnmergedWhenFinished"`
	counter                     *Counter
	register                    *Register
	set                         *Set
	map_                        *Map
	list                        *List
}

var initialDbSize int64

var trackUnmergedRowsSignal chan bool

func New(id int, configData []byte) *Crdv {
	crdv := Crdv{}
	crdv.id = id
	util.CheckErr(yaml.Unmarshal(configData, &crdv))
	return &crdv
}

func (c *Crdv) logUnmergedRows(connId int, count int64) {
	zlog.Info().Str("benchmark", "micro").Int("connId", connId).Int64("count", count).Msg("Unmerged rows")
}

// Periodically logs the number of unmerged rows in each database
func (c *Crdv) trackUnmergedRows(dbs []*sql.DB) {
	for {
		select {
		case <-trackUnmergedRowsSignal:
			trackUnmergedRowsSignal <- true
			return

		case <-time.After(time.Duration(0.1 * c.MergeDelta * float64(time.Second))):
			var wg sync.WaitGroup
			for i, db := range dbs {
				wg.Add(1)
				go func(i int, db *sql.DB) {
					defer wg.Done()
					row := db.QueryRow("select count(*) from shared")
					var s int64
					row.Scan(&s)
					c.logUnmergedRows(i, s)
				}(i, db)
			}
			wg.Wait()
		}
	}
}

func (c *Crdv) Setup(connections []any) {
	dbs := util.CastArray[any, *sql.DB](connections)
	if c.TrackUnmergedRows {
		trackUnmergedRowsSignal = make(chan bool)
		go c.trackUnmergedRows(dbs)
	}
}

func (c *Crdv) Cleanup(connections []any) {
	dbs := util.CastArray[any, *sql.DB](connections)
	// init
	dbutils.InitDb(dbs, c.MergeParallelism, c.MergeDelta, c.MergeBatchSize)
	// vacuum + checkpoint
	dbutils.VacuumAndCheckpointAllDBs(dbs)
}

func (c *Crdv) Populate(connections []any, typesToPopulate []string, itemsPerStructure int, opsPerItem int, valueLength int) {
	dbs := util.CastArray[any, *sql.DB](connections)

	// init
	dbutils.InitDb(dbs, c.MergeParallelism, c.MergeDelta, c.MergeBatchSize)

	wg := sync.WaitGroup{}
	wg.Add(len(typesToPopulate))

	if slices.Contains(typesToPopulate, "counter") {
		go populateCounters(&wg, dbs, itemsPerStructure)
	}
	if slices.Contains(typesToPopulate, "register") {
		go populateRegisters(&wg, dbs, itemsPerStructure, valueLength)
	}
	if slices.Contains(typesToPopulate, "set") {
		go populateSets(&wg, dbs, itemsPerStructure, opsPerItem)
	}
	if slices.Contains(typesToPopulate, "map") {
		go populateMaps(&wg, dbs, itemsPerStructure, opsPerItem, valueLength)
	}
	if slices.Contains(typesToPopulate, "list") {
		go populateLists(&wg, dbs, itemsPerStructure, opsPerItem, valueLength)
	}

	wg.Wait()

	// vacuum + checkpoint
	dbutils.VacuumAndCheckpointAllDBs(dbs)

	initialDbSize = dbutils.DbSize(dbs[0], c.VacuumFull)
}

func (c *Crdv) Prepare(connection any) {
	db := connection.(*sql.DB)

	// switch to the correct modes
	dbutils.SetReadMode(db, c.Modes["readMode"])
	dbutils.SetWriteMode(db, c.Modes["writeMode"])
	util.Try(db.Exec("set enable_bitmapscan = false"))

	c.counter = newCounter(db)
	c.register = newRegister(db)
	c.set = newSet(db)
	c.map_ = newMap(db)
	c.list = newList(db)
}

func (c *Crdv) GetRegister() engine.Register {
	return c.register
}

func (c *Crdv) GetCounter() engine.Counter {
	return c.counter
}

func (c *Crdv) GetSet() engine.Set {
	return c.set
}

func (c *Crdv) GetMap() engine.Map {
	return c.map_
}

func (c *Crdv) GetList() engine.List {
	return c.list
}

func (c *Crdv) GetConfigs() map[string]string {
	return map[string]string{
		"readMode":         c.Modes["readMode"],
		"writeMode":        c.Modes["writeMode"],
		"engine":           "crdv-" + c.Modes["writeMode"],
		"mergeParallelism": strconv.Itoa(c.MergeParallelism),
		"mergeDelta":       strconv.FormatFloat(c.MergeDelta, 'f', -1, 64),
		"mergeBatchSize":   strconv.Itoa(c.MergeBatchSize),
	}
}

func (c *Crdv) GetMetrics(connection any) map[string]string {
	db := connection.(*sql.DB)
	return map[string]string{
		"startSize": strconv.FormatInt(initialDbSize, 10),
		"endSize":   strconv.FormatInt(dbutils.DbSize(db, c.VacuumFull), 10),
	}
}

func (c *Crdv) Finalize(connections []any) {
	dbs := util.CastArray[any, *sql.DB](connections)

	if c.DiscardUnmergedWhenFinished {
		dbutils.DiscardUnmergedRows(dbs)
	} else {
		dbutils.ScaleMergeDaemon(dbs)
	}
	dbutils.WaitForSyncAllDBs(dbs)

	if c.TrackUnmergedRows {
		trackUnmergedRowsSignal <- true
		<-trackUnmergedRowsSignal
	}
}
