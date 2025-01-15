package riak_engine

import (
	engine "benchmarks/benchmark/engines/abstract"
	"benchmarks/util"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/basho/riak-go-client"
	zlog "github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

type Riak struct {
	id                     int
	InitialOpsPerStructure int      `yaml:"initialOpsPerStructure"`
	ItemsPerStructure      int      `yaml:"itemsPerStructure"`
	TypesToPopulate        []string `yaml:"typesToPopulate"`
	counter                *Counter
	register               *Register
	set                    *Set
	map_                   *Map
	StorageInfoPort        int `yaml:"storageInfoPort"`
	Connection             []string
	Reset                  bool `yaml:"reset"`
	PopulateClient         int  `yaml:"populateClient"`
}

var initialDbSize int64

func New(id int, configData []byte) *Riak {
	r := Riak{PopulateClient: 1}
	r.id = id
	util.CheckErr(yaml.Unmarshal(configData, &r))
	return &r
}

func (r *Riak) Setup(connections []any) {}

func (r *Riak) Cleanup(connections []any) {}

func (r *Riak) Populate(connections []any, typesToPopulate []string, itemsPerStructure int, opsPerItem int, valueLength int) {
	beginSize := r.storageSize()
	clients := util.CastArray[any, *riak.Client](connections)

	// inserts are propagated, so we just need to insert in one database
	client := clients[r.PopulateClient]
	r.Prepare(client)

	wg := sync.WaitGroup{}
	wg.Add(len(typesToPopulate))
	if slices.Contains(r.TypesToPopulate, "counter") {
		go populateCounters(&wg, client, r.ItemsPerStructure)
	}
	if slices.Contains(r.TypesToPopulate, "register") {
		go populateRegisters(&wg, client, r.ItemsPerStructure, valueLength)
	}
	if slices.Contains(r.TypesToPopulate, "set") {
		go populateSets(&wg, client, r.ItemsPerStructure, r.InitialOpsPerStructure)
	}
	if slices.Contains(r.TypesToPopulate, "map") {
		go populateMaps(&wg, client, r.ItemsPerStructure, r.InitialOpsPerStructure, valueLength)
	}
	wg.Wait()

	// wait some time for the data to replicate
	if len(clients) > 1 {
		time.Sleep(180 * time.Second)
	}

	initialDbSize = r.storageSize() - beginSize
}

func (r *Riak) Prepare(connection any) {
	client := connection.(*riak.Client)
	r.counter = newCounter(client)
	r.register = newRegister(client)
	r.set = newSet(client)
	r.map_ = newMap(client)
}

func (r *Riak) GetRegister() engine.Register {
	return r.register
}

func (r *Riak) GetCounter() engine.Counter {
	return r.counter
}

func (r *Riak) GetSet() engine.Set {
	return r.set
}

func (r *Riak) GetMap() engine.Map {
	return r.map_
}

func (r *Riak) GetList() engine.List {
	return nil
}

func (r *Riak) GetConfigs() map[string]string {
	return map[string]string{
		"initialOpsPerStructure": strconv.Itoa(r.InitialOpsPerStructure),
		"itemsPerStructure":      strconv.Itoa(r.ItemsPerStructure),
		"engine":                 "riak",
	}
}

func (r *Riak) storageSize() int64 {
	if r.StorageInfoPort == 0 {
		return 0
	}

	connectionStr := r.Connection[0]
	connectionStr = "http://" + strings.Split(connectionStr, ":")[0] + ":" + strconv.Itoa(r.StorageInfoPort)
	resp, err := http.Post(connectionStr, "", nil)

	if err != nil {
		zlog.Error().Msg(err.Error())
		return 0
	}

	body, _ := io.ReadAll(resp.Body)
	size, _ := strconv.ParseInt(string(body), 10, 64)

	return size
}

func (r *Riak) GetMetrics(connection any) map[string]string {
	return map[string]string{
		"startSize": strconv.FormatInt(initialDbSize, 10),
		// since the storage size is measure by looking at the bitcask files, a final measurement
		// would include dead tuples not yet cleaned. as the merge cannot be forced on demand, we
		// use the initial size as the final one. eventually, the storage space after the benchmark
		// was the same as the initial one, so this is a fair assumption
		"endSize": strconv.FormatInt(initialDbSize, 10),
	}
}

func (r *Riak) Finalize(connections []any) {
	if r.Reset {
		connectionStr := r.Connection[0]
		connectionStr = "http://" + strings.Split(connectionStr, ":")[0] + ":" + strconv.Itoa(r.StorageInfoPort)
		connectionStr += "/resetAll"
		util.Try(http.Post(connectionStr, "", nil))
	}
}
