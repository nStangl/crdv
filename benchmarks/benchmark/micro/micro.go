package micro

import (
	engine "benchmarks/benchmark/engines/abstract"
	"benchmarks/benchmark/engines/crdv"
	"benchmarks/benchmark/engines/electric"
	"benchmarks/benchmark/engines/native"
	"benchmarks/benchmark/engines/pg_crdt"
	riak_engine "benchmarks/benchmark/engines/riak"
	"benchmarks/util"
	"math/rand"
	"strconv"

	zlog "github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

type Micro struct {
	id                     int
	InitialOpsPerStructure int      `yaml:"initialOpsPerStructure"`
	ItemsPerStructure      int      `yaml:"itemsPerStructure"`
	TypesToPopulate        []string `yaml:"typesToPopulate"`
	EngineName             string   `yaml:"engine"`
	engine                 engine.Engine
	ValueLength            int `yaml:"valueLength"`
}

func New(id int, configData []byte) *Micro {
	micro := Micro{}
	util.CheckErr(yaml.Unmarshal(configData, &micro))
	micro.id = id

	if micro.EngineName == "crdv" {
		micro.engine = crdv.New(id, configData)
	} else if micro.EngineName == "native" {
		micro.engine = native.New(id, configData)
	} else if micro.EngineName == "electric" {
		micro.engine = electric.New(id, configData)
	} else if micro.EngineName == "pg_crdt" {
		micro.engine = pg_crdt.New(id, configData)
	} else if micro.EngineName == "riak" {
		micro.engine = riak_engine.New(id, configData)
	} else {
		panic("Unknown engine: " + micro.EngineName)
	}

	return &micro
}

func (m *Micro) log(msg string) {
	zlog.Info().Str("benchmark", "micro").Int("id", m.id).Msg(msg)
}

func (m *Micro) Setup(connections []any) {
	m.engine.Setup(connections)
}

func (m *Micro) Populate(connections []any) {
	m.log("Populating")
	m.engine.Populate(connections, m.TypesToPopulate, m.ItemsPerStructure, m.InitialOpsPerStructure, m.ValueLength)
	m.log("Populate done")
}

func (m *Micro) randomId(prefix string) string {
	return prefix + "-" + strconv.Itoa(rand.Intn(m.ItemsPerStructure))
}

func (m *Micro) randomValue() string {
	return util.RandomString(m.ValueLength)
}

func (m *Micro) randomKey() string {
	return strconv.Itoa(rand.Intn(m.InitialOpsPerStructure))
}

func (m *Micro) randomIndex() int {
	return rand.Intn(m.InitialOpsPerStructure)
}

func (m *Micro) Prepare(connection any) map[string]func() error {
	m.engine.Prepare(connection)

	counter := m.engine.GetCounter()
	register := m.engine.GetRegister()
	set := m.engine.GetSet()
	map_ := m.engine.GetMap()
	list := m.engine.GetList()

	operations := map[string]func() error{}

	if counter != nil {
		operations["counterGet"] = func() error { return util.Second(counter.Get(m.randomId("c"))) }
		operations["counterInc"] = func() error { return counter.Inc(m.randomId("c"), rand.Intn(10)+1) }
		operations["counterDec"] = func() error { return counter.Dec(m.randomId("c"), rand.Intn(10)+1) }
	}

	if register != nil {
		operations["registerGet"] = func() error { return util.Second(register.Get(m.randomId("r"))) }
		operations["registerSet"] = func() error { return register.Set(m.randomId("r"), m.randomValue()) }
	}

	if set != nil {
		operations["setGet"] = func() error { return util.Second(set.Get(m.randomId("s"))) }
		operations["setContains"] = func() error { return util.Second(set.Contains(m.randomId("s"), m.randomKey())) }
		operations["setAdd"] = func() error { return set.Add(m.randomId("s"), m.randomKey()) }
		operations["setRmv"] = func() error { return set.Rmv(m.randomId("s"), m.randomKey()) }
		operations["setClear"] = func() error { return set.Clear(m.randomId("s")) }
	}

	if map_ != nil {
		operations["mapGet"] = func() error { return util.Second(map_.Get(m.randomId("m"))) }
		operations["mapValue"] = func() error { return util.Second(map_.Value(m.randomId("m"), m.randomKey())) }
		operations["mapContains"] = func() error { return util.Second(map_.Contains(m.randomId("m"), m.randomKey())) }
		operations["mapAdd"] = func() error { return map_.Add(m.randomId("m"), m.randomKey(), m.randomValue()) }
		operations["mapRmv"] = func() error { return map_.Rmv(m.randomId("m"), m.randomKey()) }
		operations["mapClear"] = func() error { return map_.Clear(m.randomId("m")) }
	}

	if list != nil {
		operations["listGet"] = func() error { return util.Second(list.Get(m.randomId("l"))) }
		operations["listGetAt"] = func() error { return util.Second(list.GetAt(m.randomId("l"), m.randomIndex())) }
		operations["listAdd"] = func() error { return list.Add(m.randomId("l"), m.randomIndex(), m.randomValue()) }
		operations["listAppend"] = func() error { return list.Append(m.randomId("l"), m.randomValue()) }
		operations["listPrepend"] = func() error { return list.Prepend(m.randomId("l"), m.randomValue()) }
		operations["listRmv"] = func() error { return list.Rmv(m.randomId("l"), m.randomIndex()) }
		operations["listClear"] = func() error { return list.Clear(m.randomId("l")) }
	}

	return operations
}

func (m *Micro) GetConfigs() map[string]string {
	configs := m.engine.GetConfigs()
	configs["initialOpsPerStructure"] = strconv.Itoa(m.InitialOpsPerStructure)
	configs["itemsPerStructure"] = strconv.Itoa(m.ItemsPerStructure)
	return configs
}

func (m *Micro) GetMetrics(connection any) map[string]string {
	return m.engine.GetMetrics(connection)
}

func (m *Micro) Finalize(connections []any) {
	m.engine.Finalize(connections)
}
