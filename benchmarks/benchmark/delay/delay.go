package delay

import (
	engine "benchmarks/benchmark/engines/abstract"
	"benchmarks/benchmark/engines/crdv"
	"benchmarks/benchmark/engines/native"
	"benchmarks/benchmark/engines/pg_crdt"
	riak_engine "benchmarks/benchmark/engines/riak"
	"benchmarks/util"
	"math/rand"
	"strconv"
	"sync"
	"time"

	zlog "github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

type Delay struct {
	id                int
	Counters          int
	EngineName        string `yaml:"engine"`
	engine            engine.Engine
	LogDelta          int `yaml:"logDelta"`
	Workers           []int
	PostEndWait       int `yaml:"postEndWait"`
	MeasurementSample int `yaml:"measurementSample"`
}

var currWorkerIndex = -1
var countersToMeasure []string
var totalCounters = 0
var wg sync.WaitGroup
var done bool = false

func New(id int, configData []byte) *Delay {
	delay := Delay{}
	util.CheckErr(yaml.Unmarshal(configData, &delay))
	delay.id = id

	if delay.EngineName == "crdv" {
		delay.engine = crdv.New(id, configData)
	} else if delay.EngineName == "native" {
		delay.engine = native.New(id, configData)
	} else if delay.EngineName == "electric" {
		panic("Engine 'electric' does not support counters")
	} else if delay.EngineName == "pg_crdt" {
		delay.engine = pg_crdt.New(id, configData)
	} else if delay.EngineName == "riak" {
		delay.engine = riak_engine.New(id, configData)
	} else {
		panic("Unknown engine: " + delay.EngineName)
	}

	return &delay
}

func (d *Delay) log(msg string) {
	zlog.Info().Str("benchmark", "delay").Int("worker", d.id).Msg(msg)
}

// Logs the value of each counter, so we can later measure the delay
func (d *Delay) LogCounterValues() {
	wg.Add(1)
	defer wg.Done()
	counter := d.engine.GetCounter()

	for !done {
		log := zlog.Info().Str("benchmark", "delay").Int("worker", d.id).Int("totalCounters", totalCounters)
		for k, v := range util.Try(counter.GetMultiple(countersToMeasure)) {
			log.Int64("_"+k, v)
		}
		log.Msg("read")
		time.Sleep(time.Duration(d.LogDelta) * time.Millisecond)
	}
}

func (d *Delay) Setup(connections []any) {
	d.engine.Setup(connections)
	currWorkerIndex++
	done = false
	wg = sync.WaitGroup{}
}

func (d *Delay) Populate(connections []any) {
	d.log("Populating")

	d.engine.Cleanup(connections)

	countersToMeasure = []string{}
	for i := 0; i < d.Counters && len(countersToMeasure) < d.MeasurementSample; i++ {
		for j := 0; j < d.Workers[currWorkerIndex] && len(countersToMeasure) < d.MeasurementSample; j++ {
			countersToMeasure = append(countersToMeasure, "c-"+strconv.Itoa(j)+"-"+strconv.Itoa(i))
		}
	}
	totalCounters = d.Workers[currWorkerIndex] * d.Counters

	d.log("Populate done")
}

func (d *Delay) randomCounter() string {
	return "c-" + strconv.Itoa(d.id) + "-" + strconv.Itoa(rand.Intn(d.Counters))
}

func (d *Delay) Prepare(connection any) map[string]func() error {
	d.engine.Prepare(connection)

	counter := d.engine.GetCounter()
	operations := map[string]func() error{}
	operations["write"] = func() error { return counter.Inc(d.randomCounter(), 1) }

	// create counters
	for i := 0; i < d.Counters; i++ {
		counter.Inc("c-"+strconv.Itoa(d.id)+"-"+strconv.Itoa(i), 0)
	}

	go d.LogCounterValues()

	return operations
}

func (d *Delay) GetConfigs() map[string]string {
	configs := d.engine.GetConfigs()
	configs["counters"] = strconv.Itoa(d.Counters)
	return configs
}

func (d *Delay) GetMetrics(connection any) map[string]string {
	return d.engine.GetMetrics(connection)
}

func (d *Delay) Finalize(connections []any) {
	// post end wait
	time.Sleep(time.Duration(d.PostEndWait) * time.Second)
	done = true
	wg.Wait()

	d.engine.Finalize(connections)
}
