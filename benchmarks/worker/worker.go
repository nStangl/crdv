package worker

import (
	"benchmarks/benchmark"
	"benchmarks/util"
	"log"
	"math/rand"
	"sync"
	"time"

	zlog "github.com/rs/zerolog/log"
)

type Worker struct {
	id              int
	connection      any
	duration        int
	transactions    int
	warmup          int
	cooldown        int
	benchmark       benchmark.Benchmark
	operations      []Operation
	totalWeight     int
	operationsToLog chan *OperationLogEntry
	operationLogWg  *sync.WaitGroup
}

type Operation struct {
	Name   string
	Weight int
}

type OperationLogEntry struct {
	op  string
	rt  float64
	err error
	t   time.Time
}

type Metric struct {
	Rts           []float64 // list of the response times (seconds) of committed transactions
	TotalRt       float64   // sum of the response time of all committed transactions
	CompleteCount int       // number of committed operations
	AbortCount    int       // number of aborted operations
}

type BenchmarkResults struct {
	RealDuration float64
	Operations   map[string]*Metric // metric name -> Metric
}

func NewWorker(id int, duration int, transactions int, warmup int, cooldown int, connection any, operations []Operation, benchmark benchmark.Benchmark) *Worker {
	worker := new(Worker)
	worker.id = id
	worker.duration = duration
	worker.transactions = transactions
	worker.warmup = warmup
	worker.cooldown = cooldown
	worker.connection = connection
	worker.benchmark = benchmark
	worker.operations = operations
	worker.operationsToLog = make(chan *OperationLogEntry, 1024)
	worker.operationLogWg = &sync.WaitGroup{}

	for _, o := range worker.operations {
		worker.totalWeight += o.Weight
	}

	return worker
}

func (w *Worker) log(msg string) {
	zlog.Info().Int("worker", w.id).Msg(msg)
}

func (w *Worker) logOperationsWorker() {
	for operation := range w.operationsToLog {
		if operation == nil {
			break
		}

		var msg string
		if operation.err == nil {
			msg = "completed"
		} else {
			msg = "aborted"
		}

		zlog.Debug().Int("worker", w.id).Str("operation", operation.op).
			Float64("rt", operation.rt).Time("real_time", operation.t).Msg(msg)
	}

	w.operationLogWg.Done()
}

func (w *Worker) getRandomOperation() *string {
	r := rand.Intn(w.totalWeight)
	curr := 0

	for _, o := range w.operations {
		if r < o.Weight+curr {
			return &o.Name
		}
		curr += o.Weight
	}

	panic("Random operation bigger than the cumulative sum.")
}

func (w *Worker) Run(c chan *BenchmarkResults) {
	w.operationLogWg.Add(1)
	go w.logOperationsWorker()

	w.log("Preparing")
	functions := w.benchmark.Prepare(w.connection)
	results := BenchmarkResults{}
	results.Operations = map[string]*Metric{}
	for _, o := range w.operations {
		results.Operations[o.Name] = &Metric{}
	}

	w.log("Running")
	completedTransactions := 0
	start := util.EpochSeconds()
	elapsed := 0.

	for (w.duration > 0 && elapsed < float64(w.duration)) || (w.duration <= 0 && completedTransactions < w.transactions) {
		op := w.getRandomOperation()
		function, ok := functions[*op]
		if !ok {
			log.Fatalf("Function '%s' not found.\n", *op)
		}

		txStart := util.EpochSeconds()
		err := function()
		rt := util.EpochSeconds() - txStart
		w.operationsToLog <- &OperationLogEntry{*op, rt, err, time.Now()}

		if w.duration <= 0 || (elapsed > float64(w.warmup) && elapsed < float64(w.duration-w.cooldown)) {
			metric := results.Operations[*op]

			if err == nil {
				metric.CompleteCount++
				metric.Rts = append(metric.Rts, rt)
				metric.TotalRt += rt
				completedTransactions++
			} else {
				metric.AbortCount++
			}
		}

		elapsed = util.EpochSeconds() - start
	}

	results.RealDuration = util.EpochSeconds() - start
	if w.duration > 0 {
		results.RealDuration -= float64(w.warmup) + float64(w.cooldown)
	}

	w.operationsToLog <- nil
	w.operationLogWg.Wait()
	w.log("Done")

	c <- &results
}

// Returns the benchmark-specific configurations
func (w *Worker) GetConfigs() map[string]string {
	return w.benchmark.GetConfigs()
}

// Returns the benchmark-specific metrics
func (w *Worker) GetMetrics() map[string]string {
	return w.benchmark.GetMetrics(w.connection)
}
