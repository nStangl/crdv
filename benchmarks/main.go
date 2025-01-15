package main

import (
	"benchmarks/benchmark"
	"benchmarks/benchmark/delay"
	"benchmarks/benchmark/micro"
	"benchmarks/benchmark/nested"
	timestampencoding "benchmarks/benchmark/timestampEncoding"
	"benchmarks/util"
	"benchmarks/worker"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/basho/riak-go-client"
	_ "github.com/lib/pq"
	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

type BenchmarkArgs struct {
	Connection   []string
	Time         int
	Transactions int
	Warmup       int
	Cooldown     int
	Runs         int
	NoReload     bool `yaml:"noReload"`
	Workers      []int
	Isolation    string
	Benchmark    string
	Engine       string
	FileData     []byte // config file contents
	Operations   []worker.Operation
}

type ProcessedResult struct {
	name  string
	rt    float64
	ct    float64
	tps   float64
	ar    float64
	rtP95 float64
}

// Prepare zerolog
func setupLogging(disableLog bool, level string) {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	var zlevel zerolog.Level
	if disableLog {
		zlevel = zerolog.Disabled
	} else if level == "info" {
		zlevel = zerolog.InfoLevel
	} else {
		zlevel = zerolog.DebugLevel
	}
	zerolog.SetGlobalLevel(zlevel)
}

// Returns a BenchmarkArgs struct with the information in the configFile.
func buildArgs(configFile string) *BenchmarkArgs {
	if configFile == "" {
		log.Fatal("Missing config file.")
	}

	data, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatal(err)
	}

	args := BenchmarkArgs{}
	err = yaml.Unmarshal(data, &args)
	if err != nil {
		log.Fatal(err)
	}
	args.FileData = data

	return &args
}

// Returns a benchmark factory based on the benchmarkType. configData is a binary representation of
// the configuration file, so each benchmark can deserialize its respective parameters.
func getBenchmarkFactory(benchmarkType string, configData []byte) func(int) benchmark.Benchmark {
	var factory func(int) benchmark.Benchmark

	switch benchmarkType {
	case "timestampEncoding":
		factory = func(id int) benchmark.Benchmark { return timestampencoding.New(id, configData) }
	case "micro":
		factory = func(id int) benchmark.Benchmark { return micro.New(id, configData) }
	case "delay":
		factory = func(id int) benchmark.Benchmark { return delay.New(id, configData) }
	case "nested":
		factory = func(id int) benchmark.Benchmark { return nested.New(id, configData) }
	default:
		log.Fatalf("Benchmark '%s' not found.\n", benchmarkType)
	}

	return factory
}

// Create the sql.DB or riak.Client connections
func createConnections(args *BenchmarkArgs) []any {
	connections := []any{}

	if strings.Contains(args.Engine, "riak") {
		for _, v := range args.Connection {
			clientOptions := &riak.NewClientOptions{
				RemoteAddresses: []string{v},
			}
			client := util.Try(riak.NewClient(clientOptions))
			connections = append(connections, client)
		}
	} else {
		for _, v := range args.Connection {
			db := util.Try(sql.Open("postgres", v))
			db.SetMaxOpenConns(100)
			// the number of idle connections should be the same as the number of actual connections.
			// otherwise, if the number of workers is smaller than the number of open connections,
			// the system will enter in a trashing state where connections are constantly being
			// created and destroyed, because at least one connection will end up being considered
			// idle. this causes a significant performance decrease, as the majority of the time
			// will be spent at the "database/sql.(*Stmt).connStmt" function.
			db.SetMaxIdleConns(100)
			util.CheckErr(db.Ping())
			util.Try(db.Exec("alter system set default_transaction_isolation = '" + args.Isolation + "'"))
			util.Try(db.Exec("select pg_reload_conf()"))
			connections = append(connections, db)
		}
	}

	return connections
}

// Change the transactional isolation to the default value (sql) and close the connections
func closeConnections(args *BenchmarkArgs, connections []any) {
	if strings.Contains(args.Engine, "riak") {
		connections_ := util.CastArray[any, *riak.Client](connections)
		for _, client := range connections_ {
			client.Stop()
		}
	} else {
		dbs := util.CastArray[any, *sql.DB](connections)
		for _, db := range dbs {
			util.Try(db.Exec("alter system set default_transaction_isolation = default"))
			util.Try(db.Exec("select pg_reload_conf()"))
			db.Close()
		}
	}
}

// Create nWorkers with the respective arguments, connections, and benchmark.
func createWorkers(nWorkers int, args *BenchmarkArgs, connections []any, benchmarkFactory func(int) benchmark.Benchmark) []*worker.Worker {
	connections_ := reflect.ValueOf(connections)
	workers := []*worker.Worker{}

	for i := 0; i < nWorkers; i++ {
		w := worker.NewWorker(i, args.Time, args.Transactions/nWorkers, args.Warmup, args.Cooldown,
			connections_.Index(i%connections_.Len()).Interface(), args.Operations, benchmarkFactory(i))
		workers = append(workers, w)
	}

	return workers
}

// Computes the average value of a list of results
func avgMetric(results []ProcessedResult, metric string) float64 {
	var total float64

	for _, r := range results {
		switch metric {
		case "rt":
			total += r.rt
		case "ct":
			total += r.ct
		case "tps":
			total += r.tps
		case "ar":
			total += r.ar
		case "rtP95":
			total += r.rtP95
		}
	}

	return total / float64(len(results))
}

// Prints a summary of the results.
// Both the throughput (tps) and response time (rt) consider only the completed operations
func aggregateResults(allResults [][]*worker.BenchmarkResults) map[string]ProcessedResult {
	// metric -> values of each run
	processedResults := map[string][]ProcessedResult{}

	// process the results of all runs
	for _, results := range allResults {
		rts := map[string][]float64{}
		totalRts := map[string]float64{}
		completeCounts := map[string]int{}
		abortCounts := map[string]int{}
		tps := map[string]float64{}
		totalCompleted := 0
		totalAborted := 0
		totalRt := 0.
		totalTps := 0.
		allRts := []float64{}

		// combine the results of all workers
		for _, result := range results {
			for operation, value := range result.Operations {
				rts[operation] = append(rts[operation], value.Rts...)
				totalRts[operation] += value.TotalRt
				completeCounts[operation] += value.CompleteCount
				abortCounts[operation] += value.AbortCount
				tps[operation] += float64(value.CompleteCount) / result.RealDuration
				totalCompleted += value.CompleteCount
				totalAborted += value.AbortCount
				totalRt += value.TotalRt
				totalTps += float64(value.CompleteCount) / result.RealDuration
				allRts = append(allRts, value.Rts...)
			}
		}

		// add the run averages to all averages
		for k := range rts {
			processedResults[k] = append(processedResults[k], ProcessedResult{
				name:  k,
				rt:    totalRts[k] / float64(completeCounts[k]),
				ct:    float64(completeCounts[k]),
				tps:   float64(tps[k]),
				ar:    float64(abortCounts[k]) / float64(abortCounts[k]+completeCounts[k]),
				rtP95: util.Percentile(rts[k], 95),
			})
		}

		// average of all operations
		processedResults["total"] = append(processedResults["total"], ProcessedResult{
			name:  "total",
			rt:    totalRt / float64(totalCompleted),
			ct:    float64(totalCompleted),
			tps:   totalTps,
			ar:    float64(totalAborted) / (float64(totalAborted + totalCompleted)),
			rtP95: util.Percentile(allRts, 95),
		})
	}

	aggregated := map[string]ProcessedResult{}
	for k, v := range processedResults {
		aggregated[k] = ProcessedResult{
			rt:    avgMetric(v, "rt"),
			tps:   avgMetric(v, "tps"),
			ar:    avgMetric(v, "ar"),
			ct:    avgMetric(v, "ct"),
			rtP95: avgMetric(v, "rtP95"),
		}
	}

	return aggregated
}

func shortenIsolation(isolation string) string {
	if isolation == "READ COMMITTED" {
		return "RC"
	} else if isolation == "REPEATABLE READ" {
		return "RR"
	} else {
		return "n/a"
	}
}

func printSummary(aggregated map[string]ProcessedResult,
	args *BenchmarkArgs,
	nWorkers int,
	benchmarkConfigs map[string]string,
	benchmarkMetrics map[string]string,
	firstLine bool,
) {
	sortedConfigs := []string{}
	for k := range benchmarkConfigs {
		sortedConfigs = append(sortedConfigs, k)
	}
	sort.Strings(sortedConfigs)

	sortedMetrics := []string{}
	for k := range benchmarkMetrics {
		sortedMetrics = append(sortedMetrics, k)
	}
	sort.Strings(sortedMetrics)

	// CSV header
	if firstLine {
		if len(sortedMetrics) > 0 {
			fmt.Println("Csv:benchmark,time,runs,noReload,workers,isolation,sites," +
				strings.Join(sortedConfigs, ",") + "," + strings.Join(sortedMetrics, ",") + ",rt,tps,ct,ar,rtP95")
		} else {
			fmt.Println("Csv:benchmark,time,runs,noReload,workers,isolation,sites," +
				strings.Join(sortedConfigs, ",") + ",rt,tps,ct,ar,rtP95")
		}
		fmt.Println("CsvOps:benchmark,time,runs,noReload,workers,isolation,sites," +
			strings.Join(sortedConfigs, ",") + ",operation,rt,tps,ct,ar,rtP95")
	}

	isolation := shortenIsolation(args.Isolation)
	// string for the benchmark specific metrics ("Csv:" prefix)
	csv := fmt.Sprintf("Csv:%s,%d,%d,%t,%d,%s,%d",
		args.Benchmark, args.Time, args.Runs, args.NoReload, nWorkers, isolation, len(args.Connection))
	// string for the operations ("CsvOps:" prefix)
	csvOps := fmt.Sprintf("CsvOps:%s,%d,%d,%t,%d,%s,%d",
		args.Benchmark, args.Time, args.Runs, args.NoReload, nWorkers, isolation, len(args.Connection))
	// string with metrics in a key-value format to ease reading
	kv := fmt.Sprintf("benchmark: %s\ntime: %d\nruns: %d\nnoReload: %t\nworkers: %d\nisolation: %s\nsites: %d",
		args.Benchmark, args.Time, args.Runs, args.NoReload, nWorkers, isolation, len(args.Connection))

	// write benchmark-specific configs
	for _, config := range sortedConfigs {
		csv += fmt.Sprintf(",%s", benchmarkConfigs[config])
		csvOps += fmt.Sprintf(",%s", benchmarkConfigs[config])
		kv += fmt.Sprintf("\n%s: %s", config, benchmarkConfigs[config])
	}

	// write benchmark-specific metrics
	for _, metric := range sortedMetrics {
		csv += fmt.Sprintf(",%s", benchmarkMetrics[metric])
		kv += fmt.Sprintf("\n%s: %s", metric, benchmarkMetrics[metric])
	}

	// write the results of each operation
	for metric, result := range aggregated {
		if metric == "total" {
			kv += fmt.Sprintf("\nrt: %.6f", result.rt)
			kv += fmt.Sprintf("\ntps: %.6f", result.tps)
			kv += fmt.Sprintf("\nct: %.6f", result.ct)
			kv += fmt.Sprintf("\nar: %.6f", result.ar)
			kv += fmt.Sprintf("\nrtP95: %.6f", result.rtP95)
			csv += fmt.Sprintf(",%.6f,%.3f,%.0f,%.6f,%.6f", result.rt, result.tps, result.ct, result.ar, result.rtP95)
		}
		fmt.Println(csvOps + fmt.Sprintf(",%s,%.6f,%.3f,%.0f,%.6f,%.6f", metric, result.rt, result.tps, result.ct, result.ar, result.rtP95))
	}

	fmt.Println(csv)
	fmt.Println(kv)
}

func main() {
	disableLog := flag.Bool("no-log", false, "Disables the log")
	configFile := flag.String("conf", "", "Benchmark config file")
	logLevel := flag.String("level", "debug", "Log level (info|debug)")
	flag.Parse()

	setupLogging(*disableLog, *logLevel)
	args := buildArgs(*configFile)
	benchmarkFactory := getBenchmarkFactory(args.Benchmark, args.FileData)
	c := make(chan *worker.BenchmarkResults)

	for i, nWorkers := range args.Workers {
		// workaround for riak's connection limit of 256 per site
		if args.Engine == "riak" && nWorkers/len(args.Connection) > 256 {
			nWorkers = len(args.Connection) * 256
		}

		allResults := [][]*worker.BenchmarkResults{}
		configs := map[string]string{}
		metrics := map[string]string{}

		zlog.Info().Int("workers", nWorkers).Msg("Run started")

		for j := 0; j < args.Runs; j++ {
			startTime := util.EpochSeconds()
			connections := createConnections(args)
			workers := createWorkers(nWorkers, args, connections, benchmarkFactory)
			benchmark := benchmarkFactory(-1)
			benchmark.Setup(connections)

			if j == 0 || !args.NoReload {
				fmt.Println("Populating")
				benchmark.Populate(connections)
			}

			fmt.Println("Running")
			for _, w := range workers {
				go w.Run(c)
			}

			results := []*worker.BenchmarkResults{}
			for k := 0; k < nWorkers; k++ {
				results = append(results, <-c)
			}

			allResults = append(allResults, results)
			if len(configs) == 0 {
				configs = workers[0].GetConfigs()
				metrics = workers[0].GetMetrics()
			}

			fmt.Printf("setupTime=%v\n", (util.EpochSeconds() - startTime - results[0].RealDuration))

			benchmark.Finalize(connections)
			closeConnections(args, connections)
		}

		aggregated := aggregateResults(allResults)
		printSummary(aggregated, args, nWorkers, configs, metrics, i == 0)

		zlog.Info().Int("workers", nWorkers).Msg("Run ended")
	}
}
