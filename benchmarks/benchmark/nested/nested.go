package nested

import (
	dbutils "benchmarks/dbUtils"
	"benchmarks/util"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	zlog "github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

type Nested struct {
	id                int
	Items             int
	NestingLevel      int    `yaml:"nestingLevel"`
	ReadQuery         string `yaml:"readQuery"`
	Modes             map[string]string
	MergeParallelism  int     `yaml:"mergeParallelism"`
	MergeDelta        float64 `yaml:"mergeDelta"`
	MergeBatchSize    int     `yaml:"mergeBatchSize"`
	PopulateBatchSize int     `yaml:"populateBatchSize"`
	readStmt          *sql.Stmt
	explainStmt       *sql.Stmt
}

var ids []string
var totalPlanRt *atomic.Int64 // micro seconds
var totalExecRt *atomic.Int64 // micro seconds
var totalCount *atomic.Int64
var rtsMutex *sync.Mutex
var planRts []float64 // micro seconds
var execRts []float64 // micro seconds

func New(id int, configData []byte) *Nested {
	delay := Nested{}
	delay.id = id
	util.CheckErr(yaml.Unmarshal(configData, &delay))
	return &delay
}

func (n *Nested) log(msg string) {
	zlog.Info().Str("benchmark", "nested").Int("id", n.id).Msg(msg)
}

func (n *Nested) Setup(connections []any) {
	totalPlanRt = &atomic.Int64{}
	totalExecRt = &atomic.Int64{}
	totalCount = &atomic.Int64{}
	rtsMutex = &sync.Mutex{}
	planRts = []float64{}
	execRts = []float64{}
}

func (n *Nested) Populate(connections []any) {
	dbs := util.CastArray[any, *sql.DB](connections)
	// init
	dbutils.InitDb(dbs, n.MergeParallelism, n.MergeDelta, n.MergeBatchSize)

	// inserts are propagated, so we just need to insert in one database
	n.log("Populating")
	db := dbs[0]

	// add data
	var wg sync.WaitGroup
	for batch := 0; batch*n.PopulateBatchSize < n.Items; batch++ {
		wg.Add(1)
		go func(batch int) {
			defer wg.Done()
			util.Try(db.Exec(`
				select mapAdd('m-' || i || '-' || j, 'k-1', 'm-' || i || '-' || (j + 1))
				from (select generate_series($1::bigint, $2::bigint) as i) t1, (select generate_series(1, $3::bigint) as j) t2
			`, batch*n.PopulateBatchSize, min(n.Items, (batch+1)*n.PopulateBatchSize), n.NestingLevel))
		}(batch)
	}
	wg.Wait()

	// build ids
	for i := 0; i < n.Items; i++ {
		ids = append(ids, fmt.Sprintf("m-%d-1", i))
	}

	n.log("Populate done")

	// wait for the data to be synced
	dbutils.WaitForSyncAllDBs(dbs)

	// vacuum + checkpoint
	dbutils.VacuumAndCheckpointAllDBs(dbs)

	n.planSize(dbs[0])
}

func (n *Nested) randomId() string {
	return ids[rand.Intn(len(ids))]
}

func (n *Nested) read(db *sql.DB, id string) error {
	rs := util.Try(n.readStmt.Query(id))
	defer rs.Close()
	rs.Next()

	var json string
	rs.Scan(&json)

	return nil
}

func (n *Nested) explain(db *sql.DB, id string) error {
	rs := util.Try(n.explainStmt.Query(id))
	defer rs.Close()

	for rs.Next() {
		var line string
		rs.Scan(&line)
		if strings.HasPrefix(line, "Planning Time:") {
			rt, _ := strconv.ParseFloat(strings.Split(line, " ")[2], 64)
			totalPlanRt.Add(int64(rt * 1000))
			rtsMutex.Lock()
			planRts = append(planRts, rt*1000)
			rtsMutex.Unlock()
		} else if strings.HasPrefix(line, "Execution Time:") {
			rt, _ := strconv.ParseFloat(strings.Split(line, " ")[2], 64)
			totalExecRt.Add(int64(rt * 1000))
			rtsMutex.Lock()
			execRts = append(execRts, rt*1000)
			rtsMutex.Unlock()
		}
	}

	totalCount.Add(1)

	// return an error so this operation is not considered in the real response time
	return errors.New("")
}

func (n *Nested) Prepare(connection any) map[string]func() error {
	db := connection.(*sql.DB)
	// switch to the correct modes
	dbutils.SetReadMode(db, n.Modes["readMode"])
	dbutils.SetWriteMode(db, n.Modes["writeMode"])

	// create the nested json view
	util.Try(db.Exec("create or replace view nested_view as " + n.ReadQuery))

	// statements
	// while the read statement simply retrieves the JSON representation of the of the nested
	// structure, the explain performs the "explain analyze" command to retrieve both the planning
	// and execution times
	n.readStmt = util.Try(db.Prepare("select data from nested_view where id = $1"))
	n.explainStmt = util.Try(db.Prepare("explain analyze select data from nested_view where id = $1"))

	// ops
	operations := map[string]func() error{
		"read":    func() error { return n.read(db, n.randomId()) },
		"explain": func() error { return n.explain(db, n.randomId()) },
	}
	return operations
}

func (n *Nested) planSize(db *sql.DB) int {
	rs := util.Try(db.Query("explain analyze " + n.ReadQuery))
	defer rs.Close()
	size := 0

	for rs.Next() {
		size++
	}

	// exclude planning and execution times' lines
	return size - 2
}

func (n *Nested) GetConfigs() map[string]string {
	return map[string]string{
		"items":            strconv.Itoa(n.Items),
		"nestingLevel":     strconv.Itoa(n.NestingLevel),
		"readMode":         n.Modes["readMode"],
		"writeMode":        n.Modes["writeMode"],
		"engine":           "crdv-" + n.Modes["writeMode"],
		"mergeParallelism": strconv.Itoa(n.MergeParallelism),
		"mergeDelta":       strconv.FormatFloat(n.MergeDelta, 'f', -1, 64),
		"mergeBatchSize":   strconv.Itoa(n.MergeBatchSize),
	}
}

func (n *Nested) GetMetrics(connection any) map[string]string {
	db := connection.(*sql.DB)
	totalRts := planRts
	totalRts = append(totalRts, execRts...)
	return map[string]string{
		"planSize":  strconv.Itoa(n.planSize(db)),
		"planTime":  fmt.Sprintf("%.6f", float64(totalPlanRt.Load())/float64(totalCount.Load())/1e6),
		"execTime":  fmt.Sprintf("%.6f", float64(totalExecRt.Load())/float64(totalCount.Load())/1e6),
		"totalTime": fmt.Sprintf("%.6f", float64(totalPlanRt.Load()+totalExecRt.Load())/float64(totalCount.Load())/1e6),
		"planP95":   fmt.Sprintf("%.6f", util.Percentile(planRts, 95)/1e6),
		"execP95":   fmt.Sprintf("%.6f", util.Percentile(execRts, 95)/1e6),
		"totalP95":  fmt.Sprintf("%.6f", util.Percentile(totalRts, 95)/1e6),
	}
}

func (n *Nested) Finalize(connections []any) {
	dbs := util.CastArray[any, *sql.DB](connections)
	dbutils.WaitForSyncAllDBs(dbs)
}
