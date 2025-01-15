package timestampencoding

import (
	"benchmarks/benchmark/timestampEncoding/row"
	"benchmarks/benchmark/timestampEncoding/schema"
	"benchmarks/util"
	"database/sql"
	"log"
	"math/rand"
	"strconv"

	"github.com/lib/pq"
	"gopkg.in/yaml.v3"
)

type TimestampEncoding struct {
	id         int
	Schema     string
	Ops        int
	Items      int
	Sites      int
	statements *schema.SchemaStmts
	schemaObj  schema.Schema
}

func (t *TimestampEncoding) generateHistory() []row.Row {
	var rows []row.Row
	var allClocks []row.Clock
	var currClocks []row.Clock

	for i := 0; i < t.Sites; i++ {
		currClocks = append(currClocks, make(row.Clock, t.Sites))
	}

	for i := 0; i < t.Ops; {
		for site := 0; site < t.Sites && i < t.Ops; site, i = site+1, i+1 {
			clock := currClocks[site]
			clock[site] += 1
			clockCopy := make(row.Clock, len(clock))
			copy(clockCopy, clock)
			allClocks = append(allClocks, clockCopy)

			for remoteSite := 0; remoteSite < t.Sites && i < t.Ops; remoteSite++ {
				if remoteSite != site {
					if rand.Float64() < 0.05 {
						row.MergeClocks(clock, currClocks[remoteSite])
						clockCopy := make(row.Clock, len(clock))
						copy(clockCopy, clock)
						allClocks = append(allClocks, clockCopy)
						i += 1
					}
				}
			}
		}
	}

	for i := 0; i < t.Items; i++ {
		for _, c := range allClocks {
			rows = append(rows, row.Row{K: int64(i), V: rand.Int63(), Lts: c})
		}
	}

	return rows
}

func New(id int, configData []byte) *TimestampEncoding {
	timestampEncoding := TimestampEncoding{}
	timestampEncoding.id = id
	util.CheckErr(yaml.Unmarshal(configData, &timestampEncoding))

	switch timestampEncoding.Schema {
	case "row":
		timestampEncoding.schemaObj = &schema.RowSchema{}
	case "array":
		timestampEncoding.schemaObj = &schema.ArraySchema{}
	case "json":
		timestampEncoding.schemaObj = &schema.JsonSchema{}
	case "cube":
		timestampEncoding.schemaObj = &schema.CubeSchema{}
	default:
		log.Fatalf("Invalid schema '%s' in TimestampEncoding\n", timestampEncoding.Schema)
	}

	return &timestampEncoding
}

func (*TimestampEncoding) Setup(connections []any) {}

func (t *TimestampEncoding) Populate(connections []any) {
	dbs := util.CastArray[any, *sql.DB](connections)
	rows := t.generateHistory()
	t.schemaObj.Populate(dbs[0], rows)

	// vacuum + checkpoint
	util.Try(dbs[0].Exec("vacuum analyze"))
	util.Try(dbs[0].Exec("checkpoint"))
}

func (t *TimestampEncoding) readKey(_ *sql.DB) error {
	key := rand.Int63n(int64(t.Items))
	result := []row.Row{}
	rs := util.Try(t.statements.ReadKey.Query(key))

	for rs.Next() {
		var k int64
		var v int64
		var lts pq.Int64Array
		rs.Scan(&k, &v, &lts)
		lts_ := row.Clock{}
		for _, t := range lts {
			lts_ = append(lts_, t)
		}
		result = append(result, row.Row{K: k, V: v, Lts: lts_})
	}

	return nil
}

func (t *TimestampEncoding) readAll(_ *sql.DB) error {
	result := []row.Row{}
	rs := util.Try(t.statements.ReadAll.Query())

	for rs.Next() {
		var k int64
		var v int64
		var lts row.Clock
		rs.Scan(&k, &v, &lts)
		result = append(result, row.Row{K: k, V: v, Lts: lts})
	}

	return nil
}

func (t *TimestampEncoding) currTime(_ *sql.DB) error {
	key := rand.Int63n(int64(t.Items))
	rs := util.Try(t.statements.CurrTime.Query(key))

	for rs.Next() {
		var t string
		rs.Scan(&t)
	}

	return nil
}

func (t *TimestampEncoding) write(db *sql.DB) error {
	key := rand.Int63n(int64(t.Items))
	value := rand.Int63()
	txn, _ := db.Begin()

	r := txn.Stmt(t.statements.NextTime).QueryRow(key)
	var ts string
	r.Scan(&ts)

	util.Try(txn.Stmt(t.statements.Write).Exec(key, value, ts))

	return txn.Commit()
}

func (t *TimestampEncoding) Prepare(connection any) map[string]func() error {
	db := connection.(*sql.DB)
	t.statements = t.schemaObj.Prepare(db, t.Sites)
	operations := map[string]func() error{
		"readKey":  func() error { return t.readKey(db) },
		"readAll":  func() error { return t.readAll(db) },
		"currTime": func() error { return t.currTime(db) },
		"write":    func() error { return t.write(db) },
	}
	return operations
}

func (t *TimestampEncoding) size(db *sql.DB) int64 {
	util.Try(db.Exec("vacuum analyze"))
	row := t.statements.Size.QueryRow()
	var s int64
	row.Scan(&s)
	return s
}

func (t *TimestampEncoding) GetConfigs() map[string]string {
	return map[string]string{
		"schema":         t.Schema,
		"ops":            strconv.Itoa(t.Ops),
		"items":          strconv.Itoa(t.Items),
		"simulatedSites": strconv.Itoa(t.Sites),
	}
}

func (t *TimestampEncoding) GetMetrics(connection any) map[string]string {
	db := connection.(*sql.DB)
	return map[string]string{
		"size": strconv.FormatInt(t.size(db), 10),
	}
}

func (t *TimestampEncoding) Finalize(connections []any) {}
