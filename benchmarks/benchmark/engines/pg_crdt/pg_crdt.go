package pg_crdt

import (
	engine "benchmarks/benchmark/engines/abstract"
	dbutils "benchmarks/dbUtils"
	"benchmarks/util"
	"database/sql"
	"os"
	"slices"
	"strconv"
	"sync"

	"gopkg.in/yaml.v3"
)

type PgCrdt struct {
	id                     int
	VacuumFull             bool     `yaml:"vacuumFull"`
	InitialOpsPerStructure int      `yaml:"initialOpsPerStructure"`
	ItemsPerStructure      int      `yaml:"itemsPerStructure"`
	TypesToPopulate        []string `yaml:"typesToPopulate"`
	Mode                   string   `yaml:"mode"`
	counter                *Counter
	register               *Register
	set                    *Set
	map_                   *Map
	list                   *List
	dataManager            *DataManager
	Connection             []string
	Replication            string `yaml:"replication"`
}

var initialDbSize int64
var dataManagers []*DataManager
var dataManagersLock *sync.Mutex

func New(id int, configData []byte) *PgCrdt {
	pgCrdt := PgCrdt{}
	pgCrdt.id = id
	util.CheckErr(yaml.Unmarshal(configData, &pgCrdt))
	dataManagers = []*DataManager{}
	dataManagersLock = &sync.Mutex{}
	return &pgCrdt
}

func (p *PgCrdt) Setup(connections []any) {}

func (p *PgCrdt) createSchema(db *sql.DB) {
	util.Try(db.Exec("drop table if exists data"))

	// create the data table
	util.Try(db.Exec(`create table data(id varchar primary key, object crdt.autodoc, 
			last_change crdt.autochange, last_change_src varchar)`))

	// this upsert is a function that first tries to update and only then performs an insert, if the
	// row does not exist it. the regular "insert ... on conflict update ..." does not work since we
	// cannot build an object from a change (done in the insert) if the change has extra
	// dependencies (invalid AutoDoc (binary): change's deps should already be in the document), but
	// it is possible if the change was made from an empty document. the regular upsert would
	// evaluate the autodoc_from_bytea part of the insert, whether or not the row exists.
	util.Try(db.Exec(`
		create or replace function upsert(id_ varchar, change_ bytea, src_ varchar) returns void as $$
			begin
				update data
				set object = crdt.merge(data.object, crdt.autochange_from_bytea(change_)),
					last_change = crdt.autochange_from_bytea(change_),
					last_change_src = src_
				where id = id_;

				if not found then
					insert into data 
					values (id_, crdt.autodoc_from_bytea(change_), crdt.autochange_from_bytea(change_), src_);
				end if;
			end;
		$$ language plpgsql;
	`))

	// create the replication triggers for the local modes
	if p.Mode == "local" {
		if p.Replication == "operation" {
			util.Try(db.Exec(`
				create or replace function replicate_update_function() returns trigger as $$
				begin
					perform pg_notify('notification', 'u' || '|' || new.id || '|' || new.last_change_src || '|' || encode(new.last_change, 'base64'));
					return new;
				end;
				$$ language plpgsql
			`))
		} else {
			util.Try(db.Exec(`
				create or replace function replicate_update_function() returns trigger as $$
				begin
					perform pg_notify('notification', 'u' || '|' || new.id || '|' || new.last_change_src || '|' || encode(new.object, 'base64'));
					return new;
				end;
				$$ language plpgsql
			`))
		}

		util.Try(db.Exec(`
			create or replace trigger replicate_update_trigger
			after update on data
			for each row
			execute function replicate_update_function()
		`))

		util.Try(db.Exec(`
			create or replace function replicate_insert_function() returns trigger as $$
			begin
				perform pg_notify('notification', 'i' || '|' ||  new.id || '|' || new.last_change_src || '|' || encode(new.object, 'base64'));
				return new;
			end;
			$$ language plpgsql
		`))

		util.Try(db.Exec(`
			create or replace trigger replicate_insert_trigger
			after insert on data
			for each row
			execute function replicate_insert_function()
		`))

		if p.Mode == "local" {
			os.RemoveAll("./sqlite")
		}
	}
}

func (p *PgCrdt) Cleanup(connections []any) {
	dbs := util.CastArray[any, *sql.DB](connections)
	db := dbs[0]

	// create the schema
	p.createSchema(db)

	// vacuum + checkpoint
	dbutils.VacuumAndCheckpointAllDBs(dbs)
}

func (p *PgCrdt) Populate(connections []any, typesToPopulate []string, itemsPerStructure int, opsPerItem int, valueLength int) {
	dbs := util.CastArray[any, *sql.DB](connections)
	db := dbs[0]

	// create the schema
	p.createSchema(db)

	wg := sync.WaitGroup{}
	wg.Add(len(typesToPopulate))

	if slices.Contains(p.TypesToPopulate, "counter") {
		go populateCounters(&wg, db, p.ItemsPerStructure)
	}
	if slices.Contains(p.TypesToPopulate, "register") {
		go populateRegisters(&wg, db, p.ItemsPerStructure, valueLength)
	}
	if slices.Contains(p.TypesToPopulate, "set") {
		go populateSets(&wg, db, p.ItemsPerStructure, p.InitialOpsPerStructure)
	}
	if slices.Contains(p.TypesToPopulate, "map") {
		go populateMaps(&wg, db, p.ItemsPerStructure, p.InitialOpsPerStructure, valueLength)
	}
	if slices.Contains(p.TypesToPopulate, "list") {
		go populateLists(&wg, db, p.ItemsPerStructure, p.InitialOpsPerStructure, valueLength)
	}

	wg.Wait()

	// vacuum + checkpoint
	dbutils.VacuumAndCheckpointAllDBs(dbs)

	initialDbSize = p.dbSize(db, p.VacuumFull)
}

func (p *PgCrdt) Prepare(connection any) {
	db := connection.(*sql.DB)
	p.dataManager = newDataManager(db, p.Mode, p.Replication, p.Connection[0])
	dataManagersLock.Lock()
	dataManagers = append(dataManagers, p.dataManager)
	dataManagersLock.Unlock()
	p.counter = newCounter(p.dataManager)
	p.register = newRegister(p.dataManager)
	p.set = newSet(p.dataManager)
	p.map_ = newMap(p.dataManager)
	p.list = newList(p.dataManager)
}

func (p *PgCrdt) GetRegister() engine.Register {
	return p.register
}

func (p *PgCrdt) GetCounter() engine.Counter {
	return p.counter
}

func (p *PgCrdt) GetSet() engine.Set {
	return p.set
}

func (p *PgCrdt) GetMap() engine.Map {
	return p.map_
}

func (p *PgCrdt) GetList() engine.List {
	return p.list
}

func (p *PgCrdt) GetConfigs() map[string]string {
	return map[string]string{
		"initialOpsPerStructure": strconv.Itoa(p.InitialOpsPerStructure),
		"itemsPerStructure":      strconv.Itoa(p.ItemsPerStructure),
		"engine":                 "pg_crdt",
	}
}

func (p *PgCrdt) dbSize(db *sql.DB, vacuumFull bool) int64 {
	if vacuumFull {
		util.Try(db.Exec("vacuum full analyze"))
	} else {
		util.Try(db.Exec("vacuum analyze"))
	}
	row := db.QueryRow(`
		select pg_total_relation_size('data')
	`)
	var s int64
	row.Scan(&s)
	return s
}

func (p *PgCrdt) GetMetrics(connection any) map[string]string {
	db := connection.(*sql.DB)
	return map[string]string{
		"startSize": strconv.FormatInt(initialDbSize, 10),
		"endSize":   strconv.FormatInt(p.dbSize(db, p.VacuumFull), 10),
	}
}

func (p *PgCrdt) Finalize(connections []any) {
	for _, dm := range dataManagers {
		dm.finalize()
	}
}
