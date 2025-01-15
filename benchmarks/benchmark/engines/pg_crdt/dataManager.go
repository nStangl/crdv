package pg_crdt

import (
	"benchmarks/util"
	"database/sql"
	"encoding/base64"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/automerge/automerge-go"
	"github.com/google/uuid"
	"github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

type Change struct {
	id    string
	bytes []byte
}

type DataManager struct {
	remoteGetStmt         *sql.Stmt
	remoteUpdStmt         *sql.Stmt
	remoteAddStmt         *sql.Stmt
	remoteGetAllStmt      *sql.Stmt
	remoteGetMultipleStmt *sql.Stmt
	localMode             bool
	operationBased        bool
	localDb               *sql.DB
	localGetStmt          *sql.Stmt
	localUpdStmt          *sql.Stmt
	notificationsListener *pq.Listener
	changesQueue          chan *Change
	uuid                  string
	lock                  *sync.Mutex
	allDocuments          map[string]*automerge.Doc
	doneChan              chan struct{}
	doneWg                *sync.WaitGroup
}

func newDataManager(db *sql.DB, mode string, replication string, listenConnectionStr string) *DataManager {
	dm := DataManager{}
	dm.uuid = uuid.New().String()
	dm.remoteGetStmt = util.Try(db.Prepare("select crdt.to_bytea(object) from data where id = $1"))
	dm.remoteUpdStmt = util.Try(db.Prepare("select upsert($1, $2, $3)"))
	dm.remoteAddStmt = util.Try(db.Prepare(`
		insert into data 
		values ($1, crdt.autodoc_from_bytea($2), null, $3)
	`))
	dm.remoteGetAllStmt = util.Try(db.Prepare("select id, crdt.to_bytea(object) from data where id like $1"))
	dm.remoteGetMultipleStmt = util.Try(db.Prepare("select id, crdt.to_bytea(object) from data where id = any($1)"))
	dm.localMode = mode == "local"
	dm.operationBased = replication == "operation"

	if dm.localMode {
		dm.allDocuments = map[string]*automerge.Doc{}

		// prepare the notification listener
		dm.notificationsListener = pq.NewListener(listenConnectionStr, 10*time.Second, time.Minute, nil)
		dm.notificationsListener.Listen("notification")

		// local db
		dbPath := "./sqlite/" + dm.uuid + ".db"
		util.CheckErr(os.MkdirAll("./sqlite", os.ModePerm))
		dm.localDb = util.Try(sql.Open("sqlite3", dbPath))
		util.Try(dm.localDb.Exec("create table data (id varchar primary key, object blob)"))
		dm.localGetStmt = util.Try(dm.localDb.Prepare("select object from data where id = ?"))
		dm.localUpdStmt = util.Try(dm.localDb.Prepare(`
			insert into data 
			values (?, ?) 
			on conflict (id) do update set object = excluded.object
		`))
		insertStmt := util.Try(dm.localDb.Prepare("insert into data values ($1, $2)"))
		util.Try(dm.localDb.Exec("PRAGMA main.synchronous=OFF"))

		// load from remote
		tx := util.Try(dm.localDb.Begin())
		rs := util.Try(db.Query("select id, crdt.to_bytea(object) from data"))
		for rs.Next() {
			var id string
			var object []byte
			rs.Scan(&id, &object)
			tx.Stmt(insertStmt).Exec(id, object)
			dm.allDocuments[id] = util.Try(automerge.Load(object))
		}
		util.CheckErr(tx.Commit())

		// local db configs
		util.Try(dm.localDb.Exec("VACUUM"))
		util.Try(dm.localDb.Exec("PRAGMA journal_mode=WAL"))
		util.Try(dm.localDb.Exec("PRAGMA main.synchronous=FULL"))
		util.Try(dm.localDb.Exec("PRAGMA wal_autocheckpoint=1000"))

		// listen for notifications
		dm.changesQueue = make(chan *Change, 20000)
		dm.lock = &sync.Mutex{}
		dm.doneChan = make(chan struct{})
		dm.doneWg = &sync.WaitGroup{}
		dm.doneWg.Add(2)
		go dm.sendChangesWorker()
		go dm.listenNotifications()
	}

	return &dm
}

// Replicate the local changes to the remote database
func (dm *DataManager) sendChangesWorker() {
	done := false
	for !done {
		select {
		// new change to replicate
		case change := <-dm.changesQueue:
			util.Try(dm.remoteUpdStmt.Exec(change.id, change.bytes, dm.uuid))
		// quit, ignore the existing changes waiting replication to cut down on the time needed
		case <-dm.doneChan:
			done = true
		}
	}
	dm.doneWg.Done()
}

func (dm *DataManager) listenNotifications() {
	done := false
	for !done {
		select {
		// new replication
		case x := <-dm.notificationsListener.Notify:
			// connection closed
			if x == nil {
				break
			}

			tokens := strings.Split(x.Extra, "|")
			notificationType := tokens[0]
			objectId := tokens[1]
			uuid := tokens[2]
			bytes := util.Try(base64.StdEncoding.DecodeString(tokens[3]))

			if uuid == dm.uuid {
				continue
			}

			if notificationType == "u" {
				dm.processUpdateNotification(objectId, bytes)
			} else {
				dm.processInsertNotification(objectId, bytes)
			}
		// quit, ignore the existing changes waiting to be applied to cut down on the time needed
		case <-dm.doneChan:
			done = true
		}
	}

	dm.doneWg.Done()
}

// Processes an update notification from a remote site
func (dm *DataManager) processUpdateNotification(objectId string, bytes []byte) {
	dm.lock.Lock()

	doc := dm.allDocuments[objectId]

	if dm.operationBased {
		util.CheckErr(doc.LoadIncremental(bytes))
	} else {
		remoteObject := util.Try(automerge.Load(bytes))
		util.Try(doc.Merge(remoteObject))
	}

	// To improve performance, replications are not synced. The new value is kept in memory.
	// It is later synced to the database when a local write occurs.
	//util.Try(dm.localUpdStmt.Exec(objectId, doc.Save()))

	dm.lock.Unlock()
}

// Processes an insert notification from a remote site
func (dm *DataManager) processInsertNotification(objectId string, bytes []byte) {
	doc := util.Try(automerge.Load(bytes))

	dm.lock.Lock()

	dm.allDocuments[objectId] = doc
	// To improve performance, replications are not synced. The new value is kept in memory.
	// It is later synced to the database when a local write occurs.
	//util.Try(dm.localUpdStmt.Exec(objectId, doc.Save()))

	dm.lock.Unlock()
}

// Gets the automerge doc from the bytes returned by the getStmt query, with id as the query filter
func (dm *DataManager) getDoc(id string) *automerge.Doc {
	var row *sql.Row

	if dm.localMode {
		row = dm.localGetStmt.QueryRow(id)
	} else {
		row = dm.remoteGetStmt.QueryRow(id)
	}

	var bytes []byte
	row.Scan(&bytes)
	return util.Try(automerge.Load(bytes))
}

// Update the object locally
func (dm *DataManager) applyLocally(id string, newDoc *automerge.Doc) error {
	dm.lock.Lock()
	defer dm.lock.Unlock()
	localDoc := dm.allDocuments[id]
	if localDoc == nil {
		localDoc = automerge.New()
		dm.allDocuments[id] = localDoc
	}
	localDoc.Merge(newDoc)
	bytes := localDoc.Save()
	_, err := dm.localUpdStmt.Exec(id, bytes)
	return err
}

// Update the object remotely
func (dm *DataManager) applyRemotely(id string, doc *automerge.Doc) error {
	heads := doc.Heads()
	if len(heads) > 0 {
		head := heads[0]
		change := util.Try(doc.Change(head))
		bytes := change.Save()
		_, err := dm.remoteUpdStmt.Exec(id, bytes, dm.uuid)
		return err
	} else {
		return nil
	}
}

// Applies the change to the database(s)
func (dm *DataManager) applyChange(id string, doc *automerge.Doc) error {
	var err error

	if dm.localMode {
		err = dm.applyLocally(id, doc)
		if err == nil {
			// asynchronously add to the remote server
			heads := doc.Heads()
			if len(heads) > 0 {
				head := heads[0]
				lastChange := util.Try(doc.Change(head))
				change := Change{id: id, bytes: lastChange.Save()}
				dm.changesQueue <- &change
			}
		}
	} else {
		err = dm.applyRemotely(id, doc)
	}

	return err
}

// Returns a map with all docs whose id match some prefix
func (dm *DataManager) getAllByPrefix(prefix string) map[string]*automerge.Doc {
	result := map[string]*automerge.Doc{}

	if dm.localMode {
		dm.lock.Lock()
		// read from memory only, to improve performance
		for id, doc := range dm.allDocuments {
			if strings.HasPrefix(id, prefix) {
				result[id] = doc
			}
		}
		dm.lock.Unlock()
	} else {
		rs := util.Try(dm.remoteGetAllStmt.Query(prefix + "%"))
		for rs.Next() {
			var id string
			var bytes []byte
			rs.Scan(&id, &bytes)
			result[id] = util.Try(automerge.Load(bytes))
		}
	}

	return result
}

// Returns a map with all docs pertaining to the ids passed as argument
func (dm *DataManager) getMultiple(ids []string) map[string]*automerge.Doc {
	result := map[string]*automerge.Doc{}

	if dm.localMode {
		dm.lock.Lock()
		// read from memory only, to improve performance
		for _, id := range ids {
			if doc, ok := dm.allDocuments[id]; ok {
				result[id] = doc
			}
		}
		dm.lock.Unlock()
	} else {
		rs := util.Try(dm.remoteGetMultipleStmt.Query(pq.Array(ids)))
		for rs.Next() {
			var id string
			var bytes []byte
			rs.Scan(&id, &bytes)
			result[id] = util.Try(automerge.Load(bytes))
		}
	}

	return result
}

func (dm *DataManager) finalize() {
	if dm.localMode {
		dm.doneChan <- struct{}{}
		dm.doneChan <- struct{}{}
		dm.doneWg.Wait()
		util.CheckErr(dm.notificationsListener.Close())
		util.CheckErr(dm.localDb.Close())
	}
}
