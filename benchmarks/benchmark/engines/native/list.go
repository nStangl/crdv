package native

import (
	"benchmarks/util"
	"database/sql"
	"sync"

	"github.com/lib/pq"
)

type List struct {
	getStmt     *sql.Stmt
	getAtStmt   *sql.Stmt
	addStmt     *sql.Stmt
	appendStmt  *sql.Stmt
	prependStmt *sql.Stmt
	rmvStmt     *sql.Stmt
	clearStmt   *sql.Stmt
}

func populateLists(wg *sync.WaitGroup, db *sql.DB, nLists int, size int, valueLength int) {
	defer wg.Done()

	// switch the list id generation mode to improve the populate
	util.Try(db.Exec("select switch_list_id_generation('appends')"))

	util.Try(db.Exec(`
		with recursive t(p) as (
			select _generateVirtualIndexBetween('', '')
			union all
			select _generateVirtualIndexBetween(p, '')
			from t
		)
		insert into native_list
		select 'l-' || i, p, $3
		from (
			select generate_series(0, $1 - 1) as i
		) T, (select * from t limit $2) T2
	`, nLists, size, util.RandomString(valueLength)))

	// switch the list generation mode to the default
	util.Try(db.Exec("select switch_list_id_generation('regular')"))
}

func newList(db *sql.DB) *List {
	l := &List{}
	l.getStmt = util.Try(db.Prepare("select array_agg(value) from native_list where id = $1"))
	l.getAtStmt = util.Try(db.Prepare("select value from native_list where id = $1 offset $2 limit 1"))
	l.addStmt = util.Try(db.Prepare(`
		insert into native_list 
		values($1::varchar, (select _generateVirtualIndexBetween(
			(select *
			from (
				(select pos from native_list where id = $1 offset greatest($2 - 1, 0) limit least($2, 1))
				UNION ALL
				(select max(pos) from native_list WHERE id = $1 limit least($2, 1))
			) T
			limit 1),
			(select pos from native_list where id = $1 offset $2 limit 1)
		)), $3)
	`))
	l.appendStmt = util.Try(db.Prepare(`
		insert into native_list 
		values($1::varchar, (select _generateVirtualIndexBetween((select max(pos) from native_list where id = $1), '')), $2)
	`))
	l.prependStmt = util.Try(db.Prepare(`
		insert into native_list 
		values($1::varchar, (select _generateVirtualIndexBetween('', (select min(pos) from native_list where id = $1))), $2)
	`))
	l.rmvStmt = util.Try(db.Prepare(`
		delete from native_list 
		where id = $1 
			and pos = (select pos from native_list where id = $1 offset $2 limit 1)`))
	l.clearStmt = util.Try(db.Prepare("delete from native_list where id = $1"))
	return l
}

func (l *List) Get(id string) ([]string, error) {
	rs := util.Try(l.getStmt.Query(id))
	rs.Next()
	defer rs.Close()

	values := []string{}
	rs.Scan(pq.Array(&values))

	return values, nil
}

func (l *List) GetAt(id string, index int) (string, error) {
	rs := util.Try(l.getAtStmt.Query(id, index))
	rs.Next()
	defer rs.Close()

	var value string
	rs.Scan(&value)

	return value, nil
}

func (l *List) Add(id string, index int, value string) error {
	_, err := l.addStmt.Exec(id, index, value)
	return err
}

func (l *List) Append(id string, value string) error {
	_, err := l.appendStmt.Exec(id, value)
	return err
}

func (l *List) Prepend(id string, value string) error {
	_, err := l.prependStmt.Exec(id, value)
	return err
}

func (l *List) Rmv(id string, index int) error {
	_, err := l.rmvStmt.Exec(id, index)
	return err
}

func (l *List) Clear(id string) error {
	_, err := l.clearStmt.Exec(id)
	return err
}
