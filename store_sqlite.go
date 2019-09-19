package gokvstore

import (
	"database/sql"
	"encoding/json"
	"log"
	"os"

	"github.com/korovkin/gotils"

	// import sqlite implementation
	_ "github.com/mattn/go-sqlite3"
)

// StoreSqlite sqlite based key value store
type StoreSqlite struct {
	Db                 *sql.DB   `json:"-"`
	InsertStmt         *sql.Stmt `json:"-"`
	GetStmt            *sql.Stmt `json:"-"`
	IterateStmt        *sql.Stmt `json:"-"`
	IterateAllStmt     *sql.Stmt `json:"-"`
	IterateByPrefixASC *sql.Stmt `json:"-"`
	IterateByPrefixDSC *sql.Stmt `json:"-"`
	IterateDSCStmt     *sql.Stmt `json:"-"`
	DeleteStmt         *sql.Stmt `json:"-"`
	DeleteAllStmt      *sql.Stmt `json:"-"`
	DeleteStmtTag      *sql.Stmt `json:"-"`
	CountAllStmt       *sql.Stmt `json:"-"`
	Filename           string    `json:"filename"`
	currentTransaction *sql.Tx
}

// NewStoreSqlite allocate a new instance of StoreSqlite
// will create a sqlite file name 'tableName' in 'folder'
func NewStoreSqlite(tableName string, folder string) (*StoreSqlite, error) {
	var err error
	store := StoreSqlite{}

	if folder == "" {
		folder = "."
	}

	if folder == ":memory:" {
		store.Filename = folder
	} else {
		store.Filename = folder + "/" + tableName + ".db"
	}

	store.Db, err = sql.Open("sqlite3", store.Filename)
	if err != nil {
		return nil, err
	}

	_, err = store.Db.Exec(
		`PRAGMA busy_timeout = 50000;`)
	gotils.CheckFatal(err)

	_, err = store.Db.Exec(
		`PRAGMA journal_mode = OFF;`)
	gotils.CheckFatal(err)

	_, err = store.Db.Exec(
		`CREATE TABLE IF NOT EXISTS KV 
			(K string primary key, V string, T string);`)
	gotils.CheckFatal(err)

	_, err = store.Db.Exec(
		`CREATE INDEX IF NOT EXISTS KV_K 
			ON KV (K);`)
	gotils.CheckFatal(err)

	_, err = store.Db.Exec(
		`CREATE INDEX IF NOT EXISTS KV_T 
			ON KV (T);`)
	gotils.CheckFatal(err)

	store.InsertStmt, err = store.Db.Prepare(
		`INSERT OR REPLACE 
			INTO KV(K, V, T) 
			VALUES(?, ?, ?)`)
	gotils.CheckFatal(err)

	store.GetStmt, err = store.Db.Prepare(
		`SELECT K, V, T 
			FROM KV 
			WHERE K=?`)
	gotils.CheckFatal(err)

	store.IterateStmt, err = store.Db.Prepare(
		`SELECT K, V 
			FROM KV 
			WHERE K<=? 
			ORDER BY K DESC 
			LIMIT ?`)
	gotils.CheckFatal(err)

	store.IterateAllStmt, err = store.Db.Prepare(
		`SELECT K, V, T 
			FROM KV 
			ORDER BY K`)
	gotils.CheckFatal(err)

	store.IterateByPrefixASC, err = store.Db.Prepare(
		`SELECT K, V, T 
			FROM KV
			WHERE K >= $1
			ORDER BY K ASC
			LIMIT $2`)
	gotils.CheckFatal(err)

	store.IterateByPrefixDSC, err = store.Db.Prepare(
		`SELECT K, V, T 
			FROM KV
			WHERE K <= $1
			ORDER BY K DESC
			LIMIT $2`)
	gotils.CheckFatal(err)

	store.DeleteStmt, err = store.Db.Prepare(
		`DELETE 
			FROM KV 
			WHERE K=?`)
	gotils.CheckFatal(err)

	store.DeleteAllStmt, err = store.Db.Prepare(
		`DELETE 
			FROM KV 
			WHERE 1`)
	gotils.CheckFatal(err)

	store.DeleteStmtTag, err = store.Db.Prepare(
		`DELETE 
			FROM KV 
			WHERE T=$1`)
	gotils.CheckFatal(err)

	store.IterateDSCStmt, err = store.Db.Prepare(
		`SELECT K, V, T 
			FROM KV 
			ORDER BY K 
			LIMIT ?`)
	gotils.CheckFatal(err)

	store.CountAllStmt, err = store.Db.Prepare(
		`SELECT 
			COUNT(K), 
			MIN(K), 
			MAX(K) 
		FROM KV`)
	gotils.CheckFatal(err)

	return &store, err
}

// Close close all the statements and the sqlite db
func (s *StoreSqlite) Close() {
	s.InsertStmt.Close()
	s.GetStmt.Close()
	s.IterateStmt.Close()
	s.IterateDSCStmt.Close()
	s.DeleteStmt.Close()
	s.DeleteStmtTag.Close()
	s.DeleteAllStmt.Close()
	s.CountAllStmt.Close()
	s.IterateByPrefixASC.Close()
	s.IterateByPrefixDSC.Close()
	s.IterateAllStmt.Close()
	s.Db.Close()
	s.Db = nil
}

// CloseAndDelete deletes the sqlite DB from the file system
func (s *StoreSqlite) CloseAndDelete() {
	s.Close()
	log.Println("STORE: Remove:", s.Filename)
	os.RemoveAll(s.Filename)
}

// AddValueKVT add (k,v,t) to the store
func (s *StoreSqlite) AddValueKVT(k string, v string, t string) error {
	if s.currentTransaction != nil {
		stmt := s.currentTransaction.Stmt(s.InsertStmt)
		_, err := stmt.Exec(k, v, "")
		gotils.CheckNotFatal(err)
		return err
	}

	_, err := s.InsertStmt.Exec(k, v, t)
	gotils.CheckNotFatal(err)
	return err
}

// AddValueKV add (k,v) to the store
func (s *StoreSqlite) AddValueKV(k string, v string) error {
	if s.currentTransaction != nil {
		stmt := s.currentTransaction.Stmt(s.InsertStmt)
		_, err := stmt.Exec(k, v, "")
		gotils.CheckNotFatal(err)
		return err
	}

	_, err := s.InsertStmt.Exec(k, v, "")
	gotils.CheckNotFatal(err)
	return err
}

// DeleteValue delete k from the store
func (s *StoreSqlite) DeleteValue(k string) error {
	_, err := s.DeleteStmt.Exec(k)
	gotils.CheckNotFatal(err)
	return err
}

// DeleteAllWithTag delete all value with tag t from the store
func (s *StoreSqlite) DeleteAllWithTag(t string) error {
	_, err := s.DeleteStmtTag.Exec(t)
	gotils.CheckNotFatal(err)
	return err
}

// DeleteAll delete all the data in the store
func (s *StoreSqlite) DeleteAll() error {
	_, err := s.DeleteAllStmt.Exec()
	gotils.CheckNotFatal(err)
	return err
}

// AddValueAsJSON add (k, t, json(o)) to the store
func (s *StoreSqlite) AddValueAsJSON(k string, t string, o interface{}) error {
	v := gotils.ToJSONStringNoIndent(o)
	err := s.AddValueKVT(k, v, t)
	return err
}

// CountAll count number of items in the store
func (s *StoreSqlite) CountAll() (int64, string, string) {
	res, err := s.CountAllStmt.Query()
	gotils.CheckNotFatal(err)

	if err != nil {
		return -1, "", ""
	}

	defer res.Close()
	for res.Next() {
		var count int64
		var min string
		var max string
		err = res.Scan(&count, &min, &max)
		// gotils.CheckNotFatal(err)
		if err != nil {
			return 0, "", ""
		}
		return count, min, max
	}
	return -1, "", ""

}

// Transaction run the given block under a sqlite transaction
func (s *StoreSqlite) Transaction(block func()) error {
	var err error

	if s.currentTransaction != nil {
		log.Fatalln("NO CONCURRENT TRANSACTIONS")
	}
	transaction, err := s.Db.Begin()
	gotils.CheckNotFatal(err)

	s.currentTransaction = transaction

	if err != nil {
		return err
	}

	block()

	err = transaction.Commit()
	gotils.CheckNotFatal(err)
	s.currentTransaction = nil

	return err
}

// IterateValuesAsJSON traverse all the values in the store by key
func (s *StoreSqlite) IterateValuesAsJSON(
	block func(k string, v string, stop *bool),
	numItems int) {
	var err error
	var res *sql.Rows

	res, err = s.IterateDSCStmt.Query(numItems)
	gotils.CheckNotFatal(err)

	if err != nil {
		return
	}
	defer res.Close()

	stop := false
	for res.Next() {
		var k string
		var v string
		err = res.Scan(&k, &v)
		gotils.CheckNotFatal(err)

		if err != nil {
			continue
		}

		block(k, v, &stop)
		if stop {
			break
		}
	}
}

// IterateAll traverse all the items in the store
func (s *StoreSqlite) IterateAll(
	o interface{},
	block func(k string, t string, v string, stop *bool)) {
	var err error
	var res *sql.Rows
	res, err = s.IterateAllStmt.Query()
	gotils.CheckNotFatal(err)
	if err != nil {
		return
	}
	defer res.Close()

	stop := false
	for res.Next() {
		var k string
		var v string
		var t string
		err = res.Scan(&k, &v, &t)
		gotils.CheckNotFatal(err)
		if err != nil {
			continue
		}

		err = json.Unmarshal([]byte(v), &o)
		gotils.CheckNotFatal(err)
		if err != nil {
			continue
		}
		block(k, t, v, &stop)
		if stop {
			break
		}
	}
}

// GetValue get the value for the given k
func (s *StoreSqlite) GetValue(k string) *string {
	res, err := s.GetStmt.Query(k)
	gotils.CheckNotFatal(err)
	if err != nil {
		return nil
	}
	defer res.Close()
	for res.Next() {
		var k string
		var v string
		var t string
		err = res.Scan(&k, &v, &t)
		gotils.CheckNotFatal(err)
		if err != nil {
			return nil
		}
		return &v
	}
	return nil
}

// GetValueAsJSON get the value for the given k into o
func (s *StoreSqlite) GetValueAsJSON(k string, o interface{}) error {
	res, err := s.GetStmt.Query(k)
	gotils.CheckNotFatal(err)
	if err != nil {
		return err
	}
	defer res.Close()

	for res.Next() {
		var k string
		var v string
		var t string
		err = res.Scan(&k, &v, &t)
		gotils.CheckNotFatal(err)

		if err != nil {
			continue
		}

		err = json.Unmarshal([]byte(v), o)
		gotils.CheckNotFatal(err)

		// the k is a primary key
		break
	}

	return err
}

// IterateByKeyPrefixASC traverse all the items by keyPrefix in ASC order
func (s *StoreSqlite) IterateByKeyPrefixASC(
	keyPrefix string,
	limit int,
	block func(k *string, t *string, v *string, stop *bool)) error {
	var err error
	var res *sql.Rows

	res, err = s.IterateByPrefixASC.Query(keyPrefix, limit)
	gotils.CheckNotFatal(err)

	if err != nil {
		return err
	}

	defer res.Close()
	stop := false
	for res.Next() && false == stop {
		var k string
		var v string
		var t string
		err = res.Scan(&k, &v, &t)
		gotils.CheckNotFatal(err)

		if err != nil {
			break
		}

		block(&k, &t, &v, &stop)
	}

	return err
}

// IterateByKeyPrefixDESC traverse items by key prefix in DESC order
func (s *StoreSqlite) IterateByKeyPrefixDESC(
	keyPrefix string,
	limit int,
	block func(k *string, t *string, v *string, stop *bool)) error {
	var err error
	var res *sql.Rows

	res, err = s.IterateByPrefixDSC.Query(keyPrefix, limit)
	gotils.CheckNotFatal(err)

	if err != nil {
		return err
	}

	defer res.Close()
	stop := false
	for res.Next() && false == stop {
		var k string
		var v string
		var t string
		err = res.Scan(&k, &v, &t)
		gotils.CheckNotFatal(err)

		if err != nil {
			break
		}

		block(&k, &t, &v, &stop)
	}

	return err
}

// Commit execute "COMMIT;" on the sqlite store
func (s *StoreSqlite) Commit() {
	s.Db.Exec("COMMIT;")
}
