package gokvstore

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/korovkin/gotils"

	// importing the pq module
	_ "github.com/lib/pq"
)

// StorePostgres is a KV store based on Postgres DB
type StorePostgres struct {
	Db                   *sql.DB
	Name                 string
	InsertStmt           *sql.Stmt
	GetStmt              *sql.Stmt
	IterateStmt          *sql.Stmt
	IterateAllStmt       *sql.Stmt
	IterateByPrefixASCEQ *sql.Stmt
	IterateByPrefixDSCEQ *sql.Stmt
	DeleteStmt           *sql.Stmt
	DeleteStmtTag        *sql.Stmt
	DeleteStmtTagLT      *sql.Stmt
	DeleteAllStmt        *sql.Stmt
	CountAllStmt         *sql.Stmt
}

// NewStorePostgres allocates a new instance and connected to the store
func NewStorePostgres(name string, connection string, db *sql.DB) (*StorePostgres, error) {
	return NewStorePostgresWithValueType(name, "jsonb", connection, db)
}

// NewStorePostgresWithValueType allocates a new instance and connected to the store
func NewStorePostgresWithValueType(name string, valueType string, connection string, db *sql.DB) (*StorePostgres, error) {
	now := time.Now()
	tableName := "kv_" + name
	defer func() {
		log.Println("NewStorePostgres: table:", tableName, "dt:", time.Since(now))
	}()
	var err error
	store := StorePostgres{}
	store.Name = name

	if db == nil {
		db, err = sql.Open("postgres", connection)
		gotils.CheckNotFatal(err)
	}
	if err != nil {
		return nil, err
	}

	store.Db = db

	_, err = store.Db.Exec(fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (K text primary key, V %s, T text);",
		tableName,
		valueType,
	))
	gotils.CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	_, err = store.Db.Exec(
		fmt.Sprintf(
			"CREATE INDEX IF NOT EXISTS KV_K_%s ON %s (K, T);",
			name,
			tableName,
		))
	gotils.CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	_, err = store.Db.Exec(
		fmt.Sprintf(
			"CREATE INDEX IF NOT EXISTS KV_T_%s ON %s (T, K);",
			name,
			tableName,
		))
	gotils.CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	store.InsertStmt, err = store.Db.Prepare(
		fmt.Sprintf(
			"INSERT INTO %s (K, V, T) VALUES($1, $2, $3) ON CONFLICT (K) DO UPDATE SET V=$2, T=$3",
			tableName,
		))
	gotils.CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	store.GetStmt, err = store.Db.Prepare(
		fmt.Sprintf(
			"SELECT K, V, T FROM %s WHERE K=$1",
			tableName,
		))
	gotils.CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	store.IterateStmt, err = store.Db.Prepare(
		fmt.Sprintf(
			"SELECT K, V, T FROM %s WHERE K<=$1 ORDER BY K DESC LIMIT $2",
			tableName,
		))
	gotils.CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	store.IterateAllStmt, err = store.Db.Prepare(
		fmt.Sprintf(
			"SELECT K, V, T FROM %s ORDER BY K",
			tableName,
		))
	gotils.CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	store.IterateByPrefixASCEQ, err = store.Db.Prepare(
		fmt.Sprintf(
			"SELECT K, V, T FROM %s WHERE K >= $1 ORDER BY K ASC LIMIT $2",
			tableName,
		))
	gotils.CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	store.IterateByPrefixDSCEQ, err = store.Db.Prepare(
		fmt.Sprintf(
			"SELECT K, V, T FROM %s WHERE K <= $1 ORDER BY K DESC LIMIT $2",
			tableName,
		))
	gotils.CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	store.DeleteStmt, err = store.Db.Prepare(
		fmt.Sprintf(
			"DELETE FROM %s WHERE K=$1",
			tableName,
		))
	gotils.CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	store.DeleteStmtTag, err = store.Db.Prepare(
		fmt.Sprintf(
			"DELETE FROM %s WHERE T=$1",
			tableName,
		))
	gotils.CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	store.DeleteStmtTagLT, err = store.Db.Prepare(
		fmt.Sprintf(
			"DELETE FROM %s WHERE T<$1",
			tableName,
		))
	gotils.CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	store.DeleteAllStmt, err = store.Db.Prepare(
		fmt.Sprintf(
			"DELETE FROM %s",
			tableName,
		))
	gotils.CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	store.CountAllStmt, err = store.Db.Prepare(
		fmt.Sprintf(
			"SELECT COUNT(K), MIN(K), MAX(K) FROM %s",
			tableName,
		))
	gotils.CheckNotFatal(err)
	if err != nil {
		return nil, err
	}

	return &store, nil
}

// Close the connection to the store
func (s *StorePostgres) Close() {
	s.InsertStmt.Close()
	s.GetStmt.Close()
	s.IterateStmt.Close()
	s.IterateByPrefixASCEQ.Close()
	s.IterateByPrefixDSCEQ.Close()
	s.DeleteStmt.Close()
	s.DeleteStmtTag.Close()
	s.DeleteStmtTagLT.Close()
	s.DeleteAllStmt.Close()
	s.CountAllStmt.Close()
	s.Db.Close()
	s.Db = nil
}

// AddValueKVT add a (K,V,T) entry to the store
func (s *StorePostgres) AddValueKVT(k string, v string, t string) error {
	_, err := s.InsertStmt.Exec(k, v, t)
	gotils.CheckNotFatal(err)
	return err
}

// AddValueKV add a (K, V) entry to the store
func (s *StorePostgres) AddValueKV(k string, v string) error {
	_, err := s.InsertStmt.Exec(k, v, "")
	gotils.CheckNotFatal(err)
	return err
}

// DeleteValue deletes the given k from the store
func (s *StorePostgres) DeleteValue(k string) error {
	_, err := s.DeleteStmt.Exec(k)
	gotils.CheckNotFatal(err)
	return err
}

// DeleteAllWithTag delete all entries from the store with with the given tag t
func (s *StorePostgres) DeleteAllWithTag(t string) error {
	_, err := s.DeleteStmtTag.Exec(t)
	gotils.CheckNotFatal(err)
	return err
}

// DeleteWhereTagLT delete all entries with tag less than t
func (s *StorePostgres) DeleteWhereTagLT(t string) error {
	_, err := s.DeleteStmtTagLT.Exec(t)
	gotils.CheckNotFatal(err)
	return err
}

// DeleteAll delete all items from the store
func (s *StorePostgres) DeleteAll() error {
	_, err := s.DeleteAllStmt.Exec()
	gotils.CheckNotFatal(err)
	return err
}

// AddValueAsJSON store o under (k, t)
func (s *StorePostgres) AddValueAsJSON(k string, t string, o interface{}) error {
	b, err := json.Marshal(o)
	gotils.CheckNotFatal(err)

	if err == nil {
		_, err = s.InsertStmt.Exec(k, b, t)
		gotils.CheckNotFatal(err)
		return err
	}

	return err
}

// GetValueAsJSON gets the value stored for the key k
func (s *StorePostgres) GetValueAsJSON(k string, o interface{}) error {
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

// CountAll will compute the count, min, max for the store
func (s *StorePostgres) CountAll() (int64, string, string) {
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

// IterateByKeyPrefixASCEQ traverse the stored items by key prefix (ascending)
func (s *StorePostgres) IterateByKeyPrefixASCEQ(
	keyPrefix string,
	limit int,
	block func(k *string, t *string, v *string, stop *bool)) error {
	var err error
	var res *sql.Rows

	res, err = s.IterateByPrefixASCEQ.Query(keyPrefix, limit)
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

// IterateByKeyPrefixDESCEQ traverse the stored items by key prefix (descending)
func (s *StorePostgres) IterateByKeyPrefixDESCEQ(
	keyPrefix string,
	limit int,
	block func(k *string, t *string, v *string, stop *bool)) error {
	var err error
	var res *sql.Rows

	res, err = s.IterateByPrefixDSCEQ.Query(keyPrefix, limit)
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

// IterateAll traverse all the stored items
func (s *StorePostgres) IterateAll(
	o interface{},
	block func(k string, v string, t string, stop *bool)) {
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
		block(k, v, t, &stop)
		if stop {
			break
		}
	}
}

// GetValue get the value for key k
func (s *StorePostgres) GetValue(k string) *string {
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
