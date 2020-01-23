# gokvstore

Key Value stores built with relational DBs: Postgres, SQLite, (MySQL coming soon)

## Builds

[![Build Status](https://travis-ci.org/korovkin/gokvstore.svg)](https://travis-ci.org/korovkin/gokvstore)

## Examples:

```
  import "github.com/korovkin/gokvstore"

	s, err := gokvstore.NewStoreSqlite("kv_test", ".")
	defer s.Close()

  // Add Key, Value, Tag:
	s.AddValueKVT("kk", "33", "t")

  // Add Key JSON(Value), Tag:
  m := map[string]interface{}{
    "name": "superman",
  }
  s.AddValueAsJSON("superman", "t", m)

```

### SQLite: 
  
  store_sqlite_test.go

###  Postgres:
  
  store_postgres_test.go

