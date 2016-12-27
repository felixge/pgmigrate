# pgmigrate

[![GoDoc](https://godoc.org/github.com/felixge/pgmigrate?status.svg)](https://godoc.org/github.com/felixge/pgmigrate)
[![Build Status](https://travis-ci.org/felixge/pgmigrate.svg?branch=master)](https://travis-ci.org/felixge/pgmigrate)

pgmigrate implements a minimalistic migration library for postgres.

## Example Usage

```go
db, _ := sql.Open("postgres", "postgres://localhost/mydb")
ms, _ := pgmigrate.LoadMigrations(http.Dir("path/to/migrations"))
pgmigrate.DefaultConfig.Migrate(db, ms)
```

## Why this package exists

There are a number of decent database migration libraries available for Go,
but I found them to be doing too much in some cases, and too little in others.

Specifically pgmigrate is different because:

* **Only works for postgres:** This keeps the code base small and allows
  leveraging postgres specific features.
* **Executes all migrations in a single transaction:** This avoids problems
  with partially applied migrations.
* **Verifies previously executed migrations have not been modified:** This
  reduces the change of different environments ending up with different
  schemas.
* **Does not support down migrations:** This might be controversial, but I
  don't find them very useful. If a database change needs to be rolled back,
  this can be accomplished by pushing another up migration.
* **No external dependencies:** Some other libs force you to transitively
  depend on client libraries for all the databases they support.
* **Configurable schema/table:** Gives you control over where your migration
  data is stored.
* **Does not ship with a command line client:** IMO there are just too many
  integration scenarios to make a CLI that works for everybody.
* **Supports loading migrations from a virtual `http.FileSystem`:** This works
  well when using certain libraries that allow bundling static files into your
  Go binary.

If the tradeoffs above don't work for you, you're probably better off with one
of the other libraries.

## License

MIT
