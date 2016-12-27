// Package pgmigrate implements a minimalistic migration library for postgres.
// See README for more information.
package pgmigrate

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"
)

var (
	nameRegexp = regexp.MustCompile("^([\\d]+).+.sql$")
)

// LoadMigrations loads all migration files named {{id}}_{{description}}.sql
// inside dirFS and returns them or an error. The returned Migrations are
// guaranteed to be sorted, but no validated.
func LoadMigrations(dirFS http.FileSystem) (Migrations, error) {
	dir, err := dirFS.Open(".")
	if err != nil {
		return nil, err
	}
	files, err := dir.Readdir(0)
	if err != nil {
		return nil, err
	}
	ms := make(Migrations, 0, len(files))
	for _, file := range files {
		m := Migration{Description: file.Name()}
		match := nameRegexp.FindStringSubmatch(m.Description)
		if len(match) != 2 {
			continue
		} else if _, err := fmt.Sscan(match[1], &m.ID); err != nil {
			return nil, fmt.Errorf("bad id: %s: %s", m.Description, err)
		} else if data, err := readFile(dirFS, m.Description); err != nil {
			return nil, fmt.Errorf("could not read migration: %s: %s", m.Description, err)
		} else {
			m.SQL = string(data)
			ms = append(ms, m)
		}
	}
	sort.Sort(ms)
	return ms, nil
}

// readFile returns all data for file in fs, or an error.
func readFile(fs http.FileSystem, name string) ([]byte, error) {
	file, err := fs.Open(name)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(file)
}

// Migration holds a migration
type Migration struct {
	ID          int
	Description string
	SQL         string
}

// Valid returns an error if the migration is invalid.
func (m *Migration) Valid() error {
	if m.ID < 1 {
		return fmt.Errorf("invalid id: %d", m.ID)
	} else if m.Description == "" {
		return fmt.Errorf("missing description")
	} else if m.SQL == "" {
		return fmt.Errorf("missing sql")
	}
	return nil
}

// Migrations holds a list of migrations sorted by id. The first migration
// needs to have ID 1, and each following ID has to be incremented by 1.
type Migrations []Migration

// Less is part of the sort.Interface.
func (m Migrations) Less(i, j int) bool {
	return m[i].ID < m[j].ID
}

// Swap is part of the sort.Interface.
func (m Migrations) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

// Len is part of the sort.Interface.
func (m Migrations) Len() int {
	return len(m)
}

// Valid returns an error if m holds an invalid migration list.
func (m Migrations) Valid() error {
	for i := 0; i < len(m); i++ {
		if m[i].ID != i+1 {
			return fmt.Errorf("unexpected migration id: got=%d want=%d", m[i].ID, i+1)
		} else if err := m[i].Valid(); err != nil {
			return fmt.Errorf("invalid migration %d: %s", m[i].ID, err)
		}
	}
	return nil
}

// IDs returns a slice with all migration IDs in m. This is useful for logging
// which migrations have been executed.
func (m Migrations) IDs() []int {
	var ids []int
	for _, mm := range m {
		ids = append(ids, mm.ID)
	}
	return ids
}

// DefaultConfig should be used by most users.
var DefaultConfig = Config{
	Schema: "migrations",
	Table:  "migrations",
}

// Config allows to customize pgmigrate. However, most users should use the
// DefaultConfig.
type Config struct {
	// Schema is the name of the postgres schema the migrations table is stored in.
	Schema string
	// Table is the name of the migrations table.
	Table string
}

// Migrate validates ms, and on success applies any ms that has not already
// been executed. The return value is either an error, or a list of all
// migrations that were applied.
func (c *Config) Migrate(db *sql.DB, ms Migrations) (Migrations, error) {
	if err := ms.Valid(); err != nil {
		return nil, err
	}
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := c.init(tx); err != nil {
		return nil, err
	} else if ms, err = c.verifyMigrations(tx, ms); err != nil {
		return nil, err
	} else {
		return c.applyMigrations(tx, ms)
	}
}

// init initializes the migrations schema and table if it does not exist yet.
func (c *Config) init(tx *sql.Tx) error {
	sql := `
CREATE SCHEMA IF NOT EXISTS ` + quoteIdentifier(c.Schema) + `;
CREATE TABLE IF NOT EXISTS ` + c.table() + ` (
  id int NOT NULL,
	description text NOT NULL,
	sql text NOT NULL,
	duration interval NOT NULL,
  created timestamp without time zone DEFAULT (now() AT TIME ZONE 'UTC') NOT NULL
);
`
	_, err := tx.Exec(sql)
	return err
}

// table returns the schema qualified and quoted table name.
func (c *Config) table() string {
	return quoteIdentifier(c.Schema) + "." + quoteIdentifier(c.Table)
}

// verifyMigrations verifies that the db contains an umodified subset of ms
// and returns the migrations that have not yet been applied or an error.
func (c *Config) verifyMigrations(tx *sql.Tx, ms Migrations) (Migrations, error) {
	sql := "SELECT id, description, sql FROM " + c.table() + " ORDER BY id ASC"
	rows, err := tx.Query(sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var dbM Migration
		if err := rows.Scan(&dbM.ID, &dbM.Description, &dbM.SQL); err != nil {
			return nil, err
		}
		if len(ms) == 0 {
			return nil, fmt.Errorf("unknown migration %d in db", dbM.ID)
		} else if dbM != ms[0] {
			return nil, fmt.Errorf("modified migration %d detected", dbM.ID)
		}
		ms = ms[1:]
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ms, nil
}

// applyMigrations applies ms to the db and returns them or an erorr.
func (c *Config) applyMigrations(tx *sql.Tx, ms Migrations) (Migrations, error) {
	sql := "INSERT INTO " + c.table() + " (id, description, sql, duration) VALUES ($1, $2, $3, $4)"
	for _, m := range ms {
		start := time.Now()
		if _, err := tx.Exec(m.SQL); err != nil {
			return nil, fmt.Errorf("%d %s: %s", m.ID, m.Description, err)
		}
		duration := time.Since(start).Seconds()
		if _, err := tx.Exec(sql, m.ID, m.Description, m.SQL, duration); err != nil {
			return nil, fmt.Errorf("%d %s: %s", m.ID, m.Description, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	} else {
		return ms, nil
	}
}

// quoteIdentifier quotes name to be used as an identifier in a postgres SQL
// query. The implementation is copied from lib/pq.
func quoteIdentifier(name string) string {
	end := strings.IndexRune(name, 0)
	if end > -1 {
		name = name[:end]
	}
	return `"` + strings.Replace(name, `"`, `""`, -1) + `"`
}
