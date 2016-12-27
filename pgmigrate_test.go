package pgmigrate

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	_ "github.com/lib/pq"
)

func TestLoadMigrations(t *testing.T) {
	dir, err := ioutil.TempDir("", "pgmigrate")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	if err := ioutil.WriteFile(filepath.Join(dir, "1_foo.sql"), []byte("SELECT 1"), 0600); err != nil {
		t.Fatal(err)
	} else if err := ioutil.WriteFile(filepath.Join(dir, "2_bar.sql"), []byte("SELECT 2"), 0600); err != nil {
		t.Fatal(err)
	} else if err := ioutil.WriteFile(filepath.Join(dir, "10_sort.sql"), []byte("SELECT 10"), 0600); err != nil {
		t.Fatal(err)
	} else if err := ioutil.WriteFile(filepath.Join(dir, "invalid.sql"), []byte("SELECT 3"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := LoadMigrations(http.Dir(dir))
	if err != nil {
		t.Fatal(err)
	}
	want := Migrations{
		{ID: 1, Description: "1_foo.sql", SQL: "SELECT 1"},
		{ID: 2, Description: "2_bar.sql", SQL: "SELECT 2"},
		{ID: 10, Description: "10_sort.sql", SQL: "SELECT 10"},
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("\ngot: %#v\nwant: %#v\n", got, want)
	}
}

func TestMigrations_sorting(t *testing.T) {
	got := Migrations{{ID: 3}, {ID: 1}, {ID: 2}}
	want := Migrations{{ID: 1}, {ID: 2}, {ID: 3}}
	sort.Sort(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatal("bad sorting")
	}
}

func TestMigrations_IDs(t *testing.T) {
	t.Run("non-empty migrations slice", func(t *testing.T) {
		var (
			m    = Migrations{{ID: 1}, {ID: 2}, {ID: 3}}
			got  = m.IDs()
			want = []int{1, 2, 3}
		)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got=%v want=%v", got, want)
		}
	})

	t.Run("empty migrations slice", func(t *testing.T) {
		var (
			m    Migrations
			got  = m.IDs()
			want []int
		)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got=%v want=%v", got, want)
		}
	})
}

func TestMigrations_valid(t *testing.T) {
	tests := []struct {
		Migrations Migrations
		WantErr    string
	}{
		{
			Migrations{{ID: 2}},
			"unexpected migration id: got=2 want=1",
		},
		{
			Migrations{{ID: 1}},
			"missing description",
		},
		{
			Migrations{{ID: 1, Description: "foo"}},
			"missing sql",
		},
		{
			Migrations{{ID: 1, Description: "1_foo.sql", SQL: "SELECT 1"}},
			"",
		},
		{
			Migrations{
				{ID: 1, Description: "1_foo.sql", SQL: "SELECT 1"},
				{ID: 2, Description: "2_bar.sql", SQL: "SELECT 2"},
			},
			"",
		},
		{
			Migrations{
				{ID: 1, Description: "1_foo.sql", SQL: "SELECT 1"},
				{ID: 3, Description: "3_bar.sql", SQL: "SELECT 3"},
			},
			"unexpected migration id: got=3 want=2",
		},
		{
			Migrations{
				{ID: 1, Description: "1_foo.sql", SQL: "SELECT 1"},
				{ID: 1, Description: "1_bar.sql", SQL: "SELECT 3"},
			},
			"unexpected migration id: got=1 want=2",
		},
	}
	for _, test := range tests {
		gotErr := test.Migrations.Valid()
		if err := checkErr(gotErr, test.WantErr); err != nil {
			t.Error(err)
		}
	}
}

func TestConfig_Migrate(t *testing.T) {
	db, err := sql.Open("postgres", os.Getenv("PG_DSN"))
	if err != nil {
		t.Fatal(err)
	}

	type subTest struct {
		Migrations     Migrations
		WantErr        string
		WantMigrations []int
		WantQuery      string
	}

	var (
		tests = []struct {
			Name     string
			SubTests []subTest
		}{
			{
				Name: "single migration that creates a schema and table",
				SubTests: []subTest{
					{
						Migrations: Migrations{
							{
								1,
								"1_create_schema_and_table.sql",
								"CREATE SCHEMA foo; CREATE TABLE foo.bar();",
							},
						},
						WantQuery:      "SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_schema = 'foo' AND table_name = 'bar')",
						WantMigrations: []int{0},
					},
				},
			},
			{
				Name: "two migrations that create a schema and table executed together",
				SubTests: []subTest{
					{
						Migrations: Migrations{
							{
								1,
								"1_create_schema.sql",
								"CREATE SCHEMA foo;",
							},
							{
								2,
								"2_create_table.sql",
								"CREATE TABLE foo.bar();",
							},
						},
						WantQuery:      "SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_schema = 'foo' AND table_name = 'bar')",
						WantMigrations: []int{0, 1},
					},
				},
			},
			{
				Name: "two migrations that create a schema and table executed sequentially",
				SubTests: []subTest{
					{
						Migrations: Migrations{
							{
								1,
								"1_create_schema.sql",
								"CREATE SCHEMA foo;",
							},
						},
						WantMigrations: []int{0},
					},
					{
						Migrations: Migrations{
							{
								1,
								"1_create_schema.sql",
								"CREATE SCHEMA foo;",
							},
							{
								2,
								"2_create_table.sql",
								"CREATE TABLE foo.bar();",
							},
						},
						WantQuery:      "SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_schema = 'foo' AND table_name = 'bar')",
						WantMigrations: []int{1},
					},
				},
			},
			{
				Name: "unknown migration",
				SubTests: []subTest{
					{
						Migrations: Migrations{
							{
								1,
								"1_create_schema.sql",
								"CREATE SCHEMA foo;",
							},
						},
						WantMigrations: []int{0},
					},
					{
						Migrations: Migrations{},
						WantErr:    "unknown migration",
					},
				},
			},
			{
				Name: "modified migration",
				SubTests: []subTest{
					{
						Migrations: Migrations{
							{
								1,
								"1_create_schema.sql",
								"CREATE SCHEMA foo;",
							},
						},
						WantMigrations: []int{0},
					},
					{
						Migrations: Migrations{
							{
								1,
								"1_create_schema.sql",
								"CREATE SCHEMA bar;",
							},
						},
						WantErr: "modified migration",
					},
				},
			},
		}
		c       = Config{Schema: "public", Table: "migrations"}
		schemas = []string{c.Schema, "foo"}
	)

	for _, test := range tests {
		var (
			dropSQL = "DROP SCHEMA IF EXISTS " + strings.Join(schemas, ", ") + " CASCADE"
		)
		if _, err := db.Exec(dropSQL); err != nil {
			t.Fatalf("%s: %s", test.Name, err)
		}
		for _, subTest := range test.SubTests {
			ms, err := c.Migrate(db, subTest.Migrations)
			if err := checkErr(err, subTest.WantErr); err != nil {
				t.Fatalf("%s: %s", test.Name, err)
			}
			if subTest.WantQuery != "" {
				var gotQuery bool
				if err := db.QueryRow(subTest.WantQuery).Scan(&gotQuery); err != nil {
					t.Fatalf("%s: %s", test.Name, err)
				} else if !gotQuery {
					t.Errorf("%s: got=%t want=true", test.Name, gotQuery)
				}
			}
			if got, want := len(subTest.WantMigrations), len(ms); got != want {
				t.Errorf("%s: unexpected return migration count: got=%d want=%d", test.Name, got, want)
			} else {
				for i, j := range subTest.WantMigrations {
					if i >= len(ms) {
						t.Errorf("%s: missing return miration: %d", test.Name, i)
					} else if j >= len(subTest.Migrations) {
						t.Errorf("%s: invalid return migration reference: %d", test.Name, j)
					} else if ms[i] != subTest.Migrations[j] {
						t.Errorf("unexpected migration")
					}
				}
			}
		}
	}
}

func checkErr(got error, want string) error {
	var gotS string
	if got != nil {
		gotS = got.Error()
	}
	if !strings.Contains(gotS, want) || (gotS != "" && want == "") {
		return fmt.Errorf("got=%q want=%q", gotS, want)
	}
	return nil
}
