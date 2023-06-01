package changefeed

import (
	"context"
	"database/sql"
	"fmt"
	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/denisenkom/go-mssqldb/msdsn"
	"github.com/gofrs/uuid"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"
)

type StdoutLogger struct {
}

func (s StdoutLogger) Printf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
}

func (s StdoutLogger) Println(v ...interface{}) {
	fmt.Println(v...)
}

var _ mssql.Logger = StdoutLogger{}

type Fixture struct {
	DB *sql.DB
}

var fixture = Fixture{}

func (f *Fixture) RunMigrations() {
	for _, filename := range []string{
		"../../migrations/2001.changefeed-v2.sql",
		"testdata/mytable.sql",
	} {
		migrationSql, err := ioutil.ReadFile(filename)
		if err != nil {
			panic(err)
		}
		parts := strings.Split(string(migrationSql), "\ngo\n")
		lineno := 0
		for _, p := range parts {
			_, err = f.DB.Exec(p)
			if err != nil {
				fmt.Println(p)
				err2, ok := err.(mssql.Error)
				if !ok {
					fmt.Println(err)
				} else {
					for _, e := range err2.All {
						fmt.Printf("Line %d: %s\n", lineno+int(e.LineNo), e.Message)
					}
				}
				panic(err)
			}
			lineno += strings.Count(p, "\n") + 2
		}
	}
}

var MyTableObjectID int

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	dsn := os.Getenv("SQLSERVER_DSN")
	if dsn == "" {
		dsn = "sqlserver://localhost?database=master&user id=sa&password=VippsPw1"
		//panic("Must set SQLSERVER_DSN to run tests")
	}
	dsn = dsn + "&log=3"

	mssql.SetLogger(StdoutLogger{})

	var err error
	var adminDb *sql.DB

	adminDb, err = sql.Open("sqlserver", dsn)
	if err != nil {
		panic(err)
	}
	dbname := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")

	_, err = adminDb.ExecContext(ctx, fmt.Sprintf(`create database [%s]`, dbname))
	if err != nil {
		panic(err)
	}
	// These settings are just to get "worst-case" for our tests, since snapshot could interfer
	_, err = adminDb.ExecContext(ctx, fmt.Sprintf(`alter database [%s] set allow_snapshot_isolation on`, dbname))
	if err != nil {
		panic(err)
	}
	_, err = adminDb.ExecContext(ctx, fmt.Sprintf(`alter database [%s] set read_committed_snapshot on`, dbname))
	if err != nil {
		panic(err)
	}

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_ = fixture.DB.Close()
		_, _ = adminDb.ExecContext(ctx, fmt.Sprintf(`drop database %s`, dbname))
		_ = adminDb.Close()
	}()

	pdsn, _, err := msdsn.Parse(dsn)
	if err != nil {
		panic(err)
	}
	pdsn.Database = dbname

	fixture.DB, err = sql.Open("sqlserver", pdsn.URL().String())
	if err != nil {
		panic(err)
	}

	fixture.RunMigrations()

	err = fixture.DB.QueryRow(`select object_id('myservice.MyTable')`).Scan(&MyTableObjectID)
	if err != nil {
		panic(err)
	}

	code := m.Run()
	os.Exit(code)
}
