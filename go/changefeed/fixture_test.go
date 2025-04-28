package changefeed

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/microsoft/go-mssqldb"
	"github.com/gofrs/uuid"
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
	AdminDB    *sql.DB
	UserDB     *sql.DB
	ReadUserDB *sql.DB
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
			_, err = f.AdminDB.Exec(p)
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
		dsn = "sqlserver://localhost?database=master&user id=sa&password=RootPw1"
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

	_, err = adminDb.ExecContext(ctx, `sp_configure 'contained database authentication', 1; `)
	if err != nil {
		panic(err)
	}
	_, err = adminDb.ExecContext(ctx, `declare @sql nvarchar(max) = 'reconfigure'; exec sp_executesql @sql`)
	if err != nil {
		panic(err)
	}

	_, err = adminDb.ExecContext(ctx, fmt.Sprintf(`create database [%s] containment = partial`, dbname))
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

		_ = fixture.AdminDB.Close()
		_ = fixture.UserDB.Close()
		_ = fixture.ReadUserDB.Close()
		_, _ = adminDb.ExecContext(ctx, fmt.Sprintf(`drop database %s`, dbname))
		_ = adminDb.Close()
	}()

	pdsn, _, err := msdsn.Parse(dsn)
	if err != nil {
		panic(err)
	}
	pdsn.Database = dbname
	fmt.Println(pdsn.URL().String())

	fixture.AdminDB, err = sql.Open("sqlserver", pdsn.URL().String())
	if err != nil {
		panic(err)
	}

	fixture.RunMigrations()

	pdsn.User = "myuser"
	pdsn.Password = "UserPw1234"
	fixture.UserDB, err = sql.Open("sqlserver", pdsn.URL().String())
	if err != nil {
		panic(err)
	}

	err = fixture.UserDB.QueryRow(`select object_id('myservice.MyTable')`).Scan(&MyTableObjectID)
	if err != nil {
		panic(err)
	}

	pdsn.User = "myreaduser"
	pdsn.Password = "UserPw1234"
	fixture.ReadUserDB, err = sql.Open("sqlserver", pdsn.URL().String())
	if err != nil {
		panic(err)
	}

	code := m.Run()
	os.Exit(code)
}
