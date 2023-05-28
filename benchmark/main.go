package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/gofrs/uuid"
	_ "github.com/microsoft/go-mssqldb"
	mssql "github.com/microsoft/go-mssqldb"
	_ "github.com/microsoft/go-mssqldb/azuread"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"
)

/*

 */

type StdoutLogger struct {
}

func (s StdoutLogger) Printf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
}

func (s StdoutLogger) Println(v ...interface{}) {
	fmt.Println(v...)
}

var _ mssql.Logger = StdoutLogger{}

func main() {
	//dsn := "sqlserver://127.0.0.1?database=foo&user id=foouser&password=FooPasswd1&log=63"
	dsn := "sqlserver://127.0.0.1?database=foo&user id=foouser&password=FooPasswd1&log=63"
	//dsn := "sqlserver://dagss-benchmark.database.windows.net?database=dagss-benchmark"
	//mssql.SetLogger(StdoutLogger{})

	/*connector, err := mssql.NewAccessTokenConnector(dsn, func() (string, error) {
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return "", err
		}
		opts := policy.TokenRequestOptions{Scopes: []string{"https://database.windows.net/"}}
		tk, err := cred.GetToken(context.Background(), opts)
		if err != nil {
			return "", err
		}
		return tk.Token, nil
	})
	if err != nil {
		panic(err)
	}

	dbi := sql.OpenDB(connector)*/
	dbi, err := sql.Open("sqlserver", dsn)
	if err != nil {
		panic(err)
	}
	dbi.SetMaxOpenConns(100)
	dbi.SetMaxIdleConns(100)

	ctx := context.Background()

	var dbname string
	err = dbi.QueryRow(`select db_name()`).Scan(&dbname)
	if err != nil {
		panic(err)
	}
	fmt.Println("Got link to " + dbname)

	if os.Args[1] == "insert" {
		insert(ctx, dbi)
	} else if os.Args[1] == "setup" {
		setup(ctx, dbi)
	} else if os.Args[1] == "sweep" {
		sweep(ctx, dbi)
	} else if os.Args[1] == "listen" {
		listen(ctx, dbi)
	} else if os.Args[1] == "listentime" {
		listentime(ctx, dbi)
	} else {
		panic("unknown arg")
	}

}

func listen(ctx context.Context, dbi *sql.DB) {
	var lastprint time.Time
	var cursor []uint8
	cursor = make([]uint8, 16)

	for {
		var t time.Time
		var count int
		err := dbi.QueryRowContext(ctx, `
create table #changefeed_read_result (
    ULID binary(16) not null,
    AggregateID uniqueidentifier not null,
    Sequence int not null
);

exec changefeed.read_feed @shard = 0, @cursor = @cursor;

select isnull(max(e.Time), '1970-01-01'), isnull(max(r.ULID), 0x0), count(*)
from #changefeed_read_result r
join myservice.Event as e 
    on e.AggregateID = r.AggregateID and e.Sequence = r.Sequence;
`,
			sql.Named("cursor", cursor),
		).Scan(&t, &cursor, &count)
		if err != nil {
			fmt.Println(err)
			continue
		}
		if count == 0 {
			continue
		}
		if t.Sub(lastprint) > time.Second {
			fmt.Printf("Latency: %d ms\n", time.Now().UTC().Sub(t).Milliseconds())
			lastprint = t
		}
	}

}

func listentime(ctx context.Context, dbi *sql.DB) {
	// just sample and check the latency of the head nice and slow
	var lastcid int
	for {
		var t time.Time
		var cid int
		err := dbi.QueryRowContext(ctx, `
select top(1) EventID, Time
from benchmark.Event
order by Time desc
`).Scan(&cid, &t)
		if err != nil {
			fmt.Println(err)
			continue
		}
		if cid != lastcid {
			fmt.Printf("Latency: %d ms\n", time.Now().UTC().Sub(t).Milliseconds())
			lastcid = cid
		}
		//time.Sleep( * time.Millisecond)
	}

}

func sweep(ctx context.Context, dbi *sql.DB) {
	var count int
	var maxLag int
	var iterations int
	for {
		err := dbi.QueryRow(sweepSql,
			sql.Named("sweep_group", 0),
			sql.Named("wait_milliseconds", 20000),
			sql.Named("sleep_milliseconds", 1),
			sql.Named("duration_milliseconds", 5000),
			//sql.Named("change_count", sql.Out{Dest: &count}),
			//sql.Named("max_lag_milliseconds", sql.Out{Dest: &maxLag}),
			//sql.Named("iterations", sql.Out{Dest: &iterations}),
		).Scan(&count, &maxLag, &iterations)
		if err != nil {
			fmt.Println(err)
			e, ok := err.(mssql.Error)
			if ok {
				fmt.Println("line", e.LineNo)
			}
		} else {
			fmt.Printf("%d %d %d\n", count, maxLag, iterations)
		}
	}
}

func setup(ctx context.Context, dbi *sql.DB) {
	for _, filename := range []string{
		// "../migrations/2001.changefeed-v2.sql",
		"benchmark-setup.sql",
		"2001.changefeed-lazy.sql",
	} {
		migrationSql, err := ioutil.ReadFile(filename)
		if err != nil {
			panic(err)
		}
		parts := strings.Split(string(migrationSql), "\ngo\n")
		for _, p := range parts {
			_, err = dbi.Exec(p)
			if err != nil {
				fmt.Println(p)
				e2, ok := err.(mssql.Error)
				if ok {
					fmt.Println(e2.All)
				}
				panic(err)
			} else {
				fmt.Println("===okvvv")
				fmt.Println(p)
				fmt.Println("===ok^^^")
			}
		}
	}
}

func insert(ctx context.Context, dbi *sql.DB) {
	const NumThreads = 10
	const Sleep = 1 * time.Millisecond

	data := strings.Repeat("a", 100)

	stats := make(chan struct{})

	var wg sync.WaitGroup
	for i := 0; i != NumThreads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				aggregateID := uuid.Must(uuid.NewV4())
				for sequence := 0; sequence != 10; sequence++ {

					func() {
						insertCtx, cancel := context.WithTimeout(ctx, 3000*time.Millisecond)
						defer cancel()

						t0 := time.Now().UTC()
						_, err := dbi.ExecContext(insertCtx, `
insert into myservice.Event(AggregateID, Sequence, Time, Shard, JsonData)
values (@p1, @p2, @p3, @p4, @p5);
`,
							aggregateID, sequence, t0, 0, data,
						)

						if err != nil {
							fmt.Println(err)
							time.Sleep(100 * time.Millisecond)
						} else {
							stats <- struct{}{}
						}

						time.Sleep(Sleep)
					}()

				}
			}

		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		count := 0
		lastCount := 0
		t0 := time.Now()
		for {
			<-stats
			count++
			dt := time.Now().Sub(t0).Milliseconds()
			if dt == 0 {
				continue
			}
			rate := int(int64(count-lastCount) * 1000 / dt)
			if count%1000 == 0 {
				fmt.Printf("count=%d  rate=%d\n", count, rate)
				t0 = time.Now()
				lastCount = count
			}
		}
	}()

	wg.Wait()

}
