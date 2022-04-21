package changefeed

import (
	"context"
	"database/sql"
	"fmt"
	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/denisenkom/go-mssqldb/msdsn"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vippsas/mssql-changefeed/sqltest"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
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
	migrationSql, err := ioutil.ReadFile("migrations/from0001/0001.changefeed.sql")
	if err != nil {
		panic(err)
	}
	parts := strings.Split(string(migrationSql), "\ngo\n")
	for _, p := range parts {
		_, err = f.DB.Exec(p)
		if err != nil {
			fmt.Println(p)
			panic(err)
		}
	}
}

func (f *Fixture) Reset(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, discardResult(fixture.DB.ExecContext(ctx, `
delete from changefeed.change;
delete from changefeed.shard;
delete from changefeed.feed;
drop table if exists dbo.event;
alter sequence changefeed.change_id restart with 1000000000000000;
`)))

}

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	dsn := os.Getenv("SQLSERVER_DSN")
	if dsn == "" {
		panic("Must set SQLSERVER_DSN to run tests")
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

	code := m.Run()
	os.Exit(code)
}

func discardResult(r sql.Result, err error) error {
	return err
}

// Simply test that we can connect to a configured test database
func TestDatabaseSetup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var dbname string
	require.NoError(t, fixture.DB.QueryRowContext(ctx, `select db_name()`).Scan(&dbname))
	assert.Equal(t, len("54b10c7d4ea54e538dd1c04bbe75d61c"), len(dbname))
}

func TestSweepHappyDay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	fixture.Reset(t)

	require.NoError(t, discardResult(fixture.DB.ExecContext(ctx, `
insert into changefeed.feed(feed_id, name) values
  (1, 'feed_1'),
  (2, 'feed_2');

insert into changefeed.shard(feed_id, shard, sweep_group) values
  -- one sweep_group for first and last shard here...: 
  (1, 0, 100),
  (1, 1, 101),
  (2, 0, 100);

insert into changefeed.change(feed_id, shard, change_id) values
  (1, 0, next value for changefeed.change_id), -- should be swept
  (1, 0, next value for changefeed.change_id),
  (1, 0, next value for changefeed.change_id),
  (1, 0, next value for changefeed.change_id),
                                                                
  (1, 1, next value for changefeed.change_id), -- should not be swept
  (1, 1, next value for changefeed.change_id),

  (2, 0, next value for changefeed.change_id), -- should be swept
  (2, 0, next value for changefeed.change_id);
`)))

	rows := sqltest.Query(fixture.DB, `changefeed.sweep`, sql.Named("sweep_group", 100))
	// check and patch latency ms as this is unpredicatble
	assert.True(t, rows[0][3].(int) > 0 && rows[0][3].(int) < 1000)
	rows[0][3] = nil
	assert.True(t, rows[1][3].(int) > 0 && rows[1][3].(int) < 1000)
	rows[1][3] = nil

	assert.Equal(t, sqltest.Rows{
		{1, 0, 4, nil},
		{2, 0, 2, nil},
	}, rows)

	// sweep_group=100 assigns to (1,0) and (2,0), but leaves (1,1) alone
	assert.Equal(t, sqltest.Rows{
		{1000000000000000, 2000000000000001},
		{1000000000000001, 2000000000000002},
		{1000000000000002, 2000000000000003},
		{1000000000000003, 2000000000000004},
		{1000000000000004, nil},
		{1000000000000005, nil},
		{1000000000000006, 2000000000000001},
		{1000000000000007, 2000000000000002},
	}, sqltest.Query(fixture.DB, `select change_id, change_sequence_number from changefeed.change order by change_id`))

	// Second invocation should pick up sequence numbers where the first stopped...
	require.NoError(t, discardResult(fixture.DB.ExecContext(ctx, `
insert into changefeed.change(feed_id, shard, change_id) values
  (1, 0, next value for changefeed.change_id),
  (1, 0, next value for changefeed.change_id)
`)))

	rows = sqltest.Query(fixture.DB, `changefeed.sweep`, sql.Named("sweep_group", 100))
	rows[0][3] = nil
	assert.Equal(t, sqltest.Rows{
		{1, 0, 2, nil},
	}, rows)

	assert.Equal(t, sqltest.Rows{
		{1000000000000008, 2000000000000005},
		{1000000000000009, 2000000000000006},
	}, sqltest.Query(fixture.DB, `select change_id, change_sequence_number from changefeed.change where feed_id = 1 and shard = 0 and change_id > 1000000000000003 order by change_id`))

}

func TestSweepRaceProtection(t *testing.T) {
	// Load testing to test that parallel sweeps are OK.
	// Each thread runs for 10 seconds and inserts 10 items, does a sweep, and waits a random time. Then piece
	// together the resutls.
	const N = 20

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	fixture.Reset(t)

	require.NoError(t, discardResult(fixture.DB.ExecContext(ctx, `
insert into changefeed.feed(feed_id, name) values (1, 'feed_1');
insert into changefeed.shard(feed_id, shard, sweep_group) values (1, 0, 100);
`)))

	endTime := time.Now().Add(1 * time.Second)

	total := int64(0)
	races := int64(0)

	var wg sync.WaitGroup
	for i := 0; i != N; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)

			for time.Now().Before(endTime) {
				require.NoError(t, discardResult(fixture.DB.ExecContext(ctx, `
					insert into changefeed.change(feed_id, shard, change_id) values
						 (1, 0, next value for changefeed.change_id),
						 (1, 0, next value for changefeed.change_id),
						 (1, 0, next value for changefeed.change_id),
						 (1, 0, next value for changefeed.change_id),
						 (1, 0, next value for changefeed.change_id),
						 (1, 0, next value for changefeed.change_id),
						 (1, 0, next value for changefeed.change_id),
						 (1, 0, next value for changefeed.change_id),
						 (1, 0, next value for changefeed.change_id),
						 (1, 0, next value for changefeed.change_id);`)))

				time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)

				var feed_id, shard int
				var changeCount, lagMs int64
				err := fixture.DB.QueryRowContext(ctx, `changefeed.sweep`, sql.Named("sweep_group", 100)).Scan(
					&feed_id, &shard, &changeCount, &lagMs)
				if err == nil {
					atomic.AddInt64(&total, changeCount)
					assert.Equal(t, 1, feed_id)
					assert.Equal(t, 0, shard)
				} else if strings.Contains(err.Error(), "race, changefeed.sweep") {
					atomic.AddInt64(&races, 1)
					err = nil
				}
			}
		}()
	}
	wg.Wait()

	// final clean run ...
	var feed_id, shard int
	var changeCount, lagMs int64
	err := fixture.DB.QueryRowContext(ctx, `changefeed.sweep`, sql.Named("sweep_group", 100)).Scan(
		&feed_id, &shard, &changeCount, &lagMs)
	require.NoError(t, err)
	assert.Equal(t, 1, feed_id)
	assert.Equal(t, 0, shard)
	total += changeCount

	// Correct state in the end, but there were race errors and aborted transactions
	assert.Greater(t, races, int64(0))
	assert.Equal(t, 0, sqltest.QueryInt(fixture.DB, `select count(*) from changefeed.change where change_sequence_number is null`))
	assert.Equal(t, int(total), sqltest.QueryInt(fixture.DB, `select count(*) from changefeed.change`))
	// sequence numbers strictly increasing without gaps from 1:
	assert.Equal(t, 0, sqltest.QueryInt(fixture.DB, `
         with seqno_and_expected as (
           select top(100)
             change_sequence_number,
             row_number() over (order by change_sequence_number) + 2000000000000000 as expected

           from changefeed.change
           where
             feed_id = 1 and shard = 0
           order by change_sequence_number
         )
         select count(*) from seqno_and_expected
         where change_sequence_number <> expected
	`))
}

func TestSweepLoop(t *testing.T) {
	for _, longpoll := range []bool{true, false} {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		fixture.Reset(t)

		require.NoError(t, discardResult(fixture.DB.ExecContext(ctx, `
insert into changefeed.feed(feed_id, name) values (1, 'feed_1');
insert into changefeed.shard(feed_id, shard, longpoll) values (1, 0, @p1);
`, longpoll)))

		const defaultWait = 2800
		const defaultDuration = 3000

		type SweepResult struct {
			err         error
			changeCount int
			lagMs       int
			iterations  int
			timePassed  time.Duration
		}

		var wg sync.WaitGroup

		// Inserter thread, this will insert changes for 5 seconds
		inserter := func() {
			defer wg.Done()
			endTime := time.Now().Add(6 * time.Second)
			for time.Now().Before(endTime) {
				err := discardResult(
					fixture.DB.ExecContext(ctx, `
insert into changefeed.change(feed_id, shard, change_id)
values (1, 0, next value for changefeed.change_id)`),
				)
				if err != nil {
					panic(err)
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
		wg.Add(1)
		go inserter()

		var main, blocked1, blocked2, notwaiting, takingover SweepResult

		sweeploop := func(waitDuration int, duration int, r *SweepResult) {
			defer wg.Done()
			t0 := time.Now()

			r.err = discardResult(fixture.DB.ExecContext(ctx, `changefeed.sweep_loop`,
				sql.Named("sweep_group", 0),
				sql.Named("wait_milliseconds", waitDuration),
				sql.Named("duration_milliseconds", duration),
				sql.Named("sleep_milliseconds", 200),
				sql.Named("change_count", sql.Out{Dest: &r.changeCount}),
				sql.Named("max_lag_milliseconds", sql.Out{Dest: &r.lagMs}),
				sql.Named("iterations", sql.Out{Dest: &r.iterations}),
			))

			r.timePassed = time.Now().Sub(t0)
		}

		// Main sweep loop
		wg.Add(1)
		go sweeploop(defaultWait, defaultDuration, &main)

		// Ensure the worker above gets the lock first
		time.Sleep(100 * time.Millisecond)
		// Some other sweep loops at the same time should not be able to do anything.
		wg.Add(4)
		// Block, wait for 2800ms then give up
		go sweeploop(defaultWait, defaultDuration, &blocked1)
		go sweeploop(defaultWait, defaultDuration, &blocked2)
		// Block, give up after 100ms
		go sweeploop(100, defaultDuration, &notwaiting)
		// The 4th one outwaits the first worker and then takes over with assigning cursors
		go sweeploop(defaultWait+3000, defaultDuration+3000, &takingover)

		wg.Wait()

		require.True(t, main.timePassed > 3000*time.Millisecond)
		require.True(t, main.changeCount > 10)
		require.True(t, main.lagMs > 0)
		require.True(t, main.iterations > 10)

		assertBlocked := func(r SweepResult) {
			require.True(t, r.timePassed > 2800*time.Millisecond)
			require.True(t, r.iterations == 0)
			require.True(t, r.changeCount == 0)
			require.True(t, r.lagMs == 0)
		}
		assertBlocked(blocked1)
		assertBlocked(blocked2)

		require.True(t, notwaiting.timePassed < time.Second)
		require.True(t, notwaiting.iterations == 0)
		require.True(t, notwaiting.changeCount == 0)
		require.True(t, notwaiting.lagMs == 0)

		require.True(t, takingover.timePassed > 5000*time.Millisecond)
		require.True(t, takingover.changeCount > 10)
		require.True(t, takingover.lagMs > 0)
		require.True(t, takingover.iterations > 10)

	}
}

func TestLongPoll(t *testing.T) {
	const eventCount = 10

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	fixture.Reset(t)

	err := discardResult(
		fixture.DB.ExecContext(ctx, `
insert into changefeed.feed(feed_id, name) values (1, 'feed_1');
insert into changefeed.shard(feed_id, shard) values (1, 0);
insert into changefeed.shard(feed_id, shard) values (1, 1);

create table dbo.event ( change_id bigint not null, t datetime2 not null );
		`),
	)
	if err != nil {
		panic(err)
	}

	wg.Add(1)
	// Sweep for 10 seconds with 20ms sleeps
	go func() {
		defer wg.Done()
		var changeCount, lagMs, iterations int
		err := discardResult(fixture.DB.ExecContext(ctx, `changefeed.sweep_loop`,
			sql.Named("sweep_group", 0),
			sql.Named("wait_milliseconds", 0),
			sql.Named("duration_milliseconds", 10000),
			sql.Named("sleep_milliseconds", 20),
			sql.Named("change_count", sql.Out{Dest: &changeCount}),
			sql.Named("max_lag_milliseconds", sql.Out{Dest: &lagMs}),
			sql.Named("iterations", sql.Out{Dest: &iterations}),
		))
		require.NoError(t, err)
	}()

	// 10 insert every 200 ms -- 10 sweeps between each insert
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i != eventCount; i++ {
			now := time.Now().UTC()
			require.NoError(t, discardResult(fixture.DB.ExecContext(ctx, `
declare @change_id bigint = next value for changefeed.change_id; 

insert into changefeed.change(feed_id, shard, change_id) values
    (1, 0, @change_id),
    (1, 1, next value for changefeed.change_id);
if @@error <> 0 return;

insert into dbo.event (change_id, t) values (@change_id, @p1)
`,
				now,
			),
			))
			fmt.Println("INSERT ", now, " ", i)
			time.Sleep(300 * time.Millisecond)
		}
	}()

	// Longpoller read...

	type LongpollResult struct {
		eventCount                    int
		iterationCount                int
		sweepLoopNotRunningErrorCount int
		latencies                     []time.Duration
	}

	longpoll := func(result *LongpollResult) {
		defer wg.Done()

		time.Sleep(50 * time.Millisecond)
		t0 := time.Now()
		var seqno int
		require.NoError(t, fixture.DB.QueryRow(`select last_change_sequence_number from changefeed.shard where feed_id = 1 and shard = 0`).Scan(&seqno))

		for time.Now().Sub(t0) < time.Second*15 {
			err = Longpoll(ctx, fixture.DB, 1, 0, time.Second*10, seqno)
			// check err a bit further down

			var nextSeqNo int
			var eventTime time.Time
			require.NoError(t, fixture.DB.QueryRow(`
select s.last_change_sequence_number, e.t from changefeed.shard s
join changefeed.change as c on c.change_sequence_number = s.last_change_sequence_number 
join dbo.event as e on e.change_id = c.change_id
where s.feed_id = 1 and s.shard = 0;
`).Scan(&nextSeqNo, &eventTime))

			latency := time.Now().UTC().Sub(eventTime)
			fmt.Println("UNBLOCK ", nextSeqNo, " latency in ms: ", latency.Milliseconds())

			result.iterationCount++
			if nextSeqNo != seqno {
				result.eventCount++
			}
			if err != nil && strings.Contains(err.Error(), "perhaps changefeed.sweep_loop is not running?") {
				result.sweepLoopNotRunningErrorCount++
			} else {
				require.NoError(t, err)
			}
			if err == nil && nextSeqNo != seqno {
				// the pure happy day case where we immediately unblocked and sweeper is running; in this case
				// only record the latency
				result.latencies = append(result.latencies, latency)
			}
			seqno = nextSeqNo
		}
	}

	wg.Add(5)
	var results [5]LongpollResult
	for i := 0; i != 5; i++ {
		go longpoll(&results[i])
	}

	wg.Wait()

	for _, r := range results {
		fmt.Println(r)
		assert.True(t, r.eventCount >= 9)                    // allow an event to drop during startup
		assert.True(t, r.iterationCount >= 15)               // iterate some time after the sweep loop is done
		assert.True(t, r.sweepLoopNotRunningErrorCount >= 4) // we sit for 5 seconds without a sweeper loop, one error per second
		assert.True(t, len(r.latencies) > 8)
		for _, l := range r.latencies {
			// latencies only recorded for non-error cases; the latency should then always be less than 100 ms
			assert.True(t, l < 200*time.Millisecond)
		}
	}

}
