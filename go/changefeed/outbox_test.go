package changefeed

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vippsas/mssql-changefeed/go/changefeed/sqltest"

	_ "github.com/denisenkom/go-mssqldb"
)

// Simply test that we can connect to a configured test database
func TestDatabaseSetup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var dbname string
	require.NoError(t, fixture.AdminDB.QueryRowContext(ctx, `select db_name()`).Scan(&dbname))
	assert.Equal(t, len("54b10c7d4ea54e538dd1c04bbe75d61c"), len(dbname))
}

func TestIntegerConversionMssqlAndGo(t *testing.T) {
	// Just an experiment, not something that will/should regress
	ctx := context.Background()

	var minusOne int64
	err := fixture.AdminDB.QueryRowContext(ctx, `select convert(bigint, 0xffffffffffffffff)`).Scan(&minusOne)
	require.NoError(t, err)

	assert.Equal(t, "ffffffffffffffff", fmt.Sprintf("%x", uint64(minusOne)))
}

func TestHappyDayOutbox(t *testing.T) {
	_, err := fixture.AdminDB.ExecContext(context.Background(), `
exec [changefeed].setup_feed 'myservice.TestHappyDay', @outbox = 1;
alter role [changefeed.writers:myservice.TestHappyDay] add member myuser;
alter role [changefeed.readers:myservice.TestHappyDay] add member myreaduser;
`)
	require.NoError(t, err)

	_, err = fixture.UserDB.ExecContext(context.Background(), `
insert into myservice.TestHappyDay (AggregateID, Version, Data) values 
	(1000, 1, '1000-1'),
	(1000, 2, '1000-2'),
	(1000, 3, '1000-3'),
	(1001, 1, '1001-1'),
	(1001, 2, '1001-2');

-- Note: We insert in the "right" order for aggregate versions, but the time_hint is
-- going in the wrong direction for the first cases
insert into [changefeed].[outbox:myservice.TestHappyDay] (shard_id, time_hint, AggregateID, Version) values (0, '2023-05-31 12:00:00', 1000, 1);
insert into [changefeed].[outbox:myservice.TestHappyDay] (shard_id, time_hint, AggregateID, Version) values (0, '2023-05-31 12:03:00', 1001, 1);
insert into [changefeed].[outbox:myservice.TestHappyDay] (shard_id, time_hint, AggregateID, Version) values (0, '2023-05-31 12:02:00', 1000, 2);
insert into [changefeed].[outbox:myservice.TestHappyDay] (shard_id, time_hint, AggregateID, Version) values (0, '2023-05-31 12:01:00', 1001, 2);
insert into [changefeed].[outbox:myservice.TestHappyDay] (shard_id, time_hint, AggregateID, Version) values (0, '2023-05-31 12:10:00', 1000, 3);
`)
	require.NoError(t, err)

	// First consume head of feed while checking that paging works
	page1 := sqltest.Query(fixture.ReadUserDB, `
	create table #read (
		ulid binary(16) not null,
		AggregateID bigint not null,
		Version int not null	    
	);
	exec [changefeed].[read_feed:myservice.TestHappyDay] 0, 0x0, @pagesize = 3;
	select * from #read order by ulid;
`)
	// check prefix of ULID has right timestamp
	hexUlidToTime := func(s string) string {
		buf, err := hex.DecodeString(s[2:])
		require.NoError(t, err)
		var u ulid.ULID
		copy(u[:], buf)
		return time.Unix(int64(u.Time())/1000, 0).UTC().Format(time.RFC3339)
	}

	assert.Equal(t, 3, len(page1))
	// check that time_hint didn't get honored but time component of ULID shifted to maintain the order
	assert.Equal(t, "2023-05-31T12:00:00Z", hexUlidToTime(page1[0][0].(string)))
	assert.Equal(t, "2023-05-31T12:03:00Z", hexUlidToTime(page1[1][0].(string)))
	assert.Equal(t, "2023-05-31T12:03:00Z", hexUlidToTime(page1[2][0].(string)))

	page2 := sqltest.Query(fixture.ReadUserDB, `
	create table #read (
		ulid binary(16) not null,
		AggregateID bigint not null,
		Version int not null	    
	);
	declare @cursorbin binary(16) = convert(binary(16), @cursor)
	exec [changefeed].[read_feed:myservice.TestHappyDay] 0, @cursorbin, @pagesize = 100;
	select * from #read order by ulid;
`, sql.Named("cursor", page1[1][0]))

	assert.Equal(t, 5, len(page1)+len(page2))
	// the :03 is carried over through shard_state, replacing time_hint that was :00
	assert.Equal(t, "2023-05-31T12:03:00Z", hexUlidToTime(page2[0][0].(string)))
	assert.Equal(t, "2023-05-31T12:10:00Z", hexUlidToTime(page2[1][0].(string)))

	// Check that table sizes are what we expect
	assert.Equal(t, 0, sqltest.QueryInt(fixture.AdminDB, `select count(*) from [changefeed].[outbox:myservice.TestHappyDay]`))
	assert.Equal(t, 5, sqltest.QueryInt(fixture.AdminDB, `select count(*) from [changefeed].[feed:myservice.TestHappyDay]`))

	// Do a re-read of feed from start, skipping the ULIDs to get predictable data for assertion
	allEvents := sqltest.Query(fixture.ReadUserDB, `
	create table #read (
		ulid binary(16) not null,
		AggregateID bigint not null,
		Version int not null	    
	);
	exec [changefeed].[read_feed:myservice.TestHappyDay] 0, 0x0, 100;
	select AggregateID, Version from #read order by ulid;
`, sql.Named("cursor", page1[1][0]))

	// this assertion is less trivial than it looks like, since the time_hint is ordered *in reverse*,
	assert.Equal(t, sqltest.Rows{
		{1000, 1},
		{1001, 1},
		{1000, 2},
		{1001, 2},
		{1000, 3},
	}, allEvents)

}

type outboxLoadTest struct {
	description                                     string
	writerCount, readerCount, eventCountPerThread   int
	writeInParallel, readInParallel, readAfterWrite bool
}

func (tc outboxLoadTest) Run(t *testing.T) {
	_, err := fixture.AdminDB.ExecContext(context.Background(), `
truncate table [changefeed].[feed:myservice.TestLoadOutbox];
truncate table [changefeed].[outbox:myservice.TestLoadOutbox];
truncate table [changefeed].[state:myservice.TestLoadOutbox];
`)
	require.NoError(t, err)

	var writerErr, readerErr error
	var wg sync.WaitGroup
	wg.Add(tc.writerCount)

	// Run writerCount threads in parallel to write new events

	writeThread := func(ithread int) {
		defer wg.Done()
		for j := 0; j != tc.eventCountPerThread; j++ {
			_, err := fixture.UserDB.ExecContext(context.TODO(), `
insert into [changefeed].[outbox:myservice.TestLoadOutbox] (shard_id, time_hint, AggregateID, Version) 
values (0, sysutcdatetime(), @p1, @p2);`, ithread, j)
			if err != nil {
				writerErr = err
				return
			}
		}
	}
	for i := 0; i != tc.writerCount; i++ {
		if tc.writeInParallel {
			go writeThread(i)
		} else {
			writeThread(i)
		}
	}

	if tc.readAfterWrite {
		wg.Wait()
		wg = sync.WaitGroup{}
	}

	readThread := func(ithread int) {
		defer wg.Done()

		type Row struct {
			ULID        string
			AggregateID int
			Version     int
		}

		events := make([][]int, tc.writerCount)
		var cursor string
		cursor = "0x00"

		done := 0
		sameCursorCount := 0
		for done < tc.writerCount {
			if readerErr != nil {
				return
			}

			previousCursor := cursor

			rows, err := sqltest.StructSlice2[Row](context.TODO(), fixture.ReadUserDB, `
			create table #read (
				ulid binary(16) not null,
				AggregateID bigint not null,
				Version int not null	    
			);
			declare @cursorBytes binary(16) = convert(binary(16), @cursor, 1)
			exec [changefeed].[read_feed:myservice.TestLoadOutbox] 0, @cursorBytes, @pagesize = 10;
			select convert(varchar(max), ulid, 1) as ULID, AggregateID, Version from #read order by ulid;
		`, sql.Named("cursor", cursor))
			if err != nil {
				readerErr = err
				return
			}
			for _, r := range rows {
				events[r.AggregateID] = append(events[r.AggregateID], r.Version)
				if len(events[r.AggregateID]) > 1 {
					lst := events[r.AggregateID]
					if lst[len(lst)-1] != lst[len(lst)-2]+1 {
						readerErr = fmt.Errorf("%03d: did not consume events in order", ithread)
						fmt.Printf("==== ERROR in %03d: Not consuming events in order, got %d after %d, cursor is %s\n",
							ithread,
							lst[len(lst)-1],
							lst[len(lst)-2],
							cursor)
						return
					}
				}
				if len(events[r.AggregateID]) == tc.eventCountPerThread {
					done++
				}
				cursor = r.ULID
			}

			if previousCursor == cursor {
				// no progress... are we waiting for writers or are we truly done?
				feedSize := sqltest.QueryInt(fixture.AdminDB, `select count(*) from changefeed.[feed:myservice.TestLoadOutbox]`)
				if feedSize == tc.writerCount*tc.eventCountPerThread {
					sameCursorCount++
					if sameCursorCount > 10 {
						readerErr = fmt.Errorf("%03d: stuck at end of feed without having consumed all events", ithread)
						fmt.Println(events[0])
						return
					}
				}
			} else {
				sameCursorCount = 0
			}

			// Useful debugging code:

			outboxSize := sqltest.QueryInt(fixture.AdminDB, `select count(*) from changefeed.[outbox:myservice.TestLoadOutbox]`)
			feedSize := sqltest.QueryInt(fixture.AdminDB, `select count(*) from changefeed.[feed:myservice.TestLoadOutbox]`)
			maxUlid := sqltest.QueryString(fixture.AdminDB, `select convert(varchar(max), max(ulid), 1) from changefeed.[feed:myservice.TestLoadOutbox]`)
			s := ""
			for i := 0; i != tc.writerCount; i++ {
				s = s + " " + fmt.Sprintf("%d", len(events[i]))
			}
			fmt.Printf("%03d: %s   (cursor=%s, outbox=%d, feed=%d, maxulid=%s)\n", ithread, s, cursor, outboxSize, feedSize, maxUlid)

		}

		// Once everything has been consumed, assert that all events are here and in order...
		for i := 0; i != tc.writerCount; i++ {
			if len(events[i]) != tc.eventCountPerThread {
				readerErr = errors.Errorf("wrong count in events[i]")
				return
			}
			for j := 0; j != tc.eventCountPerThread; j++ {
				if events[i][j] != j {
					readerErr = errors.Errorf("events[i][j] != j")
				}
			}
		}
	}

	// Run readerCount readers. They read all events, bins them by AggregateID, and checks order within each AggregateID
	wg.Add(tc.readerCount)
	for i := 0; i != tc.readerCount; i++ {
		if tc.readInParallel {
			go readThread(i)
		} else {
			readThread(i)
		}
	}

	wg.Wait()

	require.NoError(t, writerErr)
	require.NoError(t, readerErr)

}

func TestLoadOutbox(t *testing.T) {
	_, err := fixture.AdminDB.ExecContext(context.Background(), `
exec [changefeed].setup_feed 'myservice.TestLoadOutbox', @outbox = 1;
alter role [changefeed.writers:myservice.TestLoadOutbox] add member myuser;
alter role [changefeed.readers:myservice.TestLoadOutbox] add member myreaduser;
`)
	require.NoError(t, err)

	for _, tc := range []outboxLoadTest{
		{
			description:         "small fully parallel test",
			writerCount:         10,
			readerCount:         5,
			eventCountPerThread: 100,
			writeInParallel:     true,
			readInParallel:      true,
			readAfterWrite:      false,
		},
		{
			description:         "read in parallel after single bit serial write is done (focus on readers)",
			writerCount:         1,
			readerCount:         20,
			eventCountPerThread: 1000,
			writeInParallel:     false,
			readInParallel:      true,
			readAfterWrite:      true,
		},
		{
			description:         "heavy fully parallel test",
			writerCount:         10,
			readerCount:         10,
			eventCountPerThread: 200,
			writeInParallel:     true,
			readInParallel:      true,
			readAfterWrite:      false,
		},
	} {
		t.Run(tc.description, tc.Run)
	}

}
