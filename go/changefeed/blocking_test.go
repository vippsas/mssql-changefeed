package changefeed

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vippsas/mssql-changefeed/go/changefeed/sqltest"
)

func TestHappyDayBlocking(t *testing.T) {
	_, err := fixture.AdminDB.ExecContext(context.Background(),
		`
exec [changefeed].setup_feed 'myservice.TestSerializeWriters', @blocking=1;
alter role [changefeed.writers:myservice.TestSerializeWriters] add member myuser;
`)
	require.NoError(t, err)

	_, err = fixture.UserDB.Exec(`

	begin transaction
	
	declare @now datetime2(3) = '2023-09-30'

	-- this will cause the insert into state table
	exec [changefeed].[lock:myservice.TestSerializeWriters] @shard_id = 0, @time_hint = @now
	
	insert into myservice.TestSerializeWriters(EventID, Data)
	values
	    ([changefeed].[ulid:myservice.TestSerializeWriters](0), 'a'),
	 	([changefeed].[ulid:myservice.TestSerializeWriters](1), 'b');
	
	-- this will cause update in state table
	exec [changefeed].[lock:myservice.TestSerializeWriters] @shard_id = 0, @time_hint = @now

	insert into myservice.TestSerializeWriters(EventID, Data)
	values
	    ([changefeed].[ulid:myservice.TestSerializeWriters](0), 'c');
	
	-- to verify the update of state table
	exec [changefeed].[lock:myservice.TestSerializeWriters] @shard_id = 0, @time_hint = @now

	insert into myservice.TestSerializeWriters(EventID, Data)
	values
	     ([changefeed].[ulid:myservice.TestSerializeWriters](0), 'd');
	
	-- new timestamp (default time)
	exec [changefeed].[lock:myservice.TestSerializeWriters] @shard_id = 0

	insert into myservice.TestSerializeWriters(EventID, Data)
	values
	     ([changefeed].[ulid:myservice.TestSerializeWriters](0), 'e');
	
	commit

`)
	require.NoError(t, err)

	// Test not using session state, in a new session
	_, err = fixture.UserDB.Exec(`

	begin transaction
	
	declare @ulid binary(16);
	declare @ulid_high binary(8);
	declare @ulid_low bigint;
	exec [changefeed].[lock:myservice.TestSerializeWriters] @shard_id = 0, @session_context = 0,
		@ulid = @ulid output, @ulid_low = @ulid_low output, @ulid_high = @ulid_high output;
	
	insert into myservice.TestSerializeWriters(EventID, Data)
	values
	    (@ulid, 'f'),
	    (@ulid_high + convert(binary(8), @ulid_low + 1), 'g');
	
	commit

`)
	require.NoError(t, err)

	type Row struct {
		EventID []byte
	}

	ulids, err := sqltest.StructSlice2[Row](context.Background(), fixture.AdminDB, `select EventID from myservice.TestSerializeWriters order by Data`)
	require.NoError(t, err)
	assert.Equal(t, 7, len(ulids))

	ints := []uint64{}
	for _, u := range ulids {
		ints = append(ints, binary.BigEndian.Uint64(u.EventID[8:16]))
	}
	assert.Equal(t, ints[0]+1, ints[1])
	assert.Equal(t, ints[0]+100000000000, ints[2])
	assert.Equal(t, ints[0]+200000000000, ints[3])

	assert.Equal(t, ulids[0].EventID[:8], ulids[1].EventID[:8])
	assert.Equal(t, ulids[0].EventID[:8], ulids[2].EventID[:8])
	assert.Equal(t, ulids[0].EventID[:8], ulids[3].EventID[:8])
	assert.Less(t, binary.BigEndian.Uint64(ulids[0].EventID[:8]), binary.BigEndian.Uint64(ulids[4].EventID[:8]))

	// The 2nd session has ULIDs after the 1st one
	assert.LessOrEqual(t, binary.BigEndian.Uint64(ulids[4].EventID[:8]), binary.BigEndian.Uint64(ulids[5].EventID[:8]))
	// The 2 ULIDs from the last session follow each other
	assert.Equal(t, ints[5]+1, ints[6])

}

type blockingLoadTest struct {
	description                                     string
	writerCount, readerCount, eventCountPerThread   int
	writeInParallel, readInParallel, readAfterWrite bool
}

func (tc blockingLoadTest) Run(t *testing.T) {
	_, err := fixture.AdminDB.ExecContext(context.Background(), `
truncate table myservice.TestLoadBlocking;
truncate table [changefeed].[state:myservice.TestLoadBlocking];
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
begin try 
	begin transaction
		exec [changefeed].[lock:myservice.TestLoadBlocking] @shard_id = 0
	
		insert into myservice.TestLoadBlocking(ULID, Thread, Number)
		values (changefeed.[ulid:myservice.TestLoadBlocking](0), @p1, @p2);
	commit
end try
begin catch
    if @@trancount > 0 rollback;
end catch
`, ithread, j)
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
			ULID   string
			Thread int
			Number int
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
			select top(10) convert(varchar(max), ULID, 1) as ULID, Thread, Number from myservice.TestLoadBlocking
			where ULID > convert(binary(16), @cursor, 1);
		`, sql.Named("cursor", cursor))
			if err != nil {
				readerErr = err
				return
			}
			for _, r := range rows {
				events[r.Thread] = append(events[r.Thread], r.Number)
				if len(events[r.Thread]) > 1 {
					lst := events[r.Thread]
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
				if len(events[r.Thread]) == tc.eventCountPerThread {
					done++
				}
				cursor = r.ULID
			}

			if previousCursor == cursor {
				// no progress... are we waiting for writers or are we truly done?
				feedSize := sqltest.QueryInt(fixture.AdminDB, `select count(*) from myservice.TestLoadBlocking`)
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

			s := ""
			for i := 0; i != tc.writerCount; i++ {
				s = s + " " + fmt.Sprintf("%d", len(events[i]))
			}
			fmt.Printf("%03d: %s   (cursor=%s)\n", ithread, s, cursor)

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

func TestLoadBlocking(t *testing.T) {
	_, err := fixture.AdminDB.ExecContext(context.Background(), `
exec [changefeed].setup_feed 'myservice.TestLoadBlocking', @blocking = 1;
alter role [changefeed.writers:myservice.TestLoadBlocking] add member myuser;
`)
	require.NoError(t, err)

	for _, tc := range []blockingLoadTest{
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
