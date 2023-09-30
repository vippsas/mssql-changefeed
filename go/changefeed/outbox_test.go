package changefeed

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vippsas/mssql-changefeed/go/changefeed/sqltest"
	"testing"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

func discardResult(r sql.Result, err error) error {
	return err
}

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
	declare @cursorbin binary(16) = convert(binary(16), @cursor)
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
