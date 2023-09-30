package changefeed

import (
	"context"
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vippsas/mssql-changefeed/go/changefeed/sqltest"
	"testing"
)

func TestHappyDaySerializeWriters(t *testing.T) {
	_, err := fixture.DB.ExecContext(context.Background(),
		`exec [changefeed].setup_feed 'myservice.TestSerializeWriters', @serialize_writers=1`)
	require.NoError(t, err)

	_, err = fixture.DB.Exec(`

	begin transaction
	
	declare @now datetime2(3) = '2023-09-30'

	-- this will cause the insert into state table
	exec [changefeed].[lock:myservice.TestSerializeWriters] @shard_id = 0, @time_hint = @now
	
	insert into myservice.TestSerializeWriters(EventID, Data)
	values
	    ([changefeed].ulid(0), 'a'),
	 	([changefeed].ulid(1), 'b');
	
	-- this will cause update in state table
	exec [changefeed].[lock:myservice.TestSerializeWriters] @shard_id = 0, @time_hint = @now

	insert into myservice.TestSerializeWriters(EventID, Data)
	values
	    ([changefeed].ulid(0), 'c');
	
	-- to verify the update of state table
	exec [changefeed].[lock:myservice.TestSerializeWriters] @shard_id = 0, @time_hint = @now

	insert into myservice.TestSerializeWriters(EventID, Data)
	values
	     ([changefeed].ulid(0), 'd');
	
	-- new timestamp (default time)
	exec [changefeed].[lock:myservice.TestSerializeWriters] @shard_id = 0

	insert into myservice.TestSerializeWriters(EventID, Data)
	values
	     ([changefeed].ulid(0), 'e');

	commit

`)
	require.NoError(t, err)

	type Row struct {
		EventID []byte
	}

	ulids, err := sqltest.StructSlice2[Row](context.Background(), fixture.DB, `select EventID from myservice.TestSerializeWriters order by Data`)
	require.NoError(t, err)
	assert.Equal(t, 5, len(ulids))

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

}
