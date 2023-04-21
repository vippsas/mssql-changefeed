package changefeed

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"github.com/gofrs/uuid"
	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vippsas/mssql-changefeed/sqltest"
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
	require.NoError(t, fixture.DB.QueryRowContext(ctx, `select db_name()`).Scan(&dbname))
	assert.Equal(t, len("54b10c7d4ea54e538dd1c04bbe75d61c"), len(dbname))
}

func TestInsertShard(t *testing.T) {
	insertShard := func(feedID string, shardID int) {
		_, err := fixture.DB.Exec(`[changefeed].insert_shard`,
			sql.Named("feed_id", feedID),
			sql.Named("shard_id", shardID),
		)
		require.NoError(t, err)
	}
	insertShard("28d74278-ddb9-11ed-bcc5-23a7efd30b00", 0)
	insertShard("28d74278-ddb9-11ed-bcc5-23a7efd30b00", 1)
	insertShard("28d74278-ddb9-11ed-bcc5-23a7efd30b01", 0)

	// Do the same ones again (check idempotency without error)
	insertShard("28d74278-ddb9-11ed-bcc5-23a7efd30b00", 0)
	insertShard("28d74278-ddb9-11ed-bcc5-23a7efd30b00", 1)
	insertShard("28d74278-ddb9-11ed-bcc5-23a7efd30b01", 0)

	assert.Equal(t,
		sqltest.Rows{
			{"28D74278-DDB9-11ED-BCC5-23A7EFD30B00", 0},
			{"28D74278-DDB9-11ED-BCC5-23A7EFD30B00", 1},
			{"28D74278-DDB9-11ED-BCC5-23A7EFD30B01", 0},
		},
		sqltest.Query(fixture.DB, `select convert(varchar(max), feed_id), shard_id from [changefeed].shard_v2 order by feed_id, shard_id`))
}

func TestUlidBlocking(t *testing.T) {
	timeHint, err := time.Parse(time.RFC3339, "2023-01-02T15:04:05Z")
	require.NoError(t, err)

	var ulidPrefix []byte
	var ulidSuffix int64
	var ulidFull ulid.ULID
	var outTime time.Time
	tx, err := fixture.DB.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
	require.NoError(t, err)

	_, err = tx.Exec(`[changefeed].generate_ulid_blocking`,
		sql.Named("feed_id", "28d74278-ddb9-11ed-bcc5-23a7efd30b00"),
		sql.Named("shard_id", 0),
		sql.Named("count", 100),
		sql.Named("time_hint", timeHint),
		sql.Named("random_bytes", []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}),
		sql.Named("time", sql.Out{Dest: &outTime}),
		sql.Named("ulid", sql.Out{Dest: &ulidFull}),
		sql.Named("ulid_prefix", sql.Out{Dest: &ulidPrefix}),
		sql.Named("ulid_suffix", sql.Out{Dest: &ulidSuffix}),
	)
	require.NoError(t, err)

	// Check that we get the timestamp generation right by comparing with a Go implementation
	referenceUlid, err := ulid.New(ulid.Timestamp(timeHint), nil)
	require.NoError(t, err)
	assert.Equal(t, referenceUlid[:6], ulidFull[:6])
	assert.Equal(t, referenceUlid[:6], ulidPrefix[:6]) // remaining bytes are random...
	assert.Equal(t, outTime.In(time.UTC), timeHint)

	// It is possible to add 2^62 to the ulidPrefix in mssql without overflow, even
	// if random_bytes was 0xfff.
	var result int64
	require.NoError(t, tx.QueryRow(`select @p1 + @p2`, ulidSuffix, 0x4000000000000000).Scan(&result))
	var expected uint64 = 0xbfffffffffffffff + 0x4000000000000000
	assert.Equal(t, int64(expected), result)
}

func TestIntegerConversionMssqlAndGo(t *testing.T) {
	// Just an experiment, not something that will/should regress
	ctx := context.Background()

	var minusOne int64
	err := fixture.DB.QueryRowContext(ctx, `select convert(bigint, 0xffffffffffffffff)`).Scan(&minusOne)
	require.NoError(t, err)

	assert.Equal(t, "ffffffffffffffff", fmt.Sprintf("%x", uint64(minusOne)))
}

func ulidToInt(u ulid.ULID) uint64 {
	return binary.BigEndian.Uint64(u[8:16])
}

func TestExample(t *testing.T) {
	ctx := context.Background()
	_, err := fixture.DB.ExecContext(ctx, `
create table dbo.MyEvent (
    MyAggregateID bigint not null, 
    Version int not null,
    Datapoint1 int not null,
    Datapoint2 varchar(max) not null,
    ULID binary(16) not null
);
`)
	require.NoError(t, err)

	feedID := uuid.Must(uuid.FromString("72e4bbb8-dee8-11ed-8496-07598057ad16"))
	shardID := 0

	for k := 0; k != 2; k++ {
		tx, err := BeginTransaction(fixture.DB, context.Background(), feedID, shardID, nil)
		require.NoError(t, err)
		for i := 0; i != 3; i++ {
			eventULID := tx.NextULID()
			fmt.Printf("%s = 0x%x\n", eventULID, [16]byte(eventULID))
			_, err = tx.ExecContext(ctx, `
insert into dbo.MyEvent(MyAggregateID, Version, Datapoint1, Datapoint2, ULID)
values (@i, @k, 42 * @i, 'hello', @ULID)
`,
				sql.Named("i", i),
				sql.Named("k", k),
				sql.Named("ULID", eventULID),
			)
			require.NoError(t, err)
		}
		require.NoError(t, tx.Commit())
	}

	sqltest.QueryDump(fixture.DB, `select * from dbo.MyEvent`)

}

func TestTransactionWrappers(t *testing.T) {
	timeHint, err := time.Parse(time.RFC3339, "2023-01-02T15:04:05Z")
	//var ulidPrefix []byte
	//var ulidSuffix int64
	//var ulidFull ulid.ULID

	feedID := uuid.Must(uuid.FromString("72e4bbb8-dee8-11ed-8496-07598057ad16"))
	shardID := 242

	tx, err := BeginTransaction(fixture.DB, context.Background(), feedID, shardID, &TransactionOptions{TimeHint: timeHint})
	require.NoError(t, err)
	// This is the transaction where we inserted the shard_v2; it should be zero-initiatialized
	assert.Equal(t, 0, sqltest.QueryInt(tx, `select ulid_suffix from changefeed.shard_v2`))

	ulid1 := tx.NextULID()
	ulid2 := tx.NextULID()
	ulid3 := tx.NextULID()
	assert.True(t, ulidToInt(ulid1)+1 == ulidToInt(ulid2))
	assert.True(t, ulidToInt(ulid2)+1 == ulidToInt(ulid3))

	err = tx.Commit()
	require.NoError(t, err)

	// Check state committed to DB
	var gotUlidPrefix []byte
	var gotUlidSuffix int64
	var gotTime time.Time
	require.NoError(t, fixture.DB.QueryRowContext(context.Background(),
		`select ulid_prefix, ulid_suffix, time from changefeed.shard_v2 where feed_id = @p1 and shard_id = @p2`,
		feedID, shardID).Scan(&gotUlidPrefix, &gotUlidSuffix, &gotTime))
	assert.True(t, ulidToInt(ulid3)+1 == uint64(gotUlidSuffix))
	assert.Equal(t, gotUlidPrefix[:], ulid3[0:8])
	assert.Equal(t, timeHint, gotTime)

	// Continue next transaction from *same* timestamp -- should continue counting on the same range
	tx, err = BeginTransaction(fixture.DB, context.Background(), feedID, shardID, &TransactionOptions{TimeHint: timeHint})
	require.NoError(t, err)
	ulid4a := tx.NextULID()

	fmt.Println(ulid3)
	fmt.Println(ulid4a)

	assert.True(t, ulidToInt(ulid3)+1 == ulidToInt(ulid4a))
	// roll back!
	err = tx.Rollback()
	require.NoError(t, err)

	// Continue again, since we rolled back previously we should continue on the same range
	tx, err = BeginTransaction(fixture.DB, context.Background(), feedID, shardID, &TransactionOptions{TimeHint: timeHint})
	require.NoError(t, err)
	ulid4b := tx.NextULID()
	assert.True(t, ulidToInt(ulid3)+1 == ulidToInt(ulid4b))
	// roll back!
	err = tx.Rollback()
	require.NoError(t, err)

}

func TestCommitVsRollback(t *testing.T) {
	// TODO
}
