package changefeed

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestIntegerConversionMssqlAndGo(t *testing.T) {
	// Just an experiment, not something that will/should regress
	ctx := context.Background()

	var minusOne int64
	err := fixture.DB.QueryRowContext(ctx, `select convert(bigint, 0xffffffffffffffff)`).Scan(&minusOne)
	require.NoError(t, err)

	assert.Equal(t, "ffffffffffffffff", fmt.Sprintf("%x", uint64(minusOne)))
}

func TestHappyDay(t *testing.T) {
	ctx := context.Background()

	timeHint := time.Now()

	for k := 0; k != 2; k++ {

		tx, err := fixture.DB.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
		require.NoError(t, err)

		_, err = tx.ExecContext(context.Background(), `[changefeed].init_ulid`,
			sql.Named("table", "dbo.MyEvent"),
			sql.Named("shard_id", 0),
			sql.Named("timeout", -1),
			sql.Named("time_hint", timeHint),
		)
		require.NoError(t, err)

		for i := 0; i != 3; i++ {
			var eventULID ulid.ULID
			err := tx.QueryRowContext(ctx, `select [changefeed].get_ulid(@p1)`, i).Scan(&eventULID)
			require.NoError(t, err)
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
}

/*
func TestTransactionWrappers(t *testing.T) {
	ctx := context.Background()
	timeHint, err := time.Parse(time.RFC3339, "2023-01-02T15:04:05Z")

	feedID := uuid.Must(uuid.FromString("3783faf0-e336-11ed-873f-7fc575a39d77"))
	shardID := 242

	tx, err := BeginTransaction(fixture.DB, context.Background(), feedID, shardID, &TransactionOptions{TimeHint: timeHint})
	require.NoError(t, err)
	// This is the transaction where we inserted the shard_ulid; it should be zero-initiatialized
	assert.Equal(t, 0, sqltest.QueryInt(tx, `select ulid_low from changefeed.shard_ulid where feed_id = @p1`, feedID))

	gotTime, err := tx.Time(ctx)
	require.NoError(t, err)
	assert.Equal(t, timeHint, gotTime)

	ulid1, err := tx.NextULID(ctx)
	require.NoError(t, err)
	ulid2, err := tx.NextULID(ctx)
	require.NoError(t, err)
	ulid3, err := tx.NextULID(ctx)
	require.NoError(t, err)
	assert.True(t, ulidToInt(ulid1)+1 == ulidToInt(ulid2))
	assert.True(t, ulidToInt(ulid2)+1 == ulidToInt(ulid3))

	err = tx.Commit()
	require.NoError(t, err)

	// Check state committed to DB
	var gotUlidHigh []byte
	var gotUlidLow int64
	require.NoError(t, fixture.DB.QueryRowContext(context.Background(),
		`select ulid_high, ulid_low, time from changefeed.shard_ulid where feed_id = @p1 and shard_id = @p2`,
		feedID, shardID).Scan(&gotUlidHigh, &gotUlidLow, &gotTime))
	assert.True(t, ulidToInt(ulid3)+1 == uint64(gotUlidLow))
	assert.Equal(t, gotUlidHigh[:], ulid3[0:8])
	assert.Equal(t, timeHint, gotTime)

	// Continue next transaction from *same* timestamp -- should continue counting on the same range
	tx, err = BeginTransaction(fixture.DB, context.Background(), feedID, shardID, &TransactionOptions{TimeHint: timeHint})
	require.NoError(t, err)
	ulid4a, err := tx.NextULID(ctx)
	require.NoError(t, err)

	// the +2 instead of +1 is because of extra sampling of the sequence ... keeping it instead of
	// correcting as having a bit more space for good measure doesn't hurt
	assert.True(t, ulidToInt(ulid3)+2 == ulidToInt(ulid4a))
	// roll back!
	err = tx.Rollback()
	require.NoError(t, err)

	// Continue again, since we rolled back previously we should continue on the same range
	tx, err = BeginTransaction(fixture.DB, context.Background(), feedID, shardID, &TransactionOptions{TimeHint: timeHint})
	require.NoError(t, err)
	ulid4b, err := tx.NextULID(ctx)
	require.NoError(t, err)
	// +2: see above
	assert.True(t, ulidToInt(ulid3)+2 == ulidToInt(ulid4b))
	// roll back!
	err = tx.Rollback()
	require.NoError(t, err)

}

func TestCommitVsRollback(t *testing.T) {
	// TODO
}
*/
