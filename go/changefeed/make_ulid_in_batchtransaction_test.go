package changefeed

import (
	"context"
	"database/sql"
	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestUlidInBatchTransaction(t *testing.T) {
	timeHint, err := time.Parse(time.RFC3339, "2023-01-02T15:04:05Z")
	require.NoError(t, err)

	var ulidHigh []byte
	var ulidLow int64
	var ulidFull ulid.ULID
	var outTime time.Time
	tx, err := fixture.DB.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
	require.NoError(t, err)

	_, err = tx.Exec(`[changefeed].make_ulid_in_batch_transaction`,
		sql.Named("feed_id", "28d74278-ddb9-11ed-bcc5-23a7efd30b00"),
		sql.Named("shard_id", 0),
		sql.Named("count", 100),
		sql.Named("time_hint", timeHint),
		sql.Named("random_bytes", []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}),
		sql.Named("time", sql.Out{Dest: &outTime}),
		sql.Named("ulid", sql.Out{Dest: &ulidFull}),
		sql.Named("ulid_high", sql.Out{Dest: &ulidHigh}),
		sql.Named("ulid_low", sql.Out{Dest: &ulidLow}),
	)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Check that we get the timestamp generation right by comparing with a Go implementation
	referenceUlid, err := ulid.New(ulid.Timestamp(timeHint), nil)
	require.NoError(t, err)
	assert.Equal(t, referenceUlid[:6], ulidFull[:6])
	assert.Equal(t, referenceUlid[:6], ulidHigh[:6]) // remaining bytes are random...
	assert.Equal(t, outTime.In(time.UTC), timeHint)

	// It is possible to add 2^62 to the ulidHigh in mssql without overflow, even
	// if random_bytes was 0xfff.
	var result int64
	require.NoError(t, fixture.DB.QueryRow(`select @p1 + @p2`, ulidLow, 0x4000000000000000).Scan(&result))
	var expected uint64 = 0xbfffffffffffffff + 0x4000000000000000
	assert.Equal(t, int64(expected), result)

	// Generate the *last* ULID in the count range according to protocol
	var lastUlidOf1stTxn ulid.ULID
	require.NoError(t, fixture.DB.QueryRow(`select @p1 + convert(binary(8), @p2)`,
		ulidHigh, ulidLow,
	).Scan(&lastUlidOf1stTxn))

	// 2nd invocation with same time_hint; the ULID returned should be the one following
	// right after

	tx, err = fixture.DB.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
	require.NoError(t, err)

	var firstUlidOf2ndTxn ulid.ULID
	_, err = tx.Exec(`[changefeed].make_ulid_in_batch_transaction`,
		sql.Named("feed_id", "28d74278-ddb9-11ed-bcc5-23a7efd30b00"),
		sql.Named("shard_id", 0),
		sql.Named("count", 100),
		sql.Named("time_hint", timeHint),
		sql.Named("ulid", sql.Out{Dest: &firstUlidOf2ndTxn}),
	)
	assert.Equal(t, "01GNSG5PM8ZZZVZZZZZZZZZZZZ", lastUlidOf1stTxn.String())
	assert.Equal(t, "01GNSG5PM8ZZZW000000000033", firstUlidOf2ndTxn.String())
	// so, first ULID of 1st transaction ends in 0xff; then it wraps to 0x00 which is the 2nd,
	// and so on. So, the 100th is 98 (!), so the first one of the following transaction is 99.
	assert.Equal(t, uint8(99), firstUlidOf2ndTxn[15])
	require.NoError(t, err)

}
