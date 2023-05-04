package changefeed

import (
	"context"
	"database/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vippsas/mssql-changefeed/go/changefeed/sqltest"
	"testing"
	"time"
)

func blockingTransaction(t *testing.T, waitCtx context.Context) error {
	// NOTE: using context.Background() for the actual calls, waitCtx is only for the sleep
	tx, err := fixture.DB.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
	require.NoError(t, err)

	_, err = tx.ExecContext(context.Background(), `[changefeed].lock_shard`,
		sql.Named("object_id", MyTableObjectID),
		sql.Named("shard_id", 0),
		sql.Named("timeout", 1000),
	)

	_, err = tx.ExecContext(context.Background(), `insert into myservice.MyTable(id, value) values (1, 'foo');`)

	<-waitCtx.Done()

	return tx.Commit()
}

func TestKillBlocker(t *testing.T) {
	blockingCtx, blockingCancel := context.WithCancel(context.Background())

	var blockingTransactionError error
	go func() {
		blockingTransactionError = blockingTransaction(t, blockingCtx)
	}()

	time.Sleep(100 * time.Millisecond)

	// launch another one that sits in line...
	var waitingTransactionError error
	go func() {
		waitingTransactionError = blockingTransaction(t, blockingCtx)
	}()

	// wait past the guard for killing waiting sessions
	time.Sleep(200 * time.Millisecond)

	applicationLockCount := func() int {
		return sqltest.QueryInt(fixture.DB, `select count(*) from sys.dm_tran_locks where resource_type = 'APPLICATION'`)
	}

	assert.Equal(t, 2, applicationLockCount())

	var killCount int
	_, err := fixture.DB.ExecContext(context.Background(), `[changefeed].kill_blocking_and_blocked_sessions`,
		sql.Named("object_id", MyTableObjectID),
		sql.Named("shard_id", 0),
		sql.Named("timeout", 1000),
		sql.Named("kill_count", sql.Out{Dest: &killCount}),
	)
	require.NoError(t, err)
	// only 200ms has passed, so transaction is not old enough, did not kill yet
	require.Equal(t, 0, killCount)

	assert.Equal(t, 2, applicationLockCount())

	// pass a lower timeout...
	_, err = fixture.DB.ExecContext(context.Background(), `[changefeed].kill_blocking_and_blocked_sessions`,
		sql.Named("object_id", MyTableObjectID),
		sql.Named("shard_id", 0),
		sql.Named("timeout", 50),
		sql.Named("kill_count", sql.Out{Dest: &killCount}),
	)

	// now it got killed...
	require.Equal(t, 2, killCount)
	assert.Equal(t, 0, applicationLockCount())

	blockingCancel()
	time.Sleep(100 * time.Millisecond)
	assert.Error(t, blockingTransactionError) // first session being killed
	assert.Error(t, waitingTransactionError)  // second session being killed

	// We can get the lock successfully...
	verifyCtx, verifyCancel := context.WithCancel(context.Background())
	verifyCancel() // just cancel it up front...

	t0 := time.Now()
	verifyTransactionError := blockingTransaction(t, verifyCtx)
	assert.WithinDuration(t, t0, time.Now(), 50*time.Millisecond)
	assert.NoError(t, verifyTransactionError)

}
