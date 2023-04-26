package changefeed

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vippsas/mssql-changefeed/go/changefeed/sqltest"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func acquireLockAndDetectIncidents(t *testing.T, conn *sql.Conn, feedID string, shardID int) {
	ctx := context.Background()
	_, err := conn.ExecContext(ctx, `[changefeed].acquire_lock_and_detect_incidents`,
		sql.Named("feed_id", feedID),
		sql.Named("shard_id", shardID),
		sql.Named("timeout", 500),
		sql.Named("max_attempts", 10),
	)
	require.NoError(t, err)
	return
}

func touchShard(t *testing.T, conn *sql.Conn, feedID string, shardID int) {
	ctx := context.Background()
	_, err := conn.ExecContext(ctx, `
update [changefeed].shard_ulid set ulid_low = @ulid_low where feed_id = @feed_id and shard_id = @shard_id;
`,
		sql.Named("feed_id", feedID),
		sql.Named("shard_id", shardID),
		sql.Named("ulid_low", rand.Int63()),
	)
	require.NoError(t, err)
}

/*
func TestAcquireLockHappyDay(t *testing.T) {
	ctx := context.Background()
	feedID := "91a9c4d2-e274-11ed-8fa4-6769c3d4e7fa"
	shardID := 0

	var wg sync.WaitGroup
	lockTaken := false

	// 10 threads queue up to sleep 100 milliseconds each and then make progress;
	// noone should detect an incident
	incidentDetectedGlobal := false
	for i := 0; i != 10; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			conn, err := fixture.DB.Conn(ctx)
			require.NoError(t, err)

			//tx, err := fixture.DB.BeginTx(ctx, &sql.TxOptions{
			//	Isolation: sql.LevelSnapshot,
			//})

			acquireLockAndDetectIncidents(t, conn, feedID, shardID)
			lockTaken = true

			// Got lock ... do some "work", make sure to change the state
			time.Sleep(100 * time.Millisecond)
			touchShard(t, tx, feedID, shardID)

			lockTaken = false
			require.NoError(t, tx.Commit())

		}()
	}
	wg.Wait()
	assert.False(t, incidentDetectedGlobal)
}*/
/*
func TestDebugSnapshot(t *testing.T) {
	ctx := context.Background()
	feedID := "91a9c4d2-e274-11ed-8fa4-6769c3d4e7fa"
	shardID := 0

	_, err := fixture.DB.Exec(`[changefeed].insert_shard`,
		sql.Named("feed_id", feedID),
		sql.Named("shard_id", shardID))
	assert.NoError(t, err)

	_, err = fixture.DB.Exec(`create procedure dbo.noop
as begin
    return
end;`)

	time.Sleep(10 * time.Second)
	require.NoError(t, err)
	_, err = fixture.DB.ExecContext(ctx, `exec dbo.noop`)
	require.NoError(t, err)

	var tx *sql.Tx

	tx, err = fixture.DB.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSnapshot,
	})
	require.NoError(t, err)

	_, err = tx.ExecContext(ctx, `exec dbo.noop`)
	require.NoError(t, err)
	sqltest.QueryDump(fixture.DB, `select db_name()`)
	//time.Sleep(60 * time.Minute)

	snapshotStarted := sqltest.QueryInt(tx, `select count(*) from sys.dm_tran_active_snapshot_database_transactions where transaction_id = current_transaction_id()`)
	require.Equal(t, 0, snapshotStarted)
}
*/
func TestAcquireLockWithIncidents(t *testing.T) {
	ctx := context.Background()
	feedID := "91a9c4d2-e274-11ed-8fa4-6769c3d4e7fa"
	shardID := 0

	var wg sync.WaitGroup

	var threadIDCounter int64 = 0

	var logLock sync.Mutex
	log := []string{}
	addLog := func(format string, args ...interface{}) {
		logLock.Lock()
		defer logLock.Unlock()
		log = append(log, fmt.Sprintf(format, args...))
	}

	_, err := fixture.DB.Exec(`[changefeed].insert_shard`,
		sql.Named("feed_id", feedID),
		sql.Named("shard_id", shardID))
	assert.NoError(t, err)

	for i := 0; i != 4; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			conn, err := fixture.DB.Conn(ctx)
			require.NoError(t, err)

			acquireLockAndDetectIncidents(t, conn, feedID, shardID)

			_, err = conn.ExecContext(ctx, `set transaction isolation level snapshot; begin transaction;`)
			require.NoError(t, err)

			time.Sleep(1 * time.Second)

			sqltest.QueryDump(conn, `select * from changefeed.shard_ulid`)
			touchShard(t, conn, feedID, shardID)

			_, err = conn.ExecContext(ctx, `/* */ commit`)
			require.NoError(t, err)

			_, err = conn.ExecContext(ctx, `[changefeed].release_lock`)
			require.NoError(t, err)

			return

			_, err = conn.ExecContext(ctx, `set transaction isolation level snapshot; begin transaction;`)
			require.NoError(t, err)

			// we number our threads after the point which we got to *this* position; and then
			// use that number in the assertions
			threadID := atomic.AddInt64(&threadIDCounter, 1)

			addLog("thread=%d %s", threadID, "got lock")

			// we do one happy-path thread before the incident happens so that the (feed,shard) has been
			// properly inserted in state tables. We start incidents at 2 points.
			if threadID == 2 || threadID == 6 {
				// cause an incident
				addLog("thread=%d %s", threadID, "causing incident")
				time.Sleep(3000 * time.Millisecond)
				addLog("thread=%d %s", threadID, "releasing ... this should happen very last")
				_, err = conn.ExecContext(ctx, `if @@trancount > 0 rollback`)
				require.NoError(t, err)
			} else {
				// Got lock ... do some "work", make sure to change the state
				time.Sleep(100 * time.Millisecond)
				touchShard(t, conn, feedID, shardID)
				addLog("thread=%d %s", threadID, "releasing lock")
				_, err = conn.ExecContext(ctx, `/* */ commit`)
				require.NoError(t, err)
				_, err = conn.ExecContext(ctx, `[changefeed].release_lock`)
				require.NoError(t, err)
				require.NoError(t, conn.Close())
			}

		}()
	}
	wg.Wait()

	assert.Equal(t, 2, sqltest.QueryInt(fixture.DB, `select incident_count from [changefeed].incident_count where feed_id = @p1 and shard_id = @p2`, feedID, shardID))
	assert.Equal(t, []string{
		"thread=1 got lock",
		"thread=1 releasing lock",
		"thread=2 got lock",
		"thread=2 causing incident", // incident 1
		"thread=3 got lock",
		"thread=3 releasing lock",
		"thread=4 got lock",
		"thread=4 releasing lock",
		"thread=5 got lock",
		"thread=5 releasing lock",
		"thread=6 got lock",
		"thread=6 causing incident", // incident 2
		"thread=7 got lock",
		"thread=7 releasing lock",
		"thread=8 got lock",
		"thread=8 releasing lock",
		"thread=9 got lock",
		"thread=9 releasing lock",
		"thread=10 got lock",
		"thread=10 releasing lock",
		"thread=2 releasing ... this should happen very last",
		"thread=6 releasing ... this should happen very last", // slight test race heisenbug here, but risking it..
	}, log)

}
