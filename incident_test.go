package changefeed

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vippsas/mssql-changefeed/sqltest"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func acquireLockAndDetectIncidents(t *testing.T, tx *sql.Tx, feedID string, shardID int) (incidentDetected bool, lockResult int) {
	var incidentCount int
	ctx := context.Background()
	_, err := tx.ExecContext(ctx, `[changefeed].acquire_lock_and_detect_incidents`,
		sql.Named("feed_id", feedID),
		sql.Named("shard_id", shardID),
		sql.Named("timeout", 250),
		sql.Named("incident_detected", sql.Out{Dest: &incidentDetected}),
		sql.Named("incident_count", sql.Out{Dest: &incidentCount}),
		sql.Named("lock_result", sql.Out{Dest: &lockResult}),
	)
	if incidentDetected {
		// by protocol, caller should always do this...
		bumpIncident(t, feedID, shardID, incidentCount)
	}
	require.NoError(t, err)
	return
}

func touchShard(t *testing.T, tx *sql.Tx, feedID string, shardID int) {
	ctx := context.Background()
	_, err := tx.ExecContext(ctx, `
update [changefeed].shard_ulid set ulid_low = @ulid_low where feed_id = @feed_id and shard_id = @shard_id;
if @@rowcount = 0
begin
    exec [changefeed].insert_shard @feed_id = @feed_id, @shard_id = @shard_id;
	update [changefeed].shard_ulid set ulid_low = @ulid_low where feed_id = @feed_id and shard_id = @shard_id;
end
`,
		sql.Named("feed_id", feedID),
		sql.Named("shard_id", shardID),
		sql.Named("ulid_low", rand.Int63()),
	)
	require.NoError(t, err)
}

func bumpIncident(t *testing.T, feedID string, shardID int, incidentCount int) {
	ctx := context.Background()
	_, err := fixture.DB.ExecContext(ctx, `[changefeed].set_incident_count`,
		sql.Named("feed_id", feedID),
		sql.Named("shard_id", shardID),
		sql.Named("incident_count", incidentCount),
	)
	require.NoError(t, err)
}

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

			tx, err := fixture.DB.BeginTx(ctx, nil)
			require.NoError(t, err)

			for {
				incidentDetected, lockResult := acquireLockAndDetectIncidents(t, tx, feedID, shardID)
				require.NoError(t, err)
				if incidentDetected {
					incidentDetectedGlobal = true
				}
				if lockResult >= 0 {
					break
				}
			}

			assert.False(t, lockTaken)
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
}

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

	for i := 0; i != 10; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			tx, err := fixture.DB.BeginTx(ctx, nil)
			require.NoError(t, err)

			for {
				_, lockResult := acquireLockAndDetectIncidents(t, tx, feedID, shardID)
				require.NoError(t, err)
				if lockResult >= 0 {
					break
				}
			}

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
				require.NoError(t, tx.Rollback())
			} else {
				// Got lock ... do some "work", make sure to change the state
				time.Sleep(100 * time.Millisecond)
				touchShard(t, tx, feedID, shardID)
				addLog("thread=%d %s", threadID, "releasing lock")
				require.NoError(t, tx.Commit())
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
