package changefeed

import (
	"context"
	"database/sql"
	"github.com/stretchr/testify/require"
	"github.com/vippsas/mssql-changefeed/go/changefeed/sqltest"
	"testing"
	"time"
)

func blockingTransaction(t *testing.T, ctx context.Context) {
	tx, err := fixture.DB.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot})
	require.NoError(t, err)
	defer tx.Commit()

	_, err = tx.ExecContext(ctx, `[changefeed].lock_shard`,
		sql.Named("object_id", MyTableObjectID),
		sql.Named("shard_id", 0),
		sql.Named("timeout", 1000),
	)

	_, err = tx.ExecContext(ctx, `insert into myservice.MyTable(id, value) values (1, 'foo');`)

	<-ctx.Done()

	require.NoError(t, err)
}

func TestKillBlocker(t *testing.T) {
	blockingCtx, blockingCancel := context.WithCancel(context.Background())

	go blockingTransaction(t, blockingCtx)

	time.Sleep(200 * time.Millisecond)

	sqltest.QueryDump(fixture.DB, `select * from sys.dm_tran_locks where resource_type = 'APPLICATION'`)

	_, err := fixture.DB.ExecContext(context.Background(), `[changefeed].kill_session_blocking_shard`,
		sql.Named("object_id", MyTableObjectID),
		sql.Named("shard_id", 0),
	)
	require.NoError(t, err)

	sqltest.QueryDump(fixture.DB, `select * from sys.dm_tran_locks where resource_type = 'APPLICATION'`)

	time.Sleep(3 * time.Second)
	blockingCancel()

}
