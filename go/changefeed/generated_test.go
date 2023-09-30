package changefeed

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCreateStateTable(t *testing.T) {
	var sql string
	require.NoError(t, fixture.DB.QueryRow(`select [changefeed].sql_create_state_table(object_id('myservice.MultiPK'), 'changefeed')`).Scan(&sql))
	fmt.Println(sql)
	// Simply check that generated SQL compiles
	_, err := fixture.DB.ExecContext(context.Background(), sql)
	require.NoError(t, err)
}

func TestCreateFeedTable(t *testing.T) {
	var sql string
	require.NoError(t, fixture.DB.QueryRow(`select [changefeed].sql_create_feed_table(object_id('myservice.MultiPK'), 'changefeed')`).Scan(&sql))
	fmt.Println(sql)
	// Simply check that generated SQL compiles
	_, err := fixture.DB.ExecContext(context.Background(), sql)
	require.NoError(t, err)
}

func TestCreateOutboxTable(t *testing.T) {
	var sql string
	require.NoError(t, fixture.DB.QueryRow(`select [changefeed].sql_create_outbox_table(object_id('myservice.MultiPK'), 'changefeed')`).Scan(&sql))
	fmt.Println(sql)
	// Simply check that generated SQL compiles
	_, err := fixture.DB.ExecContext(context.Background(), sql)
	require.NoError(t, err)
}

func TestCreateReadType(t *testing.T) {
	var sql string
	require.NoError(t, fixture.DB.QueryRow(`select [changefeed].sql_create_read_type(object_id('myservice.MultiPK'), 'changefeed')`).Scan(&sql))
	fmt.Println(sql)
	// Simply check that generated SQL compiles
	_, err := fixture.DB.ExecContext(context.Background(), sql)
	require.NoError(t, err)
}

// This testcase is run manually to inspect the generated read: stored procedure
func TestCreateReadProcedure(t *testing.T) {
	var sql string
	require.NoError(t, fixture.DB.QueryRow(`select [changefeed].sql_create_read_procedure(object_id('myservice.MultiPK'), 'changefeed')`).Scan(&sql))
	fmt.Println(sql)
	_, err := fixture.DB.ExecContext(context.Background(), sql)
	require.NoError(t, err)
}

func TestCreateFeedWriteLockProcedure(t *testing.T) {
	var sql string
	require.NoError(t, fixture.DB.QueryRow(`select [changefeed].sql_create_feed_write_lock_procedure(object_id('myservice.MultiPK'), 'changefeed')`).Scan(&sql))
	fmt.Println(sql)
	// Simply check that generated SQL compiles
	_, err := fixture.DB.ExecContext(context.Background(), sql)
	require.NoError(t, err)
}

func TestCreateUpdateStateProcedure(t *testing.T) {
	var sql string
	require.NoError(t, fixture.DB.QueryRow(`select [changefeed].sql_create_update_state_procedure(object_id('myservice.MultiPK'), 'changefeed')`).Scan(&sql))
	fmt.Println(sql)
	// Simply check that generated SQL compiles
	_, err := fixture.DB.ExecContext(context.Background(), sql)
	require.NoError(t, err)
}

func TestCreateLockProcedure(t *testing.T) {
	var sql string
	require.NoError(t, fixture.DB.QueryRow(`select [changefeed].sql_create_lock_procedure(object_id('myservice.MultiPK'), 'changefeed')`).Scan(&sql))
	fmt.Println(sql)
	// Simply check that generated SQL compiles
	_, err := fixture.DB.ExecContext(context.Background(), sql)
	require.NoError(t, err)
}

func TestPermissions(t *testing.T) {
	panic("hi")
}

func TestSetupFeed(t *testing.T) {
	_, err := fixture.DB.ExecContext(context.Background(),
		`exec [changefeed].setup_feed 'myservice.MultiPK'`)
	require.NoError(t, err)

	// test that upgrade_feed doesn't produce errors, it will alter/overwrite stored procedures
	_, err = fixture.DB.ExecContext(context.Background(),
		`exec [changefeed].upgrade_feed 'myservice.MultiPK'`)
	require.NoError(t, err)

	// Smoketest of generated function... the real tests are in changefeed_test.go though
	var value string
	err = fixture.DB.QueryRowContext(context.Background(), `
declare @y uniqueidentifier = newid();
    
insert into myservice.MultiPK (x, y, z, v)
values (1, @y, 'hello', 'world');

insert into [changefeed].[outbox:myservice.MultiPK] (shard_id, time_hint, x, y, z)
values (0, sysutcdatetime(), 1, @y, 'hello');

declare @tmp as [changefeed].[type:read:myservice.MultiPK];
select * into #read from @tmp;

exec [changefeed].[read_feed:myservice.MultiPK] 0, 0x0, 100;
    
select v from myservice.MultiPK as t
join #read as r on r.x = t.x and r.y = t.y and r.z = t.z;
`).Scan(&value)
	require.NoError(t, err)
	assert.Equal(t, "world", value)

}
