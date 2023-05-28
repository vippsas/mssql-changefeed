package changefeed

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

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

// This testcase is run manually to inspect the generated read: stored procedure
func TestCreateReadProcedre(t *testing.T) {
	fixture.Reset(t)
	var sql string
	require.NoError(t, fixture.DB.QueryRow(`select [changefeed].sql_create_read_procedure(object_id('myservice.MultiPK'), 'changefeed')`).Scan(&sql))
	fmt.Println(sql)
	_, err := fixture.DB.ExecContext(context.Background(), sql)
	require.NoError(t, err)
}

func TestSetupFeed(t *testing.T) {
	_, err := fixture.DB.ExecContext(context.Background(),
		`exec [changefeed].setup_feed 'myservice.MultiPK'`)
	require.NoError(t, err)

	// Smoketest of generated function... the real tests are in changefeed_test.go though
	_, err = fixture.DB.ExecContext(context.Background(), `


create table #read (
    ulid binary(16) not null,
    x int not null,
    y uniqueidentifier not null,
    z varchar(10) not null,
);

exec [changefeed].[read_feed:myservice.MultiPK] 0, 0x0, 100
`)
	require.NoError(t, err)
}
