package changefeed

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

// This testcase is run manually to inspect the generated read: stored procedure
func TestGenerateReadProc(t *testing.T) {
	fixture.Reset(t)
	var sql string
	require.NoError(t, fixture.DB.QueryRow(`select [changefeed].gen_read_feed_sql(object_id('myservice.MultiPK'), 'changefeed')`).Scan(&sql))

	fmt.Println(sql)

	// Simply check that generated SQL compiles
	_, err := fixture.DB.ExecContext(context.Background(), `
exec [changefeed].setup_feed 'myservice.MultiPK'
`)
	require.NoError(t, err)

}
