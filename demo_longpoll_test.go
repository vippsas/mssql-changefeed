package changefeed

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

// This test doesn't really test anything, is just a demonstration that sp_getapplock can be used
// for longpolling. The behaviour is confirmed by to random internet searches...:
//
// a) When you try to get an Exclusive lock, first all existing requests for a Shared lock is satisifed...
// b) BUT, no new requests for a Shared lock is granted before the Exclusive lock is handed out
//
// So, the writer can hold an Exclusive lock and release-and-lock it to signal all readers (with Shared
// locks) that new data is available.

func TestDemoUseOfLocksForLongPoll(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	endTime := time.Now().Add(5 * time.Second)

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := fixture.DB.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		require.NoError(t, discardResult(conn.ExecContext(ctx, `
		declare @lockresult int
        exec @lockresult = sp_getapplock @Resource = 'longpoll', @LockMode = 'Exclusive', @LockOwner = 'Session';
        if @lockresult < 0 raiserror ('did not get lock', 18, 1)
`)))
		fmt.Println("INITIAL LOCK")
		for time.Now().Before(endTime) {
			require.NoError(t, discardResult(conn.ExecContext(ctx, `
        exec sp_releaseapplock @Resource = 'longpoll', @LockOwner = 'Session';

		declare @lockresult int
        exec @lockresult = sp_getapplock @Resource = 'longpoll', @LockMode = 'Exclusive', @LockOwner = 'Session';
        if @lockresult < 0 raiserror ('did not get lock', 18, 1)
`)))
			time.Sleep(1 * time.Second)
			fmt.Println("=======ITERATION=====")
		}

		require.NoError(t, discardResult(conn.ExecContext(ctx, `
        exec sp_releaseapplock @Resource = 'longpoll', @LockOwner = 'Session';
		`)))
	}()

	wg.Add(10)
	for i := 0; i != 10; i++ {
		go func(i int) {
			defer wg.Done()
			for time.Now().Before(endTime) {
				var r int
				require.NoError(t, fixture.DB.QueryRowContext(ctx, `
	   		declare @lockresult int
	           exec @lockresult = sp_getapplock @Resource = 'longpoll', @LockMode = 'Shared', @LockOwner = 'Session';
	           if @lockresult < 0
	               begin
	                   declare @msg varchar(max) = concat('did not get:', @lockresult)
	                   raiserror (@msg, 18, 1)
	                   return
	                end
	           exec sp_releaseapplock @Resource = 'longpoll', @LockOwner = 'Session';
	   		
    		select @lockresult
	   `).Scan(&r))
				fmt.Println(fmt.Sprintf("reader %d %d", i, r))

			}
		}(i)

	}

	wg.Wait()
}
