package changefeed

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

// Longpoll blocks until a new change is available on the given feed, or the timeout elapses.
// The timeout is available as a separate parameter rather than reading a timeout from the
// context parameter because the timeout loop is in SQL; if ctx times out it should ideally time out
// somewhat *after* the timeout duration has passed.
//
// This function will start with doing a simple poll, and seenChangeSequenceNumber iks
//
// The function returning does not guarantee that a new change is available, and there is also no
// way to tell a timeout from a successful block and this design is intentional; the caller should
// always do a full event read afterwards. This protects us against a full disaster if there are
// bugs in the longpoll design.
func Longpoll(ctx context.Context, dbc *sql.DB, feedID, shard int, timeout time.Duration, seenChangeSequenceNumber int) (err error) {
	// DRY ... poll() sets the result variables in the enclosing scope and returns 'true' if we are to return...
	poll := func() bool {
		var seqno int
		err = dbc.QueryRowContext(ctx,
			`select last_change_sequence_number from changefeed.shard where feed_id = @p1 and shard = @p2`,
			feedID, shard,
		).Scan(&seqno)
		return err != nil || seqno != seenChangeSequenceNumber
	}

	// Initial poll, to remove overhead in cases where traffic is high enough that longpolling is never needed
	if poll() {
		return
	}

	// OK we want to block. The below is a bit hacky (to achieve race-safety), but mssql has limited
	// primitives available and this is what we found...

	// First, make a transaction, record the transaction ID.
	// The isolation doesn't matter
	var transactionID int
	tx, err := dbc.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	err = tx.QueryRowContext(ctx, `select current_transaction_id()`).Scan(&transactionID)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	// Then we want to do a blocking call in this transaction, but, do it in another goroutine,
	// because we have more work to do before we can rest:
	var wg sync.WaitGroup
	wg.Add(1)

	var longpollErr error

	go func(perr *error) {
		defer wg.Done()

		_, *perr = tx.ExecContext(ctx, `changefeed.longpoll`,
			sql.Named("feed_id", feedID),
			sql.Named("shard", shard),
			sql.Named("change_sequence_number", seenChangeSequenceNumber),
			sql.Named("timeout_milliseconds", timeout.Milliseconds()),
		)

		// We can simply roll back in this goroutine, and the caller can ignore rolling back in all cases...
		_ = tx.Rollback()

	}(&longpollErr)

	// OK, while the call above is running we want to block on a guard that makes sure, through some
	// polling, that the process above has progressed to actually holding a lock (it takes some milliseconds
	// before it gets to holding the lock...). This is taking another connection from the pool than the one
	// we hold above.
	var blocked bool
	_, guardErr := dbc.ExecContext(ctx, "changefeed.longpoll_guard",
		sql.Named("transaction_id", transactionID),
		sql.Named("blocked", sql.Out{Dest: &blocked}),
	)
	if guardErr != nil {
		// Just leave the goroutine until it shuts down cleanly on its own
		return guardErr
	}

	// OK, so the guard completed and now the goroutine is officially blocked until there is a change.
	// At this point, do an extra poll to see if changes were written in the meantime.
	if poll() {
		return
	}

	// OK, finally we are ready to block!
	wg.Wait()

	return longpollErr
}
