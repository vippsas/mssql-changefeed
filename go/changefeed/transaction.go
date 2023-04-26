package changefeed

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/gofrs/uuid"
	"github.com/oklog/ulid"
	"time"
)

const commitRollbackContextTimeout = 2 * time.Second

type Conner interface {
	Conn(ctx context.Context) (*sql.Conn, error)
}

type SQLTxMethods interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// Transaction wraps a *sql.Tx to make sure that the ULID begin/commit functions
// are run at the right point in time.
type Transaction struct {
	schema string
	conn   *sql.Conn

	feedID  mssql.UniqueIdentifier
	shardID int
}

var _ SQLTxMethods = &Transaction{}
var _ driver.Tx = &Transaction{}

func (tx *Transaction) NextULID(ctx context.Context) (result ulid.ULID, err error) {
	err = tx.conn.QueryRowContext(ctx,
		fmt.Sprintf(`select [%s].ulid(next value for [%s].sequence)`, tx.schema, tx.schema),
	).Scan(&result)
	return
}

func (tx *Transaction) Time(ctx context.Context) (result time.Time, err error) {
	err = tx.conn.QueryRowContext(ctx,
		fmt.Sprintf(`select [%s].time()`, tx.schema),
	).Scan(&result)
	return
}

func (tx *Transaction) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return tx.conn.ExecContext(ctx, query, args...)
}

func (tx *Transaction) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return tx.conn.QueryContext(ctx, query, args...)
}

func (tx *Transaction) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return tx.conn.QueryRowContext(ctx, query, args...)
}

// Commit commits the transaction
func (tx *Transaction) Commit() error {
	// The standard Commit()/Rollback() interfaces don't take a context.
	// Not really operations you want to abort anyway; so we just let them block
	// a while hard-coded timeout
	ctx, cancel := context.WithTimeout(context.Background(), commitRollbackContextTimeout)
	defer cancel()

	// This code used to be a bit tricky! Here's the deal:
	// First, anything passed to the mssql driver that *takes a parameter* will cause
	// the creation of a temporary stored procedure, which is then executed.
	// However, it is not legal to call `commit` inside a stored procedure (which is
	// not balanced by a `begin transaction`, which is the case here). So,
	// if we pass a parameter, we will not be able to call 'commit'.
	//
	// Then: It is extremely important that the call to commit_transaction
	// happens in the *same* network roundtrip to mssql as the commit; so that a sudden
	// power-off will not affect that both of them always runs -- this is because
	// the commit_transaction call will block other transactions, and if we then
	// have a power-off in-between, we'd block other processes for a long time.
	//
	// Solution: "Smuggle" the parameters using sp_set_session_context.
	// This was rolled into the stored procedures -- so now it is rather simple..
	_, errCommit := tx.conn.ExecContext(ctx, `
exec [`+tx.schema+`].commit_transaction;
/* do not remove me */ commit;
exec [`+tx.schema+`].release_lock;
`) // or else, commit will be interpreted as name of a stored procedure

	errClose := tx.conn.Close()
	if errCommit != nil {
		return errCommit
	}
	return errClose
}

func (tx *Transaction) Rollback() error {
	// The standard Commit()/Rollback() interfaces don't take a context.
	// Not really operations you want to abort anyway; so we just let them block
	// a while hard-coded timeout
	ctx, cancel := context.WithTimeout(context.Background(), commitRollbackContextTimeout)
	defer cancel()

	_, errRollback := tx.conn.ExecContext(ctx, `/*do not remove me*/ rollback`) // or else, rollback will be interpreted as name of a stored procedure
	errClose := tx.conn.Close()
	if errRollback != nil {
		return errRollback
	}
	return errClose
}

type TransactionOptions struct {
	TimeHint time.Time
	Schema   string
}

func BeginTransaction(sqlDB Conner, ctx context.Context, feedID uuid.UUID, shardID int, options *TransactionOptions) (*Transaction, error) {
	var tx Transaction
	var err error

	var timeHint *time.Time

	tx.schema = "changefeed"
	if options != nil {
		if options.TimeHint != (time.Time{}) {
			timeHintValue := options.TimeHint
			timeHint = &timeHintValue
		}
		if options.Schema != "" {
			tx.schema = options.Schema
		}
	}

	tx.feedID = mssql.UniqueIdentifier(feedID)
	tx.shardID = shardID

	tx.conn, err = sqlDB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	err = tx.initTransaction(ctx, timeHint)
	if err != nil {
		_ = tx.conn.Close()
		return nil, err
	}

	return &tx, nil
}

func (tx *Transaction) initTransaction(ctx context.Context, timeHint *time.Time) error {
	_, err := tx.conn.ExecContext(ctx, `[`+tx.schema+`].begin_driver_transaction`,
		sql.Named("feed_id", tx.feedID),
		sql.Named("shard_id", tx.shardID),
		sql.Named("time_hint", timeHint),
		sql.Named("timeout", 10000),
	)
	if err != nil {
		return err
	}

	_, err = tx.conn.ExecContext(ctx, `set transaction isolation level snapshot; begin transaction;`)
	if err != nil {
		// best effort release_lock...
		_, _ = tx.conn.ExecContext(ctx, `exec [`+tx.schema+`].release_lock`)
		// ...and return original error
		return err
	}
	return nil
}
