package changefeed

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/gofrs/uuid"
	"github.com/oklog/ulid"
	"sync"
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
	conn *sql.Conn

	// sql.Tx etc are safe to use concurrently, so least surprising if this is too
	lock sync.Mutex

	feedID     mssql.UniqueIdentifier
	shardID    int
	ulidPrefix [8]byte
	ulidSuffix uint64 // TODO test that we scan 64th bit
	time       *time.Time
}

var _ SQLTxMethods = &Transaction{}
var _ driver.Tx = &Transaction{}

func (tx *Transaction) NextULID() ulid.ULID {
	tx.lock.Lock()
	defer tx.lock.Unlock()

	var result [16]byte
	copy(result[0:8], tx.ulidPrefix[:])
	binary.BigEndian.PutUint64(result[8:16], tx.ulidSuffix)

	tx.ulidSuffix++
	return ulid.ULID(result)
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

	// This code is a bit tricky! Here's the deal:
	// First, anything passed to the mssql driver that *takes a parameter* will cause
	// the creation of a temporary stored procedure, which is then executed.
	// However, it is not legal to call `commit` inside a stored procedure (which is
	// not balanced by a `begin transaction`, which is the case here). So,
	// if we pass a parameter, we will not be able to call 'commit'.
	//
	// Then: It is extremely important that the call to commit_ulid_transaction
	// happens in the *same* network roundtrip to mssql as the commit; so that a sudden
	// power-off will not affect that both of them always runs -- this is because
	// the commit_ulid_transaction call will block other transactions, and if we then
	// have a power-off in-between, we'd block other processes for a long time.
	//
	// Solution: "Smuggle" the parameters using sp_set_session_context.
	// Most of the parameters are communicated by the functions themselves, but ulid_suffix
	// changes and is set here.

	_, err := tx.conn.ExecContext(ctx, `
declare @ulid_suffix_typed bigint = @ulid_suffix
exec sp_set_session_context N'changefeed.ulid_suffix', @ulid_suffix_typed;
	`,
		sql.Named("ulid_suffix", int64(tx.ulidSuffix)),
	)
	if err != nil {
		return err
	}

	_, errCommit := tx.conn.ExecContext(ctx, `
declare @ulid_suffix bigint = convert(bigint, session_context(N'changefeed.ulid_suffix'))
exec [changefeed].commit_ulid_transaction @ulid_suffix = @ulid_suffix;
    `)
	if errCommit != nil {
		return errCommit
	}
	_, errCommit = tx.conn.ExecContext(ctx, `/* do not remove me */ commit`) // or else, commit will be interpreted as name of a stored procedure

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
}

func BeginTransaction(sqlDB Conner, ctx context.Context, feedID uuid.UUID, shardID int, options *TransactionOptions) (*Transaction, error) {
	var tx Transaction
	var err error

	var timeHint *time.Time
	if options != nil {
		timeHintValue := options.TimeHint
		timeHint = &timeHintValue
	}

	tx.feedID = mssql.UniqueIdentifier(feedID)
	tx.shardID = shardID

	tx.conn, err = sqlDB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	_, err = tx.conn.ExecContext(ctx, `set transaction isolation level snapshot; begin transaction;`)
	if err != nil {
		_ = tx.conn.Close()
		return nil, err
	}
	var ulidPrefix []byte
	var ulidSuffix int64

	_, err = tx.conn.ExecContext(ctx, `[changefeed].begin_ulid_transaction`,
		sql.Named("feed_id", feedID),
		sql.Named("shard_id", shardID),
		sql.Named("time_hint", timeHint),
		sql.Named("ulid_prefix", sql.Out{Dest: &ulidPrefix}),
		sql.Named("ulid_suffix", sql.Out{Dest: &ulidSuffix}),
	)
	if err != nil {
		_ = tx.conn.Close()
		return nil, err
	}
	copy(tx.ulidPrefix[:], ulidPrefix)
	tx.ulidSuffix = uint64(ulidSuffix) // conversion of negative to positive is intended!

	return &tx, nil
}
