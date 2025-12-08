package changefeed

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vippsas/mssql-changefeed/go/changefeed/sqltest"
)

func TestTeardownFeedOutbox(t *testing.T) {
	ctx := context.Background()
	tableName := "myservice.TestTeardownOutbox"

	// Setup the changefeed with outbox mode
	_, err := fixture.AdminDB.ExecContext(ctx, `
exec [changefeed].setup_feed 'myservice.TestTeardownOutbox', @outbox = 1;
`)
	require.NoError(t, err)

	// Verify all changefeed objects were created
	assert.True(t, objectExists(t, "[changefeed].[state:myservice.TestTeardownOutbox]"), "state table should exist after setup")
	assert.True(t, objectExists(t, "[changefeed].[feed:myservice.TestTeardownOutbox]"), "feed table should exist after setup")
	assert.True(t, objectExists(t, "[changefeed].[outbox:myservice.TestTeardownOutbox]"), "outbox table should exist after setup")
	assert.True(t, objectExists(t, "[changefeed].[sequence:myservice.TestTeardownOutbox]"), "sequence should exist after setup")
	assert.True(t, objectExists(t, "[changefeed].[read_feed:myservice.TestTeardownOutbox]"), "read_feed proc should exist after setup")
	assert.True(t, objectExists(t, "[changefeed].[feed_write_lock:myservice.TestTeardownOutbox]"), "feed_write_lock proc should exist after setup")
	assert.True(t, objectExists(t, "[changefeed].[update_state:myservice.TestTeardownOutbox]"), "update_state proc should exist after setup")
	assert.True(t, typeExists(t, "[changefeed].[type:read:myservice.TestTeardownOutbox]"), "read type should exist after setup")
	assert.True(t, roleExists(t, "changefeed.writers:myservice.TestTeardownOutbox"), "writer role should exist after setup")
	assert.True(t, roleExists(t, "changefeed.readers:myservice.TestTeardownOutbox"), "reader role should exist after setup")

	// Insert some test data to make sure teardown works even with data
	_, err = fixture.AdminDB.ExecContext(ctx, `
insert into myservice.TestTeardownOutbox (AggregateID, Version, Data) values (1, 1, 'test');
insert into [changefeed].[outbox:myservice.TestTeardownOutbox] (shard_id, time_hint, AggregateID, Version) values (0, getutcdate(), 1, 1);
`)
	require.NoError(t, err)

	// Teardown the changefeed
	_, err = fixture.AdminDB.ExecContext(ctx, `exec [changefeed].teardown_feed @table_name = @p1`, tableName)
	require.NoError(t, err)

	// Verify all changefeed objects were removed
	assert.False(t, objectExists(t, "[changefeed].[state:myservice.TestTeardownOutbox]"), "state table should not exist after teardown")
	assert.False(t, objectExists(t, "[changefeed].[feed:myservice.TestTeardownOutbox]"), "feed table should not exist after teardown")
	assert.False(t, objectExists(t, "[changefeed].[outbox:myservice.TestTeardownOutbox]"), "outbox table should not exist after teardown")
	assert.False(t, objectExists(t, "[changefeed].[sequence:myservice.TestTeardownOutbox]"), "sequence should not exist after teardown")
	assert.False(t, objectExists(t, "[changefeed].[read_feed:myservice.TestTeardownOutbox]"), "read_feed proc should not exist after teardown")
	assert.False(t, objectExists(t, "[changefeed].[feed_write_lock:myservice.TestTeardownOutbox]"), "feed_write_lock proc should not exist after teardown")
	assert.False(t, objectExists(t, "[changefeed].[update_state:myservice.TestTeardownOutbox]"), "update_state proc should not exist after teardown")
	assert.False(t, typeExists(t, "[changefeed].[type:read:myservice.TestTeardownOutbox]"), "read type should not exist after teardown")
	assert.False(t, roleExists(t, "changefeed.writers:myservice.TestTeardownOutbox"), "writer role should not exist after teardown")
	assert.False(t, roleExists(t, "changefeed.readers:myservice.TestTeardownOutbox"), "reader role should not exist after teardown")

	// Verify the original table still exists (teardown should only remove changefeed objects)
	assert.True(t, objectExists(t, "myservice.TestTeardownOutbox"), "original table should still exist after teardown")

	// Verify the data in the original table is still there
	count := sqltest.QueryInt(fixture.AdminDB, `select count(*) from myservice.TestTeardownOutbox`)
	assert.Equal(t, 1, count, "data in original table should still exist after teardown")
}

func TestTeardownFeedBlocking(t *testing.T) {
	ctx := context.Background()
	tableName := "myservice.TestTeardownBlocking"

	// Setup the changefeed with blocking mode
	_, err := fixture.AdminDB.ExecContext(ctx, `
exec [changefeed].setup_feed 'myservice.TestTeardownBlocking', @blocking = 1;
`)
	require.NoError(t, err)

	// Verify changefeed objects were created (blocking mode creates different objects than outbox)
	assert.True(t, objectExists(t, "[changefeed].[state:myservice.TestTeardownBlocking]"), "state table should exist after setup")
	assert.True(t, objectExists(t, "[changefeed].[lock:myservice.TestTeardownBlocking]"), "lock proc should exist after setup")
	assert.True(t, objectExists(t, "[changefeed].[ulid:myservice.TestTeardownBlocking]"), "ulid function should exist after setup")
	assert.True(t, objectExists(t, "[changefeed].[update_state:myservice.TestTeardownBlocking]"), "update_state proc should exist after setup")
	assert.True(t, roleExists(t, "changefeed.writers:myservice.TestTeardownBlocking"), "writer role should exist after setup")

	// Blocking mode doesn't create reader role, feed table, outbox table, sequence, or read type
	assert.False(t, objectExists(t, "[changefeed].[feed:myservice.TestTeardownBlocking]"), "feed table should not exist for blocking mode")
	assert.False(t, objectExists(t, "[changefeed].[outbox:myservice.TestTeardownBlocking]"), "outbox table should not exist for blocking mode")
	assert.False(t, roleExists(t, "changefeed.readers:myservice.TestTeardownBlocking"), "reader role should not exist for blocking mode")

	// Insert some test data (lock procedure requires a transaction)
	_, err = fixture.AdminDB.ExecContext(ctx, `
begin transaction;
declare @now datetime2(3) = getutcdate();
exec [changefeed].[lock:myservice.TestTeardownBlocking] @shard_id = 0, @time_hint = @now;
insert into myservice.TestTeardownBlocking (EventID, Data) values ([changefeed].[ulid:myservice.TestTeardownBlocking](0), 'test');
commit;
`)
	require.NoError(t, err)

	// Teardown the changefeed
	_, err = fixture.AdminDB.ExecContext(ctx, `exec [changefeed].teardown_feed @table_name = @p1`, tableName)
	require.NoError(t, err)

	// Verify all changefeed objects were removed
	assert.False(t, objectExists(t, "[changefeed].[state:myservice.TestTeardownBlocking]"), "state table should not exist after teardown")
	assert.False(t, objectExists(t, "[changefeed].[lock:myservice.TestTeardownBlocking]"), "lock proc should not exist after teardown")
	assert.False(t, objectExists(t, "[changefeed].[ulid:myservice.TestTeardownBlocking]"), "ulid function should not exist after teardown")
	assert.False(t, objectExists(t, "[changefeed].[update_state:myservice.TestTeardownBlocking]"), "update_state proc should not exist after teardown")
	assert.False(t, roleExists(t, "changefeed.writers:myservice.TestTeardownBlocking"), "writer role should not exist after teardown")

	// Verify the original table still exists
	assert.True(t, objectExists(t, "myservice.TestTeardownBlocking"), "original table should still exist after teardown")

	// Verify the data in the original table is still there
	count := sqltest.QueryInt(fixture.AdminDB, `select count(*) from myservice.TestTeardownBlocking`)
	assert.Equal(t, 1, count, "data in original table should still exist after teardown")
}

func TestTeardownFeedIdempotent(t *testing.T) {
	ctx := context.Background()

	// Create a temporary table for this test
	_, err := fixture.AdminDB.ExecContext(ctx, `
if object_id('myservice.TestTeardownIdempotent') is not null drop table myservice.TestTeardownIdempotent;
create table myservice.TestTeardownIdempotent (
    AggregateID bigint not null,
    Version int not null,
    primary key (AggregateID, Version)
);
`)
	require.NoError(t, err)

	// Setup the changefeed
	_, err = fixture.AdminDB.ExecContext(ctx, `exec [changefeed].setup_feed 'myservice.TestTeardownIdempotent', @outbox = 1;`)
	require.NoError(t, err)

	// Teardown once
	_, err = fixture.AdminDB.ExecContext(ctx, `exec [changefeed].teardown_feed @table_name = 'myservice.TestTeardownIdempotent';`)
	require.NoError(t, err)

	// Calling teardown again should not error (idempotent)
	_, err = fixture.AdminDB.ExecContext(ctx, `exec [changefeed].teardown_feed @table_name = 'myservice.TestTeardownIdempotent'`)
	require.NoError(t, err, "teardown_feed should be idempotent and not error when called on already torn down feed")

	// Clean up
	_, _ = fixture.AdminDB.ExecContext(ctx, `drop table myservice.TestTeardownIdempotent;`)
}

func TestTeardownFeedDoesNotAffectOtherFeeds(t *testing.T) {
	ctx := context.Background()

	// Create two temporary tables for this test
	_, err := fixture.AdminDB.ExecContext(ctx, `
if object_id('myservice.TestTeardownIsolationA') is not null drop table myservice.TestTeardownIsolationA;
if object_id('myservice.TestTeardownIsolationB') is not null drop table myservice.TestTeardownIsolationB;

create table myservice.TestTeardownIsolationA (
    AggregateID bigint not null,
    Version int not null,
    Data varchar(max) not null,
    primary key (AggregateID, Version)
);

create table myservice.TestTeardownIsolationB (
    AggregateID bigint not null,
    Version int not null,
    Data varchar(max) not null,
    primary key (AggregateID, Version)
);
`)
	require.NoError(t, err)

	// Setup changefeeds for both tables
	_, err = fixture.AdminDB.ExecContext(ctx, `
exec [changefeed].setup_feed 'myservice.TestTeardownIsolationA', @outbox = 1;
exec [changefeed].setup_feed 'myservice.TestTeardownIsolationB', @outbox = 1;
`)
	require.NoError(t, err)

	// Verify both changefeeds were created
	assert.True(t, objectExists(t, "[changefeed].[state:myservice.TestTeardownIsolationA]"), "feed A: state table should exist")
	assert.True(t, objectExists(t, "[changefeed].[feed:myservice.TestTeardownIsolationA]"), "feed A: feed table should exist")
	assert.True(t, objectExists(t, "[changefeed].[outbox:myservice.TestTeardownIsolationA]"), "feed A: outbox table should exist")
	assert.True(t, roleExists(t, "changefeed.writers:myservice.TestTeardownIsolationA"), "feed A: writer role should exist")
	assert.True(t, roleExists(t, "changefeed.readers:myservice.TestTeardownIsolationA"), "feed A: reader role should exist")

	assert.True(t, objectExists(t, "[changefeed].[state:myservice.TestTeardownIsolationB]"), "feed B: state table should exist")
	assert.True(t, objectExists(t, "[changefeed].[feed:myservice.TestTeardownIsolationB]"), "feed B: feed table should exist")
	assert.True(t, objectExists(t, "[changefeed].[outbox:myservice.TestTeardownIsolationB]"), "feed B: outbox table should exist")
	assert.True(t, roleExists(t, "changefeed.writers:myservice.TestTeardownIsolationB"), "feed B: writer role should exist")
	assert.True(t, roleExists(t, "changefeed.readers:myservice.TestTeardownIsolationB"), "feed B: reader role should exist")

	// Insert data into both feeds
	_, err = fixture.AdminDB.ExecContext(ctx, `
insert into myservice.TestTeardownIsolationA (AggregateID, Version, Data) values (1, 1, 'data A');
insert into [changefeed].[outbox:myservice.TestTeardownIsolationA] (shard_id, time_hint, AggregateID, Version) values (0, getutcdate(), 1, 1);

insert into myservice.TestTeardownIsolationB (AggregateID, Version, Data) values (2, 1, 'data B');
insert into [changefeed].[outbox:myservice.TestTeardownIsolationB] (shard_id, time_hint, AggregateID, Version) values (0, getutcdate(), 2, 1);
`)
	require.NoError(t, err)

	// Teardown only feed A
	_, err = fixture.AdminDB.ExecContext(ctx, `exec [changefeed].teardown_feed @table_name = 'myservice.TestTeardownIsolationA';`)
	require.NoError(t, err)

	// Verify feed A was torn down
	assert.False(t, objectExists(t, "[changefeed].[state:myservice.TestTeardownIsolationA]"), "feed A: state table should NOT exist after teardown")
	assert.False(t, objectExists(t, "[changefeed].[feed:myservice.TestTeardownIsolationA]"), "feed A: feed table should NOT exist after teardown")
	assert.False(t, objectExists(t, "[changefeed].[outbox:myservice.TestTeardownIsolationA]"), "feed A: outbox table should NOT exist after teardown")
	assert.False(t, roleExists(t, "changefeed.writers:myservice.TestTeardownIsolationA"), "feed A: writer role should NOT exist after teardown")
	assert.False(t, roleExists(t, "changefeed.readers:myservice.TestTeardownIsolationA"), "feed A: reader role should NOT exist after teardown")

	// Verify feed B is still intact
	assert.True(t, objectExists(t, "[changefeed].[state:myservice.TestTeardownIsolationB]"), "feed B: state table should STILL exist")
	assert.True(t, objectExists(t, "[changefeed].[feed:myservice.TestTeardownIsolationB]"), "feed B: feed table should STILL exist")
	assert.True(t, objectExists(t, "[changefeed].[outbox:myservice.TestTeardownIsolationB]"), "feed B: outbox table should STILL exist")
	assert.True(t, roleExists(t, "changefeed.writers:myservice.TestTeardownIsolationB"), "feed B: writer role should STILL exist")
	assert.True(t, roleExists(t, "changefeed.readers:myservice.TestTeardownIsolationB"), "feed B: reader role should STILL exist")

	// Verify feed B data is still accessible and the feed still works
	countB := sqltest.QueryInt(fixture.AdminDB, `select count(*) from myservice.TestTeardownIsolationB`)
	assert.Equal(t, 1, countB, "feed B: data should still exist")

	outboxCountB := sqltest.QueryInt(fixture.AdminDB, `select count(*) from [changefeed].[outbox:myservice.TestTeardownIsolationB]`)
	assert.Equal(t, 1, outboxCountB, "feed B: outbox data should still exist")

	// Verify original table A still exists with its data
	assert.True(t, objectExists(t, "myservice.TestTeardownIsolationA"), "table A should still exist")
	countA := sqltest.QueryInt(fixture.AdminDB, `select count(*) from myservice.TestTeardownIsolationA`)
	assert.Equal(t, 1, countA, "table A: data should still exist")

	// Clean up feed B and tables
	_, _ = fixture.AdminDB.ExecContext(ctx, `
exec [changefeed].teardown_feed @table_name = 'myservice.TestTeardownIsolationB';
drop table myservice.TestTeardownIsolationA;
drop table myservice.TestTeardownIsolationB;
`)
}

func TestTeardownFeedErrorsForNonExistentFeed(t *testing.T) {
	ctx := context.Background()

	// Try to teardown a feed that was never set up (table doesn't exist either)
	_, err := fixture.AdminDB.ExecContext(ctx, `exec [changefeed].teardown_feed @table_name = 'myservice.NonExistentTable';`)
	require.Error(t, err, "teardown_feed should error when called for a table/feed that never existed")
}

func TestTeardownFeedAfterTableDeleted(t *testing.T) {
	ctx := context.Background()

	// Create a temporary table
	_, err := fixture.AdminDB.ExecContext(ctx, `
if object_id('myservice.TestTeardownDeletedTable') is not null drop table myservice.TestTeardownDeletedTable;
create table myservice.TestTeardownDeletedTable (
    AggregateID bigint not null,
    Version int not null,
    Data varchar(max) not null,
    primary key (AggregateID, Version)
);
`)
	require.NoError(t, err)

	// Setup the changefeed
	_, err = fixture.AdminDB.ExecContext(ctx, `exec [changefeed].setup_feed 'myservice.TestTeardownDeletedTable', @outbox = 1;`)
	require.NoError(t, err)

	// Verify changefeed objects were created
	assert.True(t, objectExists(t, "[changefeed].[state:myservice.TestTeardownDeletedTable]"), "state table should exist after setup")
	assert.True(t, objectExists(t, "[changefeed].[feed:myservice.TestTeardownDeletedTable]"), "feed table should exist after setup")
	assert.True(t, objectExists(t, "[changefeed].[outbox:myservice.TestTeardownDeletedTable]"), "outbox table should exist after setup")
	assert.True(t, roleExists(t, "changefeed.writers:myservice.TestTeardownDeletedTable"), "writer role should exist after setup")
	assert.True(t, roleExists(t, "changefeed.readers:myservice.TestTeardownDeletedTable"), "reader role should exist after setup")

	// Delete the original table BEFORE calling teardown
	_, err = fixture.AdminDB.ExecContext(ctx, `drop table myservice.TestTeardownDeletedTable;`)
	require.NoError(t, err)

	// Verify the original table is gone
	assert.False(t, objectExists(t, "myservice.TestTeardownDeletedTable"), "original table should be deleted")

	// Teardown should still work even though the original table is gone
	_, err = fixture.AdminDB.ExecContext(ctx, `exec [changefeed].teardown_feed @table_name = 'myservice.TestTeardownDeletedTable';`)
	require.NoError(t, err, "teardown_feed should work even when the original table has been deleted")

	// Verify all changefeed objects were removed
	assert.False(t, objectExists(t, "[changefeed].[state:myservice.TestTeardownDeletedTable]"), "state table should not exist after teardown")
	assert.False(t, objectExists(t, "[changefeed].[feed:myservice.TestTeardownDeletedTable]"), "feed table should not exist after teardown")
	assert.False(t, objectExists(t, "[changefeed].[outbox:myservice.TestTeardownDeletedTable]"), "outbox table should not exist after teardown")
	assert.False(t, roleExists(t, "changefeed.writers:myservice.TestTeardownDeletedTable"), "writer role should not exist after teardown")
	assert.False(t, roleExists(t, "changefeed.readers:myservice.TestTeardownDeletedTable"), "reader role should not exist after teardown")
}

// Helper function to check if an object exists in the database
func objectExists(t *testing.T, objectName string) bool {
	var exists int
	err := fixture.AdminDB.QueryRow(`select case when object_id(@p1) is not null then 1 else 0 end`, objectName).Scan(&exists)
	require.NoError(t, err)
	return exists == 1
}

// Helper function to check if a role exists
func roleExists(t *testing.T, roleName string) bool {
	var exists int
	err := fixture.AdminDB.QueryRow(`select case when database_principal_id(@p1) is not null then 1 else 0 end`, roleName).Scan(&exists)
	require.NoError(t, err)
	return exists == 1
}

// Helper function to check if a type exists
func typeExists(t *testing.T, typeName string) bool {
	var exists int
	err := fixture.AdminDB.QueryRow(`select case when type_id(@p1) is not null then 1 else 0 end`, typeName).Scan(&exists)
	require.NoError(t, err)
	return exists == 1
}
