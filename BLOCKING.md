# mssql-changefeed: User guide for blocking mode

Please see [README](README.md) for an overview of the mssql-changefeed
library, and a comparison of the *blocking mode* documented below with
the [outbox mode](OUTBOX.md).

## Setup migration

You set up a new feed by calling the `setup_feed` stored procedure,
passing in the name of a table and `@blocking = 1`, in one of your own
migration files.
```sql

exec [changefeed].setup_feed @table_name = 'myservice.MyEvent', @blocking = 1;
alter role [changefeed.writers:myservice.MyEvent] add member service1;    
```

This stored procedure will generate tables and stored procedures tailored
for your table and allow the `service1` user to publish new events.

Unlike the outbox mode, there is no special support for readers; this is
for you to provide through your table design.

## About shard_id

Below there is a `shard_id` parameter. This can be set to any `int` number.
The usage of a new `shard_id` effectively creates an independent
event feed.

If you do not have very high volumes, simply hard-code `shard_id` to 0 everywhere.

For the blocking mode, the number of partitions needed should be primarily
be affected by how many partitions it is appropriate for consumers to have;
but it could also be used to reduce the time writers are blocked on waiting
for one another on the same partition on average. This wait time should be
a small problem, unless you are using transactions that take a lot of time
to complete.

## Publisher code

The idea is to generate the ULID for your event before you write
the event. So, for instance you may have a table 
similar to this:
```sql
create table myservice.MyEvent(
    Shard smallint not null,
    ULID binary(16) not null,
    UserID bigint not null,
    ChosenShoeSize tinyint not null,
    constraint pk_MyEvent primary key (Shard, ULID)
) with (data_compression = page);
```
To support reading from the feed you should make available a unique
index on `(Shard, ULID)`. In this case we make it the primary key of our table,
but it could also be a secondary key.

### Single ULID
To avoid the [power-off issue](POWEROFF.md) we use a server-side transaction
inside the SQL batch in our example:
```sql
-- Quite a lot of boilerplate to do safe error handling in MS-SQL;
-- see Erland Sommarkog's excellent guide one errors for more detail:
-- https://www.sommarskog.se/error_handling/Part1.html
set xact_abort on
begin try
    
    begin transaction

    -- Call a stored procedure to generate a ULID
    declare @ulid binary(16)
    exec changefeed.[lock:myservice.MyEvent] @shard_id = 0, @ulid = @ulid output;
    
    -- Proceed with inserting, generating ULIDs in the right order
    insert into myservice.MyEvent (Shard, ULID, UserID, ChosenShoeSize)
    values (0, @ulid, 1234, 42);
        
    commit

end try
begin catch
    if @@trancount > 0 rollback;
    ;throw;
end catch
```

The call to `lock:*` takes a lock so that other writers to the same
shard will block and wait. It also returns a single generated ULID.

## Multiple ULIDs

### Using the ulid() function
It is possible to generate more ULIDs (millions). So the above could
also look like this (skipping the transaction boiler plate). There is a
function `changefeed.[ulid:myservice.MyEvent]` which takes a single integer
argument; by passing it 1, 2, and so on you can generate new ULIDs.
For instance:
```sql
declare @ulid binary(16)
exec changefeed.[lock:myservice.MyEvent] @shard_id = 0, @ulid = @ulid output;
    
-- Proceed with inserting, generating ULIDs in the right order
insert into myservice.MyEvent (Shard, ULID, UserID, ChosenShoeSize)
select 
    0,
    changefeed.[ulid:myservice.MyEvent](row_number() over (order by myinput.Time),
    myinput.UserID,
    myinput.ChosenShoeSize
from myinput;
```
Calling `lock:*` allocates 100000000000 (10^11) ULIDs under the hood,
so your argument to the function should stay lower than this.

If you happen to have a 2nd statement in the same database transaction
that needs ULIDs, there are two ways to do that.
Either you can call `[lock:*]` again, to allocate a new range.
Or, simply add a large safe constant, like this:

```sql
select ...
    changefeed.[ulid:myservice.MyEvent](row_number() over (order by i.UserID) + 1000000),
...
```

### Low-level version
The way the routine above works is by setting some state as a context variable.
If you prefer to avoid that, you can do the exact same thing
as above the low-level way like this:
```sql
declare @ulid_high binary(8)
declare @ulid_low bigint
exec changefeed.[lock:myservice.MyEvent] @shard_id = 0, @session_context = 0, @ulid_high = @ulid_high output, @ulid_low = @ulid_low output;
```
The `@session_context` argument is not really needed, just in case you wish
to keep the context clean.
The two components can be combined into a ULID like this:
```sql
@ulid_high + convert(binary(8), @ulid_low)
```
Now it is possible to arbitrarily generate ULIDs any way you like by adding
to the `@ulid_low` offset. As above, the number you add should stay below
10^11. 

## Consumer code

Consumers simply executes queries on the ULID generated by the publisher.
Continuing on the example above:
```sql
declare @cursor binary(16) = (select MaxReadULID from MyStateTable where Shard = 0);

insert into @pageOfData
select top(1000) * from myservice.MyEvent
where ULID > @Cursor and Shard = 0
order by ULID;

begin transaction 
   -- process the @pageOfData

   -- also update the state table in the same transaction:
   update MyStateTable
   set MaxReadULID = (select max(ULID) from @pageOfData)
   where Shard = 0;

commit
```

