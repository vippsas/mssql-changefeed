# mssql-changefeed: MS-SQL library for in-database event "broker"

## Event broker in SQL
The mssql-changefeed library provides a "mini event broker", or pub/sub features,
inside of your
Microsoft SQL database; by provisioning a few SQL tables and providing
race-safe patterns around using them. This can be useful for code executing
on your SQL server, or as a building block to export events from your
service (whether live events or older, historical events).

The "Outbox pattern" is often used to export live events. This library
can bee seen as a library providing "re-usable outboxes";
allowing to use the outbox several time for exporting events to several
places, or re-publishing older events.

The model used is the "partitioned log" model popularized by Kafka.
Log based event brokering is very simple in principle,
as the idea is the publisher simply stores events in a log table
(one log per shard/partition).

One may think that implementing this pattern is as simple as having an auto-increment
"RowID", such as
```sql
create table myservice.MyEvent (
    RowID int not null identity,
    ...
)
```
and then consumers may fetch a new set of events by
```sql
select ... where RowID > @LastReadRowID`
```
This is however not safe. There will be
race conditions between the publisher side and the consumer side
(assuming inserts to the table is done in parallel).
The mssql-changefeed library provides race-safe patterns for achieving
an event log like this. See [MOTIVATION.md](MOTIVATION.md) for further descriptions
of the race condition.


### About ULIDs

mssql-changefeed makes an opinionated choice of using [ULIDs](https://github.com/ulid/spec)
as "event sequence numbers". The disadvantage of this scheme is the storage cost.
The advantages are:
* Since each event ID embeds a timestamp, it will always be possible to navigate to a set
  of historical events based on time
* It is easier to change the number of partitions
* It's never a problem to backfill older events into a feed, and similar cases

The library would have worked equally well with integer sequence numbers;
this could be made an option in the future.

The ULIDs are stored as `binary(16)`, not in the the string crockford32
encoding.


### Feed types

The library provides two different mechanisms to maintain a feed:

* Outbox pattern
  * Pro: Writers are not blocked and operate fully in parallel
  * Con: The assigned ULIDs is not available at the time of the write
    happening.
  * Con: Consuming the feed happens using a stored procedure
* Serialize writers. Only one writer gets a lock at the time, for each
  feed and partition.
  * Pro: The ULID is known at the time of writing, so it can for instance be used as a primary key.
  * Pro: The feed can safely be consumed directly by querying the table on the ULID column,
    without using a stored procedure
  * Con: You risk [blocking all writes for 60 seconds](POWEROFF.md) if you use
    client-side transactions.

Which you use depend on the usecase. The former is likely best for typical backend
APIs that insert individual events. The latter may perform better for high-through
batch-style processing where each SQL statement touches 1000+ rows. If you are in
doubt, and do not need to use the ULID as the primary key of your table,
go with the Outbox pattern.

## Installation

Copy the file(s) in the `migrations` directory and run them in the database.
The standard is to create tables and stored procedures in a `changefeed`
schema (the migration file is written such that you can globally search and replace
`[changefeed]` to change the schema name).

## Usage

### Feed setup

You set up a new feed by calling a stored procedure,
passing in the name of a table, in one of your own
migration files.
```sql
exec [changefeed].setup_feed @table = 'myservice.MyEvent', @shard_key_type = 'uniqueidentifier', @outbox = 1;
alter role [changefeed].[role:myservice.MyEvent] add member myservice_user;    
```

For the outbox pattern, pass `@outbox = 1` like above, while for the serialized writers mode
pass `@serialize_writers = 1`.

Calling this stored procedure will generate tables
and stored procedures tailored for your table; in particular
the primary keys of the table is embedded.

### Outbox pattern

* Pro: Writers are not blocked
* Con: Generated ULIDs not available at the time of write

Call `setup_feed` using `@outbox=1`, as in the example above.

Whenever you insert into your main event table, you must make
sure to always also insert into an *outbox table*:
```sql
-- the event table looks however you want; in this example,
-- the primary key is (AggregateID, Version)
insert into myservice.MyEvent (AggregateID, Version, ChosenShoeSize)
    values (1000, 1, 38);

-- Also always insert into the outbox. The last columns
-- are the primary key of your table
insert into [changefeed].[outbox:myservice.MyEvent] (shard_id, time_hint, AggregateID, Version)
values (1000 % 2, '2023-05-31 12:00:00', 1000, 1);
```
You can do this extra insert either in the backend code, or by using a trigger in SQL
(that you provide). Make sure both inserts happen in the same database
transaction!

Now, the ULIDs which determines the final ordering of the event
on the feed is actually not generated yet. This is generated *the first*
time the feed is read; and to organize this, all readers need to consume
the feed by calling a stored procedure. Example read:
```sql
create table #read (
    -- always here:
    ulid binary(16) not null,
    -- primary keys of your particular table:
	AggregateID bigint not null,
	Version int not null	    
);
exec [changefeed].[read_feed:myservice.MyEvent] @shard, @cursor, @pagesize
select * from #read as r
join myservice.MyEvent as e
    on r.AggregateID = e.AggregateID and r.Version = e.Version 
order by ulid;
```
To consume the feed, the maximum `ulid` returned should be stored and used as `@cursor` in the next
iteration. The `changefeed.read_feed:*` procedure will first attempt
to read from `changefeed.feed:*`. If there are no new entries, it will process the
`changefeed.outbox:*` table, assign ULIDs, and both write the rows
to `changefeed.feed:*` for future lookups as well as returning them.

You should not insert into `changefeed.feed:*` directly, unless if you are
backfilling old data. Such data inserted manually into the feed will not be
seen by currently active consumers reading from the head of the feed. Never
insert near the head of `changefeed.feed:*` as you risk triggering race conditions.

### Serializing writers

* Pro: Generated ULIDs available at time of write / before commit
* Con: Writers are serialized (within their allocated `shard_id`)
* Con: If you have client-managed transactions, you run the risk of
  [blocking all writes for 60 seconds](POWEROFF.md).

In this method, we generate the ULIDs *before* they are needed
and simply write them as ordinary data. This means you will have some table
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
index on `(Shard, ULID)`, in this case we make it the primary key of our table.

Then when setting up the feed you
want to use the `@serialize_writers=1` option instead:

```sql
exec [changefeed].setup_feed @table = 'myservice.MyEvent', @shard_key_type = 'uniqueidentifier', @serialize_writers = 1;
```
(The state table is under the hood the same, so it would be possible in the future to develop
some compatability so that *both* approaches can be used at the same time,
if needed.)

To avoid the [power-off issue](POWEROFF.md) we use a server-side transaction
inside the SQL batch in our example:
```sql
-- Quite a lot of boilerplate to do safe error handling in MS-SQL;
-- see Erland Sommarkog's excellent guide one errors for more detail:
-- https://www.sommarskog.se/error_handling/Part1.html
set xact_abort on
begin try
    
    begin transaction

    -- The first thing we do in our transaction is lock our shard for writes by us.
    exec changefeed.[lock_feed:myservice.MyEvent] @shard_id = 0;
    
    -- Proceed with inserting, generating ULIDs in the right order
    insert into myservice.MyEvent (Shard, ULID, UserID, ChosenShoeSize)
    select
        changefeed.ulid(next value for changefeed.sequence over (order by i.Time))
    from @inputtable as i;
        
    commit

end try
begin catch
    if @@trancount > 0 rollback;
    ;throw;
end catch
```

The call to `lock_feed:*` takes a lock so that other writers to the same
shard will stop at that location (serializing writers). After this call,
it is possible to call `changefeed.ulid(next value for changefeed.sequence)`.
All the ULIDs generated in the same database transaction will have the same
timestamp.

In the example above, the `over` clause is used on `next value for` t
to generate a single ULID, or use `changefeed.ulid(next value for changefeed.sequence over (order by ...))`
to generate a large number of UILD

which generates any number of ULIDs. See reference for more
details.




## Still curious?

Head over to the [experts guide](EXPERTS-GUIDE.md).

## Versions
Note: Version 1 used a rather different approach. It is
still available on the [v1 branch](TODO). Compared to v2, it:

* Requires a sweeper to run in the background, instead of moving
  between outbox and feed tables on access
* Uses integer cursors instead of ULIDs

