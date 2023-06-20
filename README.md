# mssql-changefeed: Changefeed SQL library for SQL Server

## Overview

### Mini event broker in SQL
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

Why is this not as simple as having a "RowID", such as
```sql
create table myservice.MyEvent (
    RowID int not null identity,
    ...
)
```
and then simply have consumers `select ... where RowID > @LastReadRowID`?
The problem with such an implementation is that there will be
race conditions between the publisher side and the consumer side
(assuming inserts to the table is done in parallel).
This is the fundamental problem solved by mssql-changefeed.
See [MOTIVATION.md](MOTIVATION.md) for further descriptions
of the race condition.


### About ULIDs

mssql-changefeed makes an opinionated choice of using [ULIDs](https://github.com/ulid/spec)
as "event sequence numbers". This allows for generating cursor values
that embed a timestamp, and you get unique IDs for your events that are ensured
to be in sequence. Also, the use of ULIDs makes changing the number of partitions
an easier affair.

The library would have worked equally well with integer sequence numbers;
it could be made an option in the future.

The ULIDs are stored as `binary(16)`, not in the the string crockford32
encoding.

## Installation

### Migrations
Copy the file(s) in the `migrations` directory and run them in the database.
The standard is to create tables and stored procedures in a `changefeed`
schema (the migration file is written such that you can globally search and replace
`[changefeed]` to change the schema name).

### Feed setup

You set up a new feed by calling a stored procedure,
passing in the name of a table, in one of your own
migration files.
```sql
exec [changefeed].setup_feed @table = 'myservice.MyEvent', @shard_key_type = 'uniqueidentifier', @outbox = 1;
alter role [changefeed].[role:myservice.MyEvent] add member myservice_user;    
```
Calling this stored procedure will generate tables
and stored procedures tailored for your table; in particular
the primary keys of the table is embedded.
The [REFERENCE.md](REFERENCE.md) has more information about
what is generated.

## Usage

There are two different options for how to use the library, which one to use
depends on your context. For most real world usecases, *both* are acceptable.

* Outbox pattern
  * Pro: Writers are not blocked and operate fully in parallel
  * Con: The assigned ULIDs is not available at the time of the write
    happening.
* Serialize writers. Only one writer gets a lock at the time, for each
  feed and partition.
  * Pro: The ULID is known at the time of writing, so it can for instance be used as a primary key.
  * Con: You risk [blocking all writes for 60 seconds](POWEROFF.md) if you use
    client-side transactions.

In general, choose the outbox pattern for "OLTP" workloads where you do lots
of single-row inserts in parallel, and to serialize writers for more
batch-oriented workloads writing larger chunks of data at the time.


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
You can do this extra insert either in the backend code, or using a trigger in SQL
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
to read from `changefeed.feed:*` (which you should never insert into directly).
If there are no results, it will process the
`changefeed.outbox:*` table, assign ULIDs, and both write the rows
to `changefeed.feed:*` for future lookups as well as returning them.

### Serializing writers

* Pro: Generated ULIDs available at time of write / before commit
* Con: Writers are serialized (within their allocated `shard_id`)
* Con: If you have client-managed transactions, you run the risk of
  [blocking all writes for 60 seconds](POWEROFF.md).

In this method, we generate the ULIDs *before* they are needed
and simply write them as ordinary data. When setting up the feed you
want to use the `@serialize_writers=1` option instead:

```sql
exec [changefeed].setup_feed @table = 'myservice.MyEvent', @shard_key_type = 'uniqueidentifier', @serialize_writers = 1;
```
Note: The state table is under the hood the same, so it would be possible in the future to develop
some compatability so that *both* approaches can be used at the same time,
if needed.

To avoid the [power-off issue](POWEROFF.md) we use a server-side transaction
inside the SQL batch in our example:
```sql
begin try
    
    begin transaction

    exec changefeed.[lock:myservice.MyEvent] @shard_id = 0;
        
    insert into myservice.MyEvent (ULID, UserID, ChosenShoeSize)
    select
        changefeed.ulid(next value for changefeed.sequence over (order by i.UserID))
    from @inputtable as i
    values (1000, 1, 38);
        
    commit

end try
begin catch
    if @@trancount > 0 rollback;
    ;throw;
end catch
```

The call to `ulid_begin:*` takes a lock, so that other writers to the same
shard will stop at that location (serializing writers). After this call,
it is possible to call `changefeed.ulid(next value for changefeed.sequence)`
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

