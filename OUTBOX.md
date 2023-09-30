# mssql-changefeed: User guide for outbox mode

Please see [README](README.md) for an overview of the mssql-changefeed
library, and a comparison of the *outbox mode* documented below with
the [blocking mode](BLOCKING.md).

## Setup migration

You set up a new feed by calling the `setup_feed` stored procedure,
passing in the name of a table and `@outbox = 1`, in one of your own
migration files.
```sql
exec changefeed.setup_feed @table = 'myservice.MyEvent', @outbox = 1;
alter role changefeed.[writers:myservice.MyEvent] add member service1;
alter role changefeed.[readers:myservice.MyEvent] add member service2;
```

This stored procedure will generate tables and stored procedures tailored
for your table, allow the `service1` user to publish new events, and the
`service2` user to consume the event feed.

## About shard_id

Below there is a `shard_id` parameter. This can be set to any `int` number.
The usage of a new `shard_id` effectively creates an independent
event feed.

If you do not have very high volumes, simply hard-code `shard_id` to 0 everywhere.

For the outbox mode, you should primarily worry how many partitions
the consumers would like to have; there is little need to increase the
number of partitions to increase write throughput.

## Publisher code

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

You may add this extra insert either in the backend code, or by using a trigger in SQL.
Whatever you do, make very sure that both inserts happen in the same database transaction!
The example above is hard-coded to use two partitions.

The new row in `[outbox:myservice.MyEvent]` will have:
* An `order_sequence` number generated, using a regular SQL sequence.
  * The `order_sequence` column is generated automatically with a default constraint. 
* The `time_hint` says which timestamp should *ideally* be embedded in the ULID.

These variables are hints, but do not fully determine the *final, race-free event sequence*:
* The ULID can embed a later timestamp than `time_hint`, if this is needed to
  honor the ordering in `order_sequence`.
* If races happen between writers and readers in a particular way, it could be that
  some events are re-ordered slightly with respect to `order_sequence` in the final ordering. 

The specific contract provided is:
* If a transaction A1 commits first, then another transaction A2 starts after A1
  is known to have been committed, *then* the events from A2 will *always* be ordered
  after the events from A1.
  * This is the important guarantee to have
* If two transactions A and B run in parallel, then events may be reordered in the end
  w.r.t. the `order_sequence` allocated in the transaction.
  * This should be fine in almost all cases, as it is in general unknown in general which
    of A and B will commit first anyway.

## Consumer code

The actual event ordering is done by the *first reader*. On the *first read*
of an event, the reader will copy it from `[outbox:myservice.MyEvent]`
to `[feed:myservice.MyEvent]`.

To facilitate this one needs to use a stored procedure to consume the feed.
The stored procedure wraps the whole process, so the consumer can act
exactly the same whether it is the 1st or the 10th consumption of the
feed.

Example usage to get one page of 100 events:
```sql
create table #read (
    -- always here:
    ulid binary(16) not null,
    -- primary keys of your particular table:
    AggregateID bigint not null,
    Version int not null	    
);

exec changefeed.[read_feed:myservice.MyEvent] @shard_id = 0, @cursor = @cursor, @pagesize = 100;

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

## Still curious?

Head over to the [experts guide](EXPERTS-GUIDE.md).