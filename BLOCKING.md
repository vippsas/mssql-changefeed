# mssql-changefeed: User guide for blocking mode
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
    exec changefeed.[lock:myservice.MyEvent] @shard_id = 0;
    
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