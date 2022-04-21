# mssql-changefeed usage

Scenario:

* Your backend generates events that other services, or your own workers,
  should consume
* To ensure correctness, you need to commit to SQL before the event can
  be published
* Possibly your service exposes the same events on multiple "feeds",
  where different client is interest in a different feed 

To support this we have the following steps:

1. Set up the feeds
2. Insert changes
3. Set up the sweepers
4. Read changes

Also, optionally,

5. longpolling.

## Setting up feeds

This library supports any number of feeds, each with any number of shards.
On a technical level each `(feed_id, shard)` combination is a distinct
change feed published to and consumed isolated from others. Within each feed
all events are assigned a given order.  Between different feeds there
are no ordering guarantees.

The ID `feed_id` is something you allocate in your application. In
low-traffic scenarios (a few dozen events per second) it may be
entirely appropriate to have a single `feed_id=0` for your entire
database, this gives reader a consistent view across all your tables.
You can also have one per table, or group of related tables that together
make up "an event".

**Consider needs for ordering guarantees.** If you have 3 types of
events, but they are related to one another and the consumer needs to
receive them in a given order, then they need to use a common
`feed_id`, and also be assigned to shards in such a way that
events that are related end up on the same shard.

The `shard` is used in scenarios where you want to provide multiple
parallel feeds to consumers so that consumers can process in parallel
to achieve high throughput. There really isn't a technical difference
from using more than one `feed_id` for the same table; having it as a
separate field is just for convenience. It is fine to just always set
`shard=0` unless you know you need it. Note that some systems (e.g. Azure
Event Hub) has a minimum partition count of 2 (in order to ensure everyone
writes scalable applications); and if you are going to publish events
to such systems a minimum of 2 shards may be appropriate.

To use a feed you insert into tables. Assume we have defined `37` as a constant
in our service for the `myevent` feed, we get:

```sql


insert into changefeed.feed(feed_id, name, comment) values
  (37, 'myevent', 'Inserts to myscheme.MyEvent and myscheme.MyEventExtraDetails');

insert into changefeed.shard(feed_id, shard /* + more optional, see below */) values
  (37, 0),
  (37, 1);
  
```

---

*NOTE*

We recommend one feed per "concept", "type of event" or similar. You should
probably *NOT* make one feed per "customer", for instance. Instead, consider
making an indexed joined view that joins `change_sequence_number`
with e.g. `user_id`; then you will get both the feed of all your changes 
and a feed per customer.


### changefeed.feed

* `feed_id`: **primary key**, an enum value used in your service for the feed
* `name`: **required**, a name. Please keep this `[a-zA-Z0-9_-]+`.
   Intention is to use this in
   frameworks/libraries higher in the stack for e.g. automatically exposing 
   a feed over http.
* `comment`: **optional**, up to you

### changefeed.shard

* `feed_id`, `shard`: **primary key**, `feed_id` references the feed and
  shard can be any number. We recommend using 0,...,n and using a number
  of shards that is a power of 2.
* `sweep_group`: **optional**, groups together feeds/shards that share
  the *sweeper*, documented below. The default is to share a single global
  sweep group `0` and this is appropriate for low volume feeds. In high 
  volume, low-latency scenarios one can give each feed, or shard, a dedicated
  sweeper by assigning a unique number and using the same number when
  calling the sweeper. **NOTE**: There should not be a large number of shards
  per `sweep_group`, only a dozen or so, especially if longpolling
  is turned on.
* `longpoll`: **optional**, defaults to 1. This is a setting that configures
  whether the shard should support longpolling; this adds a tiny bit of
  overhead to the sweeper. Leave it to 1 unless you know you will never
  need longpolling and need very high volumes/low latency.
* `last_change_sequence_number`: **optional**, defaults to 2000000000000000.
  The value before the first `change_sequence_number` that will be allocated
  (see below). This high starting value is the default in case you want to
  migrate existing data into the feed at lower values.

## Inserting changes

Central to inserting a change into a feed is the `change_id`.
This concept is present so that it is possible to have a single
scheme work across all tables in the database; as a sort of global foreign key
into any (relevant) row in any (relevant) table.

Typically each INSERT will also write a `change_id`, but it is possible
to be creative and also use `change_id` when you UPDATE data.

To allocate a `change_id` we use a single global counter `changefeed.change_id`.
If your needs are very special you can use another scheme as long as everyone
using the same database uses it; the sequence is not relied on in any way
as long as all changes get a unique ID.

---
**STEP 1**

Your own tables need the `change_id` with an index:

```sql
create table myschema.MyEvent (
    ...,
    change_id bigint not null
);

create unique index change_id on myschema.MyEvent(change_id) with (online=on);
```

We recommend using exactly `change_id` as the name, despite your own
naming conventions, so that any libraries that are written to work on top
of mssql-changefeed, e.g. to expose REST APIs, can automatically use it.

---
**STEP 2**

Whenever one inserts into the table, you also insert into `changefeed.change`:

```sql
begin transaction

declare @change_id bigint = next value for changefeed.change_id;
insert into myschema.MyEvent (..., change_id) values
  (..., @entityID, ..., @change_id);
insert into changefeed.change (feed_id, shard, change_id) values
  (37, @entityID % 2, @change_id);

commit
```

The last insert could be done using a trigger. Notice that we here assume
two shards are in use, and shard by some EntityID that we assume is a part
of MyEvent.

*NOTE:* Nothing stops you from inserting a given change/event into several feeds
at the moment of insertion; simply insert one `changefeed.change` per feed.


---
**STEP 3**

The insert into `changefeed.change` acts as a *request to be put on a feed*
(see [MOTIVATION.md](MOTIVATION.md) for why it is not safe to put the change
directly into a feed structure -- then this library would not be needed).
What will happen next is that the **sweeper** will need to be executed
in order to assemble *committed* changes into feeds.

### Consideration to partitioning and event ordering

It is important to always make sure that consumers sees events in a correct
order. The common case is one is publishing events about some MyEntity, and
the it's important that events with the same MyEntityID arrive in a given
order, while ordering between events concerning different MyEntityID does
not need to be ordered.

To make such guarantees you need to take care of two things:

* When you pick a `shard` (assuming you have more than one) you should
  pick it deterministically from the MyEntityId ("partition key")
* When you pick change_id, that pick should be *increasing* with respect
  to MyEntityId:

Specifically,

```sql
select * from MyEventAboutMyEntity
where EntityId = @EntityID
order by change_id
```

should return events in the order they happened. The simple use
of a SQL sequence (`next value for changefeed.change_id`) takes care of this,
so you normally do not need to worry, but if you have more creative allocation
of  change_id you need to keep this in mind.

## Set up the sweeper

### About change_sequence_number

With the setup above we have organized all our changes into a
`changefeed.change` table and, with an increasing `change_id`, already
have a sorted list of changes within each feed. However, using this
directly would not be race-safe.  This is because `change_id` is
allocated in an active transaction before the transaction commits, and
for safe reading we need an ID that is allocated *after* the event has
committed.

The `changefeed.change` table also has a `change_sequence_number`
column which should be left `null` when inserting. We then
employ a worker (not very different from Microsoft's
built-in CDC worker..)  which will populate the
`change_sequence_number` column, **after** the initial transaction
has committed. The worker simply scans for new entries in
`changefeed.change` (which have `change_sequence_number is null`)
and assigns `change_sequence_number`.

The sequence number:

* Is unique within a `(feed_id, shard)` combination, but *not* across feeds/shards

* Is an increasing sequence starting with 2000000000000000
  (or a value you provide when
  inserting into `changefeed.shard`) without any holes.

### Running the sweeper

* The sweeper must be run by you, in some service you control, in a loop that runs forever
  in a background thread.

* The sweeper is without any overhead to your service; uses 0% CPU and 1 DB connection.
  You simply do a stored procedure call with a timeout
  (e.g., 50 seconds), and the routine runs for 50 seconds then give control
  back to you. Then you probably want to do some logging, before you
  call the routine again and is blocked for 50 more seconds waiting for
  an IP packet back.

* Consumes some SQL server resources. There is a sleep flag that can
  be tuned for resource vs latency.

* You can/should run many instances in parallel on many service instances/pods;
  and one such set for each `sweep_group` in use.
  Locks are used (in SQL) in such a way that only one pod per `sweep_group`
  will be doing work; the others will stick block and wait for a chance
  to take over.

To implement sweeping simply call `changefeed.sweep_loop` procedure
in a loop. Parameters:

* `@sweep_group`: What group of shards to sweep, see `shard.sweep_group` above
* `@wait_milliseconds`: Timeout to wait for taking over, before sweeping.
  Recommended: 30 seconds = `30000`. This is the earliest point of return,
  if the sweeper is not doing any work and only being a backup.
* `@duration_milliseconds`: Time before returning even if work is being done.
  Recommended: 50 seconds = `50000`.
* `@change_count`, `@max_lag_milliseconds`, `@iterations`: Output parameters.
  Log these.

## Consuming events

At this point we can consume the events using the pattern below.
We maintain a *cursor* in the consumer's state that we use to read pages of events.
To read one queries `changefeed.change` and joins with the application tables:


```sql
declare @feed_id smallint = 37;  -- just part of your contract with the producer

-- used for parallelization of readers; number of shards hardcoded by producer
-- as it must be supported by SQL indexes
declare @shard smallint = 0;
   
-- You have a cursor from the last time you ran...this can often be another DB:
declare @cursor bigint = (select isnull(seqnum_cursor, 0) from ConsumerStateTable where shard = @shard)

select
  top(100)
  *
  into #result
from myschema.MyEvent e
join changefeed.change change on change.change_id = e.change_id
where
  e.change_sequence_number > @cursor
  and e.shard = @shard
  and e.feed_id = @feed_id;


-- Persist the cursor together with your changes; this can be in another DB
begin tran
  -- Use the #result somehow, and also:

  update ConsumerStateTable
  set seqnum_cursor = (select max(change_seqnum) from #result)
  where shard = @shard;
     
commit 
```


## Longpolling

For continuous processing, the simplest is to execute the consumer above in a loop.
However for rare events, longpolling may be preferred.

Because Microsoft SQL doesn't provide many primitives to work with, but we
have a working implementation. To avoid a tricky race condition two stored
procedures must be used in two SQL connections to set up a long-poll.
A reference implementation is available in [longpoll.go](longpoll.go).