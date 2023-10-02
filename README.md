# mssql-changefeed: MS-SQL library for in-database event "broker"

The mssql-changefeed library provides a "mini event broker", or pub/sub features,
inside of your Microsoft SQL database. This allows you to use programming patterns
known from Kafka etc., in a convenient (enough) way.

The "partitioned log" model was popularized by Kafka.
Log based event brokering is very simple in principle,
as the idea is the publisher simply stores events in a log table.
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
However, this is not race safe. There will be
race conditions between the publisher side and the consumer side
(assuming inserts to the table is done in parallel).
The mssql-changefeed library provides race-safe patterns for achieving
an event log like this. See [MOTIVATION.md](MOTIVATION.md) for further descriptions
of the race condition.

## Installation and usage

The Go code in this library *is only for testing purposes*. The library
itself is in pure Microsoft SQL.

To install it, execute the file [migrations/2001.changefeed-v2.sql](migrations/2001.changefeed-v2.sql)
on your SQL server. This will create and populate the `changefeed` schema.

Further usage depends on which of the two available modes you use, as described below.

## Fundamental concept: Assign ULIDs to events in a race-safe manner.

The fundamental idea is to store your events in a regular SQL table, much
like the example above. 

Event writers `insert` a row to a table, and also make use of mssql-changefeed
to assign an event sequence number in a race-safe manner.
 
Consumers keep track of a *cursor* for each *partition*. Running one loop for each
partition, they query for events newer than the current cursor, and process the events
and update the cursor (ideally in the same database transaction, for *exactly-once*
processing).

mssql-changefeed makes an opinionated choice of using [ULIDs](https://github.com/ulid/spec)
as event sequence numbers. The disadvantage of this scheme is the storage cost.
The advantages are:
* Since each event ID embeds a timestamp, it will always be possible to navigate to a set
  of historical events based on time
* It is easier to change the number of partitions
* It's never a problem to backfill older events into a feed, and similar cases

The library would have worked equally well with integer sequence numbers;
this could be made an option in the future.

There is no built in type for ULID in MS SQL, so we simply use the type `binary(16)`.
Encoding/decoding to crockford32 is strictly optional and should be done on your backend;
but the bits stored in the `binary(16)` are exactly according to the ULID specification;
in particular the first 6 bytes contains an encoded timestamp.

There is a [separate page about how we generate ULIDs](ULID-NOTES.md).

## Which mode to use?

We have gone through *many* iterations of possible patterns to do race-safe, Kafka-like
event processing on SQL tables. In the end there wasn't a single winner and we provide two
different mode. For most needs, *both* methods will do the job nicely.

* [Blocking mode](BLOCKING.md):
  * The idea is to serialize writers (for each partition) using traditional `update` locking
    * Scaling up throughput requires increasing number of partitions
  * This is the "least intrusive" method, simplest to understand and use
  * Recommended option for batch-like processing
  * If you use client-side transactions, you risk [blocking all writes for 60 seconds](POWEROFF.md)
    * May not be appropriate for very high-uptime scenarios combined with client-side transactions
    * Note: Client-side transactions are seldom needed; stored procedures running transactions
      will usually lead to higher performance as they reduce the number of network roundtrips.

* [Outbox pattern](OUTBOX.md) mode:
  * The idea is that the writer stores events in a special semi-ordered outbox data structure;
    in such a way that ULIDs can be assigned by the first reader.
  * A bit harder to learn to use; in particularly on the consumption side
  * Writers are more decoupled from mssql-changefeed library
    * Risk of blocking all writes for 60 seconds is eliminated; client-side transactions
      can safely be used
    * If the number of partitions is too low, it is primarily the readers that are hurt,
      writers are less affected by locks.
  * Problem: The ULIDs assigned are not available at the time of doing the insert
    * As a consequence the ULID can not be used as a primary key

Each mode has its own user manual, so please click one of the links above.


## Versions
Note: Version 1 used a rather different approach. It is
still available on the [v1 branch](TODO). Compared to v2, it:

* Requires a sweeper to run in the background, instead of moving
  between outbox and feed tables on access
* Uses integer cursors instead of ULIDs

