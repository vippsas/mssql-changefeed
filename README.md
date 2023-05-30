# mssql-changefeed: Changefeed SQL library for SQL Server

## Reusable outbox pattern

The Outbox pattern is often used to make sure that events are exported
to an event broker if and only if the change is also committed to the SQL database.

The mssql-library builds on the Outbox pattern to provide an in-database
change feed. This allows the same Outbox to be used several times for different
purposes; or for e.g. both re-constitution and live event export using
the same building block.

In a sense, mssql-changefeed is like having a "mini-Kafka" in the form
of SQL tables;  this can then be useful for several things such as:
* Ordered export of events to brokers
* APIs to allow fetching old or new events (such as [ZeroEventHub](https://github.com/vippsas/zeroeventhub)).
* In-database event processing / computation / data flow patterns

Why not simply use a `Time` column, or an integer `RowId int identity`
column for these purposes? Because race conditions will make it unsafe
to consume newer events. See [MOTIVATION.md](MOTIVATION.md) for further description
of the basic problem this library solves.

### About ULIDs

mssql-changefeed makes an opinionated choice of using [ULIDs](https://github.com/ulid/spec)
as "event sequence numbers". This allows for generating cursor values
that embed a timestamp, and you get unique IDs for your events that are ensured
to be in sequence. Also, the use of ULIDs makes changing the number of partitions
a much easier affair.

The library would have worked equally well with integer sequence numbers;
it could be made an option in the future.

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
exec [changefeed].setup_feed @table = 'myservice.MyEvent', @shard_key_type = 'uniqueidentifier';
alter role [changefeed].[role:myservice.MyEvent] add member myservice_user;    
```
Calling this stored procedure will generate tables
and stored procedures tailored for your table; in particular
the primary keys of the table is embedded. The following
are generated; further details provided below.
You should inspect them to ensure that they make sense,
in particular that the primary key has been correctly
detected and generated.

* `[changefeed].[outbox:myservice.MyEvent]`: Outbox table, for changes
  that have never been read
* `[changefeed].[feed:myservice.MyEvent]`: Feed table, for older changes
  that have been read once
* `[changefeed].[read_feed:myservice.MyEvent]`: Stored procedure to read
  events (new or old)
* `[changefeed].[type:myservice.MyEvent]`: Type documenting the result of `read_feed`
* `[changefeed].[role:myservice.MyEvent]`: Add members to this role to grant
  permissions for the above.

### Writing events and the outbox table

To use mssql-changefeed, the main thing you have to ensure is that
writes happen to `[changefeed].[outbox:myservice.MyEvent]` when appropriate.
For a typical event sourcing table this would happen with every INSERT
to your table. The insert can happen using a SQL trigger or by changing your
backend code; the details of achieving this is left up to you.

The outbox tables has the following columns:

* `shard_id`: Used to increase throughput; so that consumers can consume/process
  several partitions at the same time, in line with the "partitioned log"
  model. If you don't wish to have several partitions, simply always pass 0.
  If you wish to have several partitions, you want to do something like
  `aggregate_id % shard_count`; the sharding mechanism is left entirely to you.
* `time_hint`: The time component that should be used in the generated ULIDs.
  It is a *hint*, as it will not always be honored; making ULIDs monotonically
  increase within each shard takes precedence. Pass the current time
  for this.
* `shard_key` and `ordering`: Used to explicitly demand an ordering of
  events on the feed that will never be violated, even if `time_hint`
  is giving a different order. The `shard_key` is meant to identify
  your "entity"/"aggregate" and has a configurable type (defaults
  to `bigint`); specifically, every event with the same `shard_key`
  should be on the same shard. Then `ordering` (always `bigint`) specifies
  an ordering *within* each `shard_key`. If these concepts do not make
  sense in your application; for instance, simply always pass `0` for both
* values.
* *Primary keys*: The primary key column(s) of your table. These are simply
  copied over to the feed table, and returned in the result. Essentially
  these are the payload of the outbox/feed mechanism.




## Still curious?

Head over to the [experts guide](EXPERTS-GUIDE.md).

## Versions
Note: Version 1 used a rather different approach. It is
still available on the [v1 branch](TODO). Compared to v2, it:

* Requires a sweeper to run in the background, instead of moving
  between outbox and feed tables on access
* Uses integer cursors instead of ULIDs

