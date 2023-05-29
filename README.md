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
exec [changefeed].setup_feed 'myservice.MyEvent';
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

### Usage: Writing events

To use mssql-changefeed, the main thing you have to ensure is that
writes happen to `[changefeed].[outbox:myservice.MyEvent]` when appropriate.
For a typical event sourcing table this would happen with every INSERT
to your table. The insert can happen using a SQL trigger or by changing your
backend code; the details of achieving this is left up to you.

The outbox tables has the following columns:

* `shard`: Used to increase througput; so that consumers can consume/process
  several partitions at the same time, in line with the "partitioned log"
  model. If you don't wish to have several partitions, simply always pass 0.
* `time_hint`: The time component that should be used in the generated ULIDs.
  It is a *hint*, because, if you pass an older time than events
  already processed from the outbox, it will not be honored, since
  generated ULIDs must always increase (within a shard).
* `order_after_time`: A number that specifies ordering of events.
  If yor primary key is a simple `(AggregateID, Version)`, then
  simply pass `Version`.
* *Primary key*: The primary key column(s) of your table. This is *also included in the sort order*,
  at the end.

If ordering of events matter to you, please make sure that
`(time_hint, order_after_time)` reflects this ordering. This may
include making sure that a new event on the same aggregate is using
a later timestamp *even in the event of clock drift between nodes*.





## Still curious?

Head over to the [experts guide](EXPERTS-GUIDE.md).

## Versions
Note: Version 1 used a rather different approach. It is
still available on the [v1 branch](TODO). Compared to v2, it:

* Requires a sweeper to run in the background, instead of moving
  between outbox and feed tables on access
* Uses integer cursors instead of ULIDs

