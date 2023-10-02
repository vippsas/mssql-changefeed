# Reference on stored procedures

## Common
All functions take in the parameters
* `feed_id`: A `uniqueidentifer` that specifices your feed, 
* `shard_id`: A sub-division of a feed.

The combination `(feed_id, shard_id)` specifies the specific feed partition
to which we serialize writes.

See [README.md](README.md) for more details on these.

## Generate ULIDs within a changefeed ULID transaction



## Setting up changefeed ULID transaction

### begin_ulid_transaction / commit_ulid_transaction 

These allows for using the `changefeed.ulid(... changefeed.sequence)` tools
documented above. Control can pass back to the client, but only if the client
follows the protocol explained.



## One-shot ULID allocation

### make_ulid_in_batch_transaction

This function is provided for one-shot ULID allocation. It is important that control
does not pass back to the backend, but is executed in a single SQL batch.

```sql
create procedure changefeed.make_ulid_in_batch_transaction
  ( @feed_id uniqueidentifier
  , @shard_id int

  , @time_hint datetime2(3) = null
  , @random_bytes binary(10) = null
  , @count bigint = 68719476736

  , @time datetime2(3) = null output
  , @ulid binary(16) = null output
  , @ulid_high binary(8) = null output
  , @ulid_low bigint = null output
  )
```

#### Input parameters:

* `@time_hint`: If not provided, `sysutcdatetime()` is used. This is the time
  component that should be embedded in the ULID. The reason it is a *hint* is
  that if a ULID with a *higher* time component has already been used
  for this `(feed_id, shard_id)` 

* `@count` (mainly debugging use): The number of ULIDs to allocate; the *next* invocation with the *same*
  time prefix will increment the randomly started counter by this many. The
  default is 2^36, which is 1 TB of ULIDs; this default should be fine for
  all uses.

* `@random_bytes` (mainly debugging use): Instead of randomly generating bytes, use these. This is
  mainly useful for writing tests.

#### Output parameters:

* `@time`: The time component of the ULID that was used in the end. 
  This will be equal to `@time_hint`, except if the previous write on
  the partition used a higher timestamp.

* `@ulid`: Get a single ULID. Provided for convenience;
   it is always equal to `@ulid_high + convert(binary(8), @ulid_low)`.

* `@ulid_high` and `@ulid_low`: Allows you to generate any number of sequential
  ULIDs; `@ulid_high + convert(binary(8), @ulid_low + @number)`.
  Here, `@number` should be less than `@count`.


#### Example

```sql
declare @ulid binary(16)
begin transaction
exec [changefeed].make_ulid_in_batch_transaction
    @feed_id = @feed_id    
  , @shard_id = @shard_id
  , @ulid = @ulid output;
-- use @ulid in subsequence INSERT in transaction
commit
```



### Writing events and the outbox table

To use mssql-changefeed, the main thing you have to ensure is that
writes happen to `[changefeed].[outbox:myservice.MyEvent]` when appropriate.
For a typical event sourcing table this would happen with every INSERT
to your table. The insert can happen using a SQL trigger or by changing your
backend code; the details of achieving this is left up to you.

An example insert is:
```sql
insert into [changefeed].[outbox:myservice.MyEvent]
    (shard_id, time_hint, AggregateID, Version)
values (0, '2023-05-31 12:00:00', 1000, 1);
```
This is for a table `myservice.MyEvent` with primary key `(AggregateID, Version)`.

#### Reference for [changefeed].[outbox:(tablename)]:

The outbox table has the following columns; in addition to the primary
keys of your table.

##### shard_id

Used to increase throughput; so that consumers can consume/process
several partitions at the same time, in line with the "partitioned log"
model. If you don't wish to have several partitions, simply always pass 0.
If you wish to have several partitions, you want to do something like
`aggregate_id % shard_count`; the sharding mechanism is left entirely to you.

##### time_hint

The time component that should be used in the generated ULIDs.
It is a *hint*, as it will not always be honored; making ULIDs monotonically
increase within each shard takes precedence. Pass the current time
for this.

##### order_sequence

This is optional; by the default the next value for `[sequence:(tablename)]`
will be used. If you care about event ordering, *and* want to
insert many entries into the outbox in the same SQL statement,
you should use the following pattern:

```sql
insert into [changefeed].[outbox:myservice.MyTable](shard_id, order_sequence, time_hint, AggregateID, Version)
select
    shard_id,
    next value for [changefeed].[sequence:myservice.MyTable] over (order by source.Sequence),
    ...
from SomeSourceTable as source;
```
In this case, it was important that events are ordered by `myservice.MyTable.Sequence`.


* `order_sequence` (optional) and `order_number`: Used to explicitly demand an ordering of
  events on the feed that will never be violated, even if `time_hint`
  is giving a different order. The `shard_key` is meant to identify
  your "entity"/"aggregate" and has a configurable type (defaults
  to `bigint`); specifically, every event with the same `shard_key`
  should be on the same shard. Then `order_number` (always `bigint`) specifies
  an ordering *within* each `shard_key`. If these concepts do not make
  sense in your application; for instance, simply always pass `0` for both
* values.
* *Primary keys*: The primary key column(s) of your table. These are simply
  copied over to the feed table, and returned in the result. Essentially
  these are the payload of the outbox/feed mechanism.


