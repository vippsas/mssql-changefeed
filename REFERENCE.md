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
