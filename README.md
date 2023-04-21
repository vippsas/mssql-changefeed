# mssql-changefeed v2: Changefeed SQL library for SQL Server

Note: Version 1 used a rather different approach. It is
still available on the [v1 branch](TODO).

### On ULIDs

In this library we have standardized on using ULIDs to identity rows
in the database.```

### Problem statement
The simple view is that polling for INSERTs in a table can be done
simply by querying for new data since last time. For instance,
if `MyTable` has an `EventID identity` column, you can do: 
```
select * from MyTable
where EventID > @LastEventIDFromPreviousPolling  
```

The problem is that this solution is not race-safe
on its own. The problem is illustrated in the following
figure.

![Illustration of multiple concurrent inserts into a SQL table](bottom-of-table.png)
In this image, the solid black rows have been committed and are visible
to readers. The grayed out rows (4, 6, and 8) are in *pending* transactions,
not yet committed, and are only visible to the transactions writing them.

If a reader now reads the table, it will see the 7th row as the last
one and note down `ULID = 0x...B5A` as its cursor position in the table,
*without* having read the 4th and 6th row -- thus never consuming this data.

### How the problem is solved

It is not very difficult to use database transactions to *serialize* the
access to a particular `(feed_id, shard)` pair. This approach works rather
well if everything you do is in a single stored procedure in the SQL server.

The problem is if you use client-managed database transactions, and are
disconnected at the wrong moment.

The mssql-changefeed library provides the ability to serialize writes
to a given shard in a *safe* manner, no matter if you use client-managed
transactions or not. [There is separate document
for the technical details](EXPERTS-GUIDE.md).

### Installation

Copy the file(s) in the `migrations` directory and run them in the database.
The standard is to create tables and stored procedures in a `changefeed`
schema; alter the migration file as needed to serve your needs.

If you use stored procedures or otherwise make sure your SQL transactions
are done with a single network round-trip to the SQL server, this is all
you need. If you want to do client-side transactions, use of these tools
is not perfectly safe unless you also create those transactions
using the libraries. We have support for:

* Go (see `go/` sub-directory)
* Goal for later: .NET

### Usage

Concepts:

* `feed_id` is a UUID that you generate once manually and put into your
  source code. This identifies a particular change feed; the SQL table
  or set of tables that you insert into in a single, conceptual "change"
  or "event".
* `shard` is an integer, and represents a sub-division of each `feed_id`
  to support higher throughput. You need to benchmark performance
  in your case, but as a very rough guide, think 100 changes per secon
  per shard. An example of using shard would be to set it to `customer_id % 32`
  to make 32 shards and shard along the `customer_id` axis.
  If you want to manage sharding in some other way, it is of course
  possible to hard-code `shard` to be 0, and instead pass different
  UUIDs for inserts into the same database tables. Only the combination
  of `(feed_id, shard_id)` ever matters to mssql-changefeed.

What we then want to achieve is

1) Generate an ULID for every row you write to your table(s) 
2) Ensure that the ULID are monotonically increasing for any observer,
   within a shard. This means serializing the writes within each
   shard.

The locking mechanisms to achieve this are a bit complex, and this complexity
is abstracted away behind a special "changefeed ULID transaction".
For client-managed transactions, it is important that you use the client
libraries for Go or C#.

### Go

Instead of `pool.BeginTx`, as usual, instead use `changefeed.BeginTransaction`.
This will return another object with a similar protocol to `sql.Tx`.

Once the transaction has been started, the ULID tools described below are
available. It is also possible to call `tx.NewULIDs()` or `tx.NewSingleULID()`
to generate ULIDs.

### C#

TODO

### SQL

If the **entire transaction is done in a single batch / stored procedure**,
you can use the following methods. (They can also be used if you don't care about
30-60 seconds downtime for your service).

One starts the transaction by always calling these together:
```sql
begin transaction
exec changefeed.begin_ulid_transaction
    @feed_id = '80270756-deeb-11ed-91f2-4f03a365d3f8',
    @shard_id = 0;
```

Within the transaction, the ULID tools described below are available to
generate ULIDs. Finally, make sure the following pair is executed
closely together:
```sql
exec changefeed.commit_ulid_transaction
-- very important that NOTHING ELSE happens in-between the previous line
-- and the next line
commit
```

### ULID tools

Once the changefeed ULID transaction has been set up, ULIDs can be generated
in two ways:

**Single ULID**: Call the `changefeed.single_ulid` stored procedure:
```sql
declare @ulid binary(10);
exec changefeed.new_ulid @ulid = @ulid output;
insert into customer_data.ShoeSizeChanged(CustomerID, ..., Shard, ULID) values (..., @Shard, @ULID);
```

**Multiple ULIDs**: To generate many (potentially millions) of ULIDs in a single
database transaction, use the following incantation:

```sql
insert into customer_data.ShoeSizeChanged(CustomerID, ..., Shard, ULID)
select
    input.CustomerID,
    ...
    @Shard,
    changefeed.ulid_sequence(next value for changefeed.sequence over (order by form_input.Time))
from form_submissions.Input as form_input
where
    form_input.CustomerID % 32 = @Shard;
```
This example also points out that you have to write to a *single*
target shard within the database transaction. On the other hand,
if you are doing this kind of batch processing, then the throughput
is very high, and the chances of more than one target shard being
needed is much less.

Note that the output of `changefeed.sequence` has *no* meaning by itself.
It is also used concurrently by other parallel threads.
It is a bit of a hack to be able to provide a neat interface where it
reasonably easy to always do the correct thing. Ideally we would
like `changefeed.ulid() over (order by form_input.Time)`, but SQL
does not let us build that.

**Note:** If the use of a sequence is a problem, we could introduce an alternative
interface `changefeed.ulid_row_number(row_number() over (order by ...))`.
The disadvantage of such an interface is that one would need to manually
track and signal the number of ULIDs generated.

## Still curious?

Head over to the [experts guide](EXPERTS-GUIDE.md).
