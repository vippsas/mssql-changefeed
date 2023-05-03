# mssql-changefeeed: Implementation description

The implementation of mssql-changefeed is a bit like coming up with a bunch
of tricks (or solving a bunch of independent issues), then putting
it all together. The tricks will be described one by one below, but are
reasonably independent of each other.

## ULIDs

### Internal representation for ULID

Generating a single isolated ULID is simple enough. But, when supporting ULID
for SQL, we should also:
* Ensure that ULIDs allocated in different transactions after one another
  follow each other (see section "Monotonicity" in the ULID spec)
* Do things in such a way that *millions* of ULIDs can be generated
  in a single database transaction.
* We consider a database transaction to happen at a single instant.
  So, if you generate millions of ULIDs in a transaction, they will
  all have the same timestamp in the ULID.

To help doing the arithmetic we internally treat ULID as two components
(using the common naming situation in this situation when discussing
binary layouts):
```
ulid_high binary(8)
ulid_low bigint
```
The full ULID is simply
```
ulid_high + convert(binary(8), ulid_low)
```
as the `convert` function will correctly convert to big-endian
and negative numbers having the highest bit.

The first 6 bytes of `ulid_low` is the 48-bit timestamp, again
this is almost built into SQL:
```sql
convert(binary(6), datediff_big(millisecond, '1970-01-01 00:00:00', @time))
```
The last 2 bytes of `ulid_high`, as well as `ulid_low`, is initially
generated easily with `crypt_gen_random(10)` which returns 10 random bytes.

The main point of passing around `ulid_low` as `bigint` is to do easy
arithmetic. Since the starting point is random, if you are extremely
unlucky and draw something close to `0xffff...fffff`, there could be
an integer overflow as we increment the counter. This will cause an
error. In order to simply not have to think about this, we
simply always zero out the second-highest bit (bit 62):
```sql
ulid_low = convert(bigint, substring(@random_bytes, 3, 8)) & 0xbfffffffffffffff
```
One can then add up to 2^62 to `ulid_low` before any chance of overflow,
in practice eliminating it.
**This reduces the random component from 80 to 79 random bits**, but we feel this is
worth the trade-off of simpler code.

### ULID allocation within timestamp 

To generate ULIDs within timestamp/transaction we need a way of
generating a sequential order of the ULIDs wanted.
After some benchmarks and experimentation the best trick seem to be
a slight abuse of SQL sequences. The goal is to be able to support
the user interface
```sql
insert <...>
select
    <...>,
    changefeed.ulid_sequence(next value for changefeed.sequence over (order by <...>)).
```
This lets us:
a) Use a convenient window function / `order by` clause
b) Allows multiple SQL statements in the transaction without worrying about 
   overlapping ULIDs.

However we can't really create a sequence that is only in scope of the current
transaction. So, to achieve this, we create a single sequence:

```sql
create sequence changefeed.sequence as bigint start with -9223372036854775808...
```

Then we *sample* this sequence at the start of the transaction, and
use the difference between the current value and the value at the start
of the transaction as the increment counter, and again at the end to estimate
a count. This value might skip 1 or 1000
or a million nubmers now and then because of concurrent use by other processes,
which does not really matter much:

It does not really matter if that estimate is off by a factor of a thousand
or so because of concurrent processes using the same counter.
Consider: If 2^36 ULIDs are generated, then even if the *only* thing the
transaction does is write the ULIDs without any data to follow them, that
would generate a need for 1 TB of transaction log space, the current Hyperscale
maximum. This still leaves 2^26 = 67 million transactions, per millisecond,
before overflowing.

**Note for future maintainers:** The sequence seems to be safest user interface.
However, if the sequence is ever seen as a bottleneck (even with the optimizations
mentioned in the reference manual), it's easy enough to add other methods
of ULID generation such as 
`changefeed.ulid_row_number(@offset + row_number() over (order by ...)`.
One should then probably not try to count the
number of ULIDs in the transaction (for safety and convenience),
but simply assume that e.g. 2^36 ULIDs are generated
and add that number to `ulid_low` in the commit.

### Use of session_context

At least from Go, it is not possible to execute code such as
```sql
changefeed.commit_ulid_transaction @ulid_high = @ulid_high, ...
commit
```
The problem is that

a) if any parameters are passed, a temporary stored procedure is created to marshal the parameters, and
b) it is forbidden to have a `commit` in a stored procedure (without a balancing `begin transaction`)

A workaround for this is to pass the arguments needed using `session_context`
instead. Once having started down this road anyway, it was a short way to simply
use this as the primary mechanism for all state to also simplify
the use of `changefeed.ulid()`.

The performance hit from this is at least not big (although it may be measurable).


## The locking scheme to avoid long stalls

This is the *main* hurdle to pass for the library: Ensure that it is impossible
(or at least very unlikely)
to get into a state where a client-managed transaction that is abandoned (e.g.
power-off of client) can stop the entire shard for 30-60 seconds (the SQL Server
network connection timeout).

This should never happen in a server-managed transaction (transaction within single
batch), so below the scenario is always a client-managed transaction. All the
extra precautions are not needed for a server-managed transactions they
don't really hurt either.

### The basic, vulnerable version

We keep a state table `changefeed.shard_ulid` with primary
key `(feed_id, shard_id)` and data `(time, ulid_prefix, ulid_suffix)`.
Then:
```sql
begin transaction
update changefeed.shard_ulid ... -- <compute, read and write values>
--- <return control to backend>
commit
```

This has the effect of:
a) Blocking all concurrent sessions on the `update`.
b) 








To keep the main section at the bottom readable, some components
we will use are described first, and then it comes together in the
later sections.

Below the general scenario is that a *service* is running
with client-managed transactions on a number of *pods*; and
we assume that a pod.

In principle (for the purposes of reasoning and proofs),
all of the pods has a high number of connections trying at exactly
the same time to write something to the same shard. We want
to ensure that the algorithm doesn't slow down writing beyond
the fundamental throughput limitations that are inherent in
serializing writes to a single shard. (Since the writes will
hit the same B-tree pages in the end, they are usually
serialized *at some point* anyway, the only way to avoid that
is to not have a randomly distribute key instead, although that
comes with other and often higher performance disadvantages).

### Note about memory-optimized tables

Some of the tricks below can be done in different ways, probably
much simpler, with the availability of the lock-free algorithms
that are used for In-memory OLTP tables. However, these are not available
on Azure SQL Hyperscale. If they are made available, the library can be
simplified.

### Note about snapshot transactions

Part of the challenge is to make it possible to run snapshot
transactions; where the state of the database is set to when
the transaction starts.

The snapshot transaction doesn't *really* start when
`begin transaction` is issued; it starts later when the first
actual read that depends on snapshot is issued. For instance,
a read with the option `with (readuncommitted)` did this.

At first we tried exploiting this, so that one could get
the application lock inside the transaction, owned by the
transaction, and not worry about releasing it. However, this
turned out to be fragile and in some cases (but not others)
simply executing a stored procedure that does nothing would start
the snapshot transaction, for instance.

Even if we got this working, these cases made it clear that we
would be depending on implementation specific behaviour. So:
The final solution the locks are owned by `Session` and we have
to take care to release them in all situations.



### Detecting that a power-off has happened 

We will detect when poweroffs happen, and write those to a table
`changefeed.incident(feed_id, shard_id, count)`. To do this detection
we require a user-input timeout which should be much longer than the transaction
should reasonably use to commit (for instance, 1 second). Then we proceed
as follows:
```sql
-- get a *short*, 20 millisecond lock first
declare @result int
exec @result = sp_getapplock ..., @LockTimeout = 20
if @result = 1
begin
    -- Got lock after waiting a short while. This *may*
    -- be the start of a power-off. However, we really don't
    -- want to assume a power-off if we are simply in a high-traffic
    -- scenario with a long queue.
    declare @sample_ulid_low = (select ulid_low from changefeed.shard_ulid with (readuncommitted) where ....)
    -- wait the remaining time
    exec @result = sp_getapplock ..., @LockTimeout = @timeout
    if @result = 1 and @sample_ulid_low = (select ulid_low from changefeed.shard_ulid with (readuncommitted) where ....)
    begin
      -- Reached @timeout, *and* there was no activity in the state
      -- table in the meantime. Poweroff detected!
        <...>
    end
end
<...>
```

So, the trick here is to first have a short lock duration (because
we don't want to bother with the sampling if we are not waiting in a long
line), but if we have to wait for a longer time we also check if there
was activity.

The `with (readuncommitted)` is really important because it allows
"escaping" the snapshot transaction we are in. Since we are only trying
to detect activity, detecting uncommitted activity is just as good.


### The algorithm

**Component 1:** Ensure that Exclusive lock is taken in same network roundtrip as commit.
The **first** component of this is the idea to make sure that
```sql
update [changefeed].shard_ulid
    set ...
    where feed_id = @feed_id and shard_id = @shard_id; 

-- We now have an infinite-duration Exclusive lock on a row of shard_ulid.
-- Important to not return control to client code at this point.
commit
```
always execute in the same network round-trip. This should ensure the lock
is never *stuck* for 30-60 seconds before the network connection times out.

**Component 2:** Usually, one would put the `update` statement above at the
*beginning* of the transaction. Then we would both get the starting point
for the ULIDs (in case we are in the same millisecond as the previous transaction),
and, even more important, serialize transactions on each shard so that
they run one after the other.

With the exclusive lock on the state table at the end of the
transaction, we can explicitly make a serialization point by
getting an exclusive `sp_getapplock`.

Why do we gain something by first delaying the locking of
`[changefeed].shard_ulid` until the end, but then adding a new
lock manually at the start using `sp_getapplock`?
In the event on an incident, the `sp_getapplock` will *also*
be held for 30-60 seconds. But, this lock can be "burned",
if all other sessions simply choose another lock name,
they can continue.

To facilitate this we track the incidents and include the
incident count in the lock name:

```sql
declare @incident_count int = (select isnull(incident_count, 0) from [changefeed].incident_count with (readuncommitted) where feed_id = @feed_id and shard_id = @shard_id)
declare @lock_name nvarchar(256) = concat('changefeed/', @feed_id, '/', @shard_id, '/', @incident_count);
exec @result = sp_getapplock @LockName = @lock_name, ...
```
The `with (readuncommitted)` is **really** important here:
If the transaction isolation level is snapshot, then each
transaction will have a "real" snapshot start. We want to
make sure that we don't take this snapshot too early, which would
leave us at a state where we a) get the sp_getapplock, but
b) work with stale shard state data. (The transaction would
anyway be aborted at the end, but that is a waste).

If the lock times out -- and we detect an incident, as described
above -- then the `incident_count` should be incremented.
To do this we return an error code and have the client do
an `update` statement on its own, without being in an explicit
transaction, to avoid any issues with stalls on *that* operation.


on a key including `(feed_id, shard_id)`,
*with a timeout*. The timeout should be set sufficiently high
that all transactions finish well within the timeout.

**Happy day scenario**: The `sp_getapplock` his causes all transactions to line up nicely
and execute one by one (for each shard independently)
After getting the lock, we do a `select ... from [changefeed].shard_ulid`,
without locking, after getting the `sp_getapplock`. To guarantee **correctness**
entirely independent of the `sp_getapplock`, we verify the values we get
there when performing the final `update` statement, and roll back the transaction
and throw an error if there is a mismatch.

**Unhappy day scenario**: If there is a timeout in getting the `sp_getapplock`,
it *may* be the case there was a power-off incident. The powered-off session
has then gone through the "preamble"
```sql
begin transaction
exec sp_getapplock ...
select ... from changefeed.shard_ulid
```
and then stops somewhere before getting to the `update` and `commit`.
In this case, the applock is going to be held for a long time.


The first thing to do in this
case is to verify that the current connection is still alive; after all there
is a rather high likelihood that the connection was initiated from the same pod
and is *also* bust at this moment. So, at this point we roll back the transaction
and return an error.

However, when the client retries, 







### Notes

Options include:

* A: ULID generated in transaction
* B: ULID generated post-transaction

axis 2:

* 1: ULID put in same table
* 2: ULID put in another table



A1: ongoing approach
A2: 



2 in general:

- change_id to link?
- 















