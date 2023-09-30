# Notes on how ULIDs are generated

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
simply always zero out the second-highest bit:
```sql
ulid_low = convert(bigint, substring(@random_bytes, 3, 8)) & 0xbfffffffffffffff
```
One can then add up to 2^62 to `ulid_low` before any chance of overflow,
in practice eliminating it.
**This reduces the random component from 80 to 79 random bits**, but we feel this is
worth the trade-off of simpler code.
