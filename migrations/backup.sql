
go

-- See REFERENCE.md for documentation.
create procedure [changefeed].new_ulid
  ( @object_id int = null
  , @object_name nvarchar(max) = null
  , @shard_id int

  , @time_hint datetime2(3) = null
  , @random_bytes binary(10) = null
  , @count bigint = 68719476736  -- 1 TB of ULIDs...

  , @time datetime2(3) = null output
  , @ulid binary(16) = null output
  , @ulid_high binary(8) = null output
  , @ulid_low bigint = null output
  )
as begin
    set nocount, xact_abort on;

    declare @msg nvarchar(max);

    if @@trancount = 0 throw 77000, 'Please call new_ulid in a database transaction, so that you avoid race conditions with consumers. See README.md.', 0;

    if @object_id is null
begin
        set @object_id = object_id(@object_name, 'table');
        if @object_id is null
begin
            set @msg = concat('Table not found: ', quotename(@object_name))
            throw 77001, @msg, 1;
end
end
    if @time_hint is null set @time_hint = sysutcdatetime();
    if @random_bytes is null set @random_bytes = crypt_gen_random(10);

    -- T-SQL primer, to be able to read the below code:
    -- 1) We are going to compute some expressions that depend on each other. To do this
    --    we use the `cross apply` clause. This construct similarly to a `let` clause in
    --    an imperative traditional programming language. Unfortunately we can't depend
    --    on expressions, so we need 3 layers of these for syntactic reasons.
    -- 2) The set clause has some little known features:
    --    @variable = expression, -- lift out something we computed; i.e., like a `select`...
    --    @variable = target_column = expression,  -- put something we computed *both* in variable *and* in table row
    --
    -- Why cram all of this into a single update statement? Because it allows us to look up the row,
    -- do the computation needed, and do the change, in a single B-tree lookup. Doing a `select` first
    -- then `update` later would need 2 lookups.
update shard_state
set @time = time = new_time
  , @ulid_high = ulid_high = new_ulid_high
  , @ulid_low = ulid_low_range_start
  , @ulid = new_ulid_high + convert(binary(8), ulid_low_range_start)
  , ulid_low = ulid_low_range_end
from [changefeed].shard_state shard_state with (serializable, rowlock)
    cross apply (select
    new_time = iif(@time_hint > shard_state.time, @time_hint, shard_state.time)
    ) let1
    cross apply (select
    new_ulid_high = (case
    when @time_hint > shard_state.time then
                                              -- New timestamp, so generate new high. The high is simply the timestamp + 2 random bytes.
    convert(binary(6), datediff_big(millisecond, '1970-01-01 00:00:00', new_time)) + substring(@random_bytes, 1, 2)
    else
    shard_state.ulid_high
    end)
        , ulid_low_range_start = (case
    when @time_hint > shard_state.time then
                                              -- Start low in new place.
                                              -- The mask 0xbfffffffffffffff will zero out bit 63, ensuring that overflows will not happen
                                              -- as the caller adds numbers to this
    convert(bigint, substring(@random_bytes, 3, 8)) & 0xbfffffffffffffff
    else
                                              -- We store the *next* ULID to use in shard_state, so it is the start of the range
                                              -- we allocate now.
    shard_state.ulid_low
    end)
    ) let2
    cross apply (select
                                              -- The result of this call allocates ULIDs in the range [ulid_low_range_start, ..., ulid_low_range_end),
                                              -- i.e. endpoint is exclusive
    ulid_low_range_end = ulid_low_range_start + @count
    ) let3
where
    object_id = @object_id and shard_id = @shard_id;

if @@rowcount = 0
begin
        -- Note: In the event that @@rowcount = 0, then no @variables will have been changed by
        -- the above `update` statement. So we start from scratch.
exec [changefeed].insert_shard @object_id = @object_id, @shard_id = @shard_id;
        -- Try again
exec [changefeed].new_ulid
            @object_id = @object_id
          , @shard_id = @shard_id
          , @count = @count
          , @time_hint = @time_hint
          , @random_bytes = @random_bytes
          , @time = @time output
          , @ulid = @ulid output
          , @ulid_high = @ulid_high output
          , @ulid_low = @ulid_low output
end
end;

go


create procedure [changefeed].begin_driver_transaction
  ( @feed_id uniqueidentifier
  , @shard_id int
  , @timeout int = 1000
  , @time_hint datetime2(3) = null
  )
as begin
    set nocount, xact_abort on;
begin try
if @@trancount > 0 throw 55100, 'Please read the documentation; begin_driver_transaction should be called *before* starting the transaction', 0;

        if @time_hint is null set @time_hint = sysutcdatetime();

        declare @ulid_high binary(8)
        declare @ulid_low bigint
        declare @time datetime2(3)

        exec [changefeed].acquire_lock_and_detect_incidents
             @feed_id = @feed_id
           , @shard_id = @shard_id
           , @timeout = @timeout;
        -- at this point we have lock; because otherwise an error is thrown

        declare @previous_time datetime2(3);
        declare @random_bytes binary(10);

select
        @ulid_high = ulid_high
     , @ulid_low = ulid_low
     , @previous_time = time
from [changefeed].shard_ulid
where
    feed_id = @feed_id and shard_id = @shard_id;

if @@rowcount = 0
begin
            -- At this point we just block the transaction -- creation of a new shard is a corner
            -- case we don't bother with protecting or optimizing
exec [changefeed].insert_shard @feed_id = @feed_id, @shard_id = @shard_id;
end

        if @previous_time is null or (@time_hint > @previous_time)
begin
            set @time = @time_hint;
            -- new timestamp; re-seed entropy
            set @random_bytes = crypt_gen_random(10);
            set @ulid_high = convert(binary(6), datediff_big(millisecond, '1970-01-01 00:00:00', @time_hint)) + substring(@random_bytes, 1, 2);
            -- The mask 0xbfffffffffffffff will zero out bit 63, ensuring that overflows will not happen
            set @ulid_low = convert(bigint, substring(@random_bytes, 3, 8)) & 0xbfffffffffffffff;
end
else
begin
            set @time = @previous_time
end

        -- If there are several backend<->SQL transactions going on, it's hard to
        -- channel parameters to commit_transaction.
        -- See comment in transaction.go. So, while we are at it, just always use
        -- this method so that parameters don't have to be passed again
        -- to commit_transaction.
        declare @sequence_start bigint = next value for [changefeed].sequence;
exec sp_set_session_context N'changefeed.feed_id', @feed_id;
exec sp_set_session_context N'changefeed.shard_id', @shard_id;
exec sp_set_session_context N'changefeed.ulid_high', @ulid_high;
exec sp_set_session_context N'changefeed.ulid_low', @ulid_low;
exec sp_set_session_context N'changefeed.time', @time;
exec sp_set_session_context N'changefeed.sequence_start', @sequence_start;

end try
begin catch
if @@trancount > 0 rollback;
        ;throw;
end catch
end

go

create procedure [changefeed].begin_batch_transaction
( @feed_id uniqueidentifier
, @shard_id int
, @timeout int = -1
, @time_hint datetime2(3) = null
)
as begin
    set nocount, xact_abort on;
begin try
        declare @lock_result int
        declare @incident_detected bit;

exec [changefeed].begin_driver_transaction
            @feed_id = @feed_id
          , @shard_id = @shard_id
          , @timeout = @timeout
          , @time_hint = @time_hint
          , @incident_detected = @incident_detected output
          , @lock_result = @lock_result output;

        if @lock_result = -1 and @incident_detected = 1
            throw 77010, 'timeout waiting for lock on (feed_id, shard_id); may be orphan session holding lock', 0
        else if @lock_result = -1
            throw 77011, 'timeout waiting for lock on (feed_id, shard_id); progress is being made, so just long queue', 0
        else if @lock_result < 0
            throw 77011, 'sp_getapplock error', @lock_result;
        -- Else, OK result and return
end try
begin catch
if @@trancount > 0 rollback;
        ;throw;
end catch
end;

go

create function [changefeed].ulid(@sequence_sample bigint) returns binary(16)
as begin
    return convert(binary(8), session_context(N'changefeed.ulid_high'))
         + convert(binary(8),
             convert(bigint, session_context(N'changefeed.ulid_low'))
             + (@sequence_sample - convert(bigint, session_context(N'changefeed.sequence_start')))
             );
end

go

create function [changefeed].time() returns datetime2(3)
as begin
    return convert(datetime2(3), session_context(N'changefeed.time'))
end

go

create procedure [changefeed].commit_transaction
as begin
    set nocount, xact_abort on
begin try
        declare @feed_id uniqueidentifier = convert(uniqueidentifier, session_context(N'changefeed.feed_id'));
        declare @shard_id int = convert(int, session_context(N'changefeed.shard_id'));
        declare @ulid_high binary(8) = convert(binary(8), session_context(N'changefeed.ulid_high'));
        declare @ulid_low bigint = convert(bigint, session_context(N'changefeed.ulid_low'));
        declare @time datetime2(3) = convert(datetime2(3), session_context(N'changefeed.time'));
        declare @sequence_start bigint = convert(bigint, session_context(N'changefeed.sequence_start'));
        declare @count bigint = (next value for [changefeed].sequence) - @sequence_start;

        if @@trancount = 0 throw 77001, 'Please read the documentation and call changefeed.commit_transaction in a *special kind of* database transaction (@trancount = 0)', 0;
        if @feed_id is null throw 77002, 'Please read the documentation and call changefeed.commit_transaction in a *special kind of* database transaction (no feed_id in session context).', 0;

update [changefeed].shard_ulid
set
    ulid_high = @ulid_high
    -- for good measure, just add 1; so that @ulid_low can be passed inclusive or exclusive, doesn't matter..
        , ulid_low = @ulid_low + @count
        , time = @time
where
    feed_id = @feed_id and shard_id = @shard_id;

exec sp_set_session_context N'changefeed.feed_id', null;
exec sp_set_session_context N'changefeed.shard_id', null;
exec sp_set_session_context N'changefeed.ulid_high', null;
exec sp_set_session_context N'changefeed.ulid_low', null;
exec sp_set_session_context N'changefeed.time', null;
exec sp_set_session_context N'changefeed.sequence_start', null;
end try
begin catch
if @@trancount > 0 rollback;
        ;throw;
end catch
end


