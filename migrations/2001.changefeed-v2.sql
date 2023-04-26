-- mssql-changefeed migration 2001
--
-- THE ORIGINAL VERSION OF THIS FILE LIVES IN github.com/vippsas/mssql-changefeed
--
-- IF YOU EDIT THIS COPY IN YOUR LOCAL REPOSITORY, PLEASE RECORD THE CHANGES HERE, TO HELP
-- YOURSELF WITH FUTURE UPGRADES:
--
-- END CHANGES
--

-- This migration is the first migration for version 2 of mssql-changefeed. It choses names
-- that does not overlap with changefeed v1, so that they can live in the same schema,
-- but the two versions are entirely independent and can be used without each other.

-- Note: We put the schema name in [brackets] everywhere, so that it is easier for you
-- to search & replace it with something else if needed.
create schema [changefeed];
go

create table [changefeed].shard_ulid
  ( feed_id uniqueidentifier not null
  , shard_id int not null
  , time datetime2(3) not null
        constraint def_shard_ulid_time default '1970-01-01'

  -- See EXPERTS-GUIDE.md for description of ulid_prefix
  -- and ulid_low.
  , ulid_high binary(8) not null
        constraint def_shard_ulid_ulid_prefix default 0x0
  , ulid_low bigint not null
        constraint def_shard_ulid_ulid_low default 0
  -- For convenience, the ulid is displayable directly. Also serves as documentation:
  , ulid as ulid_high + convert(binary(8), ulid_low)
  , constraint pk_shard_ulid primary key (feed_id, shard_id)
  );

-- can't imagine locks escalating on this table, but would sure be in
-- trouble if they did, so just for good measure:
alter table [changefeed].shard_ulid set (lock_escalation = disable);

go

create table [changefeed].incident_count
  ( feed_id uniqueidentifier not null
  , shard_id int not null
  , incident_count int not null
  );

go

create sequence changefeed.sequence as bigint
    start with -9223372036854775808
    increment by 1
    cache 100000
    no maxvalue;

go

-- insert_shard will ensure that a shard exists, without causing an error if it does
create procedure [changefeed].insert_shard
  ( @feed_id uniqueidentifier
  , @shard_id int
  , @incident_count int = 0
  )
as begin
    insert into [changefeed].shard_ulid (feed_id, shard_id)
    select
        @feed_id
      , @shard_id
    where
        not exists (
            select 1 from [changefeed].shard_ulid with (updlock, serializable, rowlock)
            where feed_id = @feed_id and shard_id = @shard_id);
    if @@rowcount = 1
    begin
        -- always inserted in transaction with shard_ulid, so don't need another if-exists check
        insert into [changefeed].incident_count (feed_id, shard_id, incident_count)
        values (@feed_id, @shard_id, @incident_count);
    end
end;

go

-- See REFERENCE.md for documentation.
create procedure [changefeed].make_ulid_in_batch_transaction
  ( @feed_id uniqueidentifier
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
    if @@trancount = 0 throw 77000, 'Please call generate_ulid_blocking in a database transaction, so that you avoid race conditions with consumers. See README.md.', 0;

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
    from [changefeed].shard_ulid shard_state with (serializable, rowlock)
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
        feed_id = @feed_id and shard_id = @shard_id;

    if @@rowcount = 0
    begin
        -- Note: In the event that @@rowcount = 0, then no @variables will have been changed by
        -- the above `update` statement. So we start from scratch.
        exec [changefeed].insert_shard @feed_id = @feed_id, @shard_id = @shard_id;
        -- Try again
        exec [changefeed].make_ulid_in_batch_transaction
            @feed_id = @feed_id
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

create procedure [changefeed].release_lock
as begin
    set xact_abort, nocount on
    begin try
        declare @lock_name nvarchar(255) = convert(nvarchar(255), session_context(N'changefeed.lock_name'))
        if @lock_name is null throw 77103, 'release_lock called in a session where acquire_lock_and_detect_incidents has not been called', 0;

        exec sp_releaseapplock @Resource = @lock_name, @LockOwner = 'Session';

        exec sp_set_session_context N'changefeed.lock_name', null;
    end try
    begin catch
        if @@trancount > 0 rollback;
        ;throw;
    end catch
end

go

create procedure [changefeed].acquire_lock_and_detect_incidents
  ( @feed_id uniqueidentifier
  , @shard_id int
  , @timeout int = 1000
  , @max_attempts int = 3
  )
as begin
    set xact_abort, nocount on
    begin try
        if @@trancount > 0 throw 77101, 'please call [changefeed].acquire_lock_and_detect_incidents before starting transaction', 1;
        if @timeout < 100 throw 77102, '@timeout >= 100 required', 0;  -- todo: consider @timeout = -1 all the way through...
        if session_context(N'changefeed.lock_name') is not null throw 77103, 'acquire_lock_and_detect_incidents called, but session has already taken a lock', 0;

        declare @msg nvarchar(max);
        declare @timeout_1st int;
        declare @timeout_2nd int;
        declare @incident_detected bit;
        declare @incident_count int;
        declare @lock_result int;

        set @timeout_1st = 99;
        set @timeout_2nd = @timeout - 99;


        -- Get number of incidents => lock generation we are currently on.
        set @incident_count = (select incident_count
            from [changefeed].incident_count
            where
                feed_id = @feed_id and shard_id = @shard_id);

        if @incident_count is null
        begin
            set @incident_count = 0;
            -- no shard state, set it up
            set transaction isolation level read committed
            begin transaction
                exec [changefeed].insert_shard
                    @feed_id = @feed_id
                  , @shard_id = @shard_id
                  , @incident_count = 0;
            commit
        end

        set @incident_detected = 0;

        declare @lock_name nvarchar(255) = concat('changefeed/', @incident_count, '/', @feed_id, '/', @shard_id);

        -- First do a short, initial lock to figure out if we will actually be waiting
        -- for a while; happy-day optimization. Hardcoded to 100ms.
        exec @lock_result = sp_getapplock
              @Resource = @lock_name
            , @LockMode = 'Exclusive'
            , @LockOwner = 'Session'
            , @LockTimeout = 99;

        if @lock_result = -1
        begin
            -- Lock timed out. We want to detect if we have a long backlog, or if we are waiting
            -- for a *single* thread to finish. To do this we sample the shard state before and after.
            declare @sampled_ulid_high binary(8)
            declare @sampled_ulid_low bigint
            select
                @sampled_ulid_high = ulid_high
              , @sampled_ulid_low = ulid_low
            from [changefeed].shard_ulid
            where
                feed_id = @feed_id and shard_id = @shard_id;

            if @sampled_ulid_high is null
                throw 77200, 'assertion failed, should not be possible as we insert the shard further up', 0;

            exec @lock_result = sp_getapplock
                  @Resource = @lock_name
                , @LockMode = 'Exclusive'
                , @LockOwner = 'Session'
                , @LockTimeout = @timeout_2nd;

            if @lock_result = -1
            begin
                -- Timed out *again*. Do we have an incident, or a long queue of waiters?
                -- If ulid state has not changed in the time, judge it to be an incident.
                select
                    @incident_detected = iif(
                        @sampled_ulid_high = ulid_high and @sampled_ulid_low = ulid_low
                        , 1, 0)
                from [changefeed].shard_ulid with (readuncommitted)
                where
                    feed_id = @feed_id and shard_id = @shard_id;

                if @incident_detected = 1
                begin
                    set @incident_count = @incident_count + 1;

                    -- Many threads will do this update at once; but since they will only
                    -- update if the current value is lower, only one of them will actually do the write.
                    -- Rest will get @@rowcount = 0.
                    update [changefeed].incident_count
                    set
                        incident_count = @incident_count
                    where
                      feed_id = @feed_id
                      and shard_id = @shard_id
                      and incident_count < @incident_count;

                    if @max_attempts <= 0
                    begin
                        throw 77103, '[changefeed].acquire_lock_and_detect_incidents: timeout, and tried all attempts', 0;
                    end
                    else
                    begin
                        -- Recurse for next attempt
                        declare @max_attempts_minus_one int = @max_attempts - 1;  -- gotta love T-SQL...
                        exec [changefeed].acquire_lock_and_detect_incidents
                             @feed_id = @feed_id
                           , @shard_id = @shard_id
                           , @timeout = @timeout
                           , @max_attempts = @max_attempts_minus_one;

                        -- At this point, either we have the lock (successful execution),
                        -- and changefeed.lock_name has been set on the session; or, an
                        -- error has been thrown.
                        return
                    end
                end
            end
            -- fall through, @lock_result <> -1 is handled the same for both levels of if-tests
        end

        -- At this point, @lock_result <> -1, and can come from either attempt at getting the lock
        if @lock_result >= 0
        begin
            exec sp_set_session_context N'changefeed.lock_name', @lock_name;
            return
        end
        else if @lock_result = -1
        begin
            ;throw 77103, '[changefeed].acquire_lock_and_detect_incidents: timeout', 1;
        end
        else
        begin
            set @msg = concat('[changefeed].acquire_lock_and_detect_incidents: error trying to get lock: ', @lock_result);
            ;throw 77104, @msg, 1
        end
    end try
    begin catch
        if @@trancount > 0 rollback;
        ;throw;
    end catch
end

go

create procedure [changefeed].set_incident_count
  ( @feed_id uniqueidentifier
  , @shard_id int
  , @incident_count int
  )
as begin
    set nocount, xact_abort  on;

    if @@trancount > 0
    begin
        rollback;
        throw 70101, 'Do NOT call register_incident in transaction', 1;
    end

    begin try
        update [changefeed].incident_count
        set
            incident_count = @incident_count
        where
            feed_id = @feed_id
            and shard_id = @shard_id
            and incident_count < @incident_count;

    end try
    begin catch
        if @@trancount > 0 rollback;
        ;throw;
    end catch
end
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


