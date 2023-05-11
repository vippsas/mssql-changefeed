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

create table [changefeed].shard_state_ulid
  ( object_id int not null
  , shard_id int not null
  , time datetime2(3) not null
        constraint def_shard_ulid_time default '1970-01-01'

  -- See EXPERTS-GUIDE.md for description of ulid_prefix
  -- and ulid_low.
  , ulid_high binary(8) not null
        constraint def_shard_state_ulid_ulid_prefix default 0x0
  , ulid_low bigint not null
        constraint def_shard_state_ulid_ulid_low default 0
  -- For convenience, the ulid is displayable directly. Also serves as documentation:
  , ulid as ulid_high + convert(binary(8), ulid_low)
  , constraint pk_shard_ulid primary key (object_id, shard_id)
  );

-- can't imagine locks escalating on this table, but would sure be in
-- trouble if they did, so just for good measure:
alter table [changefeed].shard_state_ulid set (lock_escalation = disable);

go


-- insert_shard will ensure that a shard exists, without causing an error if it does
create procedure [changefeed].insert_shard
  ( @object_id int
  , @shard_id int
  )
as begin
    insert into [changefeed].shard_state_ulid (object_id, shard_id)
    select
        @object_id
      , @shard_id
    where
        not exists (
            select 1 from [changefeed].shard_state_ulid with (updlock, serializable, rowlock)
            where object_id = @object_id and shard_id = @shard_id);
end;


go

-- Note: Always returns successfully; the caller HAS to check the @result output variable, which
-- has the same values as the return from sp_getapplock.
create procedure [changefeed].lock_shard
  ( @object_id int
  , @shard_id int
  , @timeout int
  , @result int = null output  -- raw sp_getapplock result
)
as begin
    set nocount, xact_abort on
    begin try
        declare @msg nvarchar(max);

        -- according to sp_getapplock, the first 32 characters can be retrieved in plain text;
        -- since we plan on scanning sys.dm_tran_locks, and we don't know the hash function, this
        -- is important. We use "changefeed/<object_id in hex>/<shard_id in hex>" as the lock name.
        declare @lockname nvarchar(32) = concat('changefeed/'
          , convert(nvarchar(8), convert(varbinary(4), @object_id), 2)
          , '/'
          , convert(nvarchar(8), convert(varbinary(4), @shard_id), 2)
          );

        exec @result = sp_getapplock @Resource = @lockname, @LockMode = 'Exclusive', @LockOwner = 'Transaction', @LockTimeout = @timeout;
        -- Caller is responsible for checking @result
    end try
    begin catch
        if @@trancount > 0 rollback;
        ;throw;
    end catch
end

go

create or alter procedure [changefeed].kill_blocking_and_blocked_sessions
( @object_id int = null
, @table nvarchar(max) = null
, @shard_id int
, @timeout int
, @kill_count int = 0 output
)
as begin
    set nocount, xact_abort on
    declare @msg nvarchar(max)

    begin try
        if @object_id is null
        begin
            set @object_id = object_id(@table, 'table');
            if @object_id is null
                begin
                    set @msg = concat('Table not found: ', quotename(@table));
                    ;throw 77001, @msg, 1;
                end
        end

        -- resource_description will be for instance "0:[changefeed/01234567/89abcdef]:(0446bf9a)";
        -- make a pattern to match it using the "like" operator. % is wildcard, brackets escaped.
        declare @resource_description_pattern nvarchar(max) = concat('%[[changefeed/'
            , convert(nvarchar(8), convert(varbinary(4), @object_id), 2)
            , '/'
            , convert(nvarchar(8), convert(varbinary(4), @shard_id), 2)
            , ']]%'
            );

        -- Serialize all kills done by changefeed. Typically there could be many queued transactions
        -- timing out at about the same time so races are likely; and, well, it seems nice to ensure
        -- that there's not a race between the `select` and the `kill` below. Just use a single
        -- lock for all shards here. Hard-coded timeout of 1 second -- this really should never happen...
        declare @result int
        exec @result = sp_getapplock @Resource = 'changefeed/kill_blocking_session', @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 1000
        if @result < 0
        begin
            set @msg = concat('[changefeed].kill_blocking_session not able to get lock: ', @result);
            ;throw 77201, @msg, 0
        end

        declare @sessions table (
            session_id int not null,
            has_locked_beyond_timeout bit not null
        );

        insert into @sessions (session_id, has_locked_beyond_timeout)
        select
            lock.request_session_id
          , iif(lock.request_status = 'GRANT', 1, 0)
        from sys.dm_tran_locks as lock
        join sys.dm_tran_active_transactions as txn
            on lock.request_owner_type = 'TRANSACTION' and lock.request_owner_id = txn.transaction_id
        where
            lock.resource_type = 'APPLICATION'
            and lock.resource_description like @resource_description_pattern
            and (
                -- We get *all* sessions associated with the lock, *both* request_status=WAIT and request_status=GRANT.
                -- This is because there is a somewhat high risk that if one session is dead due to pod power-off,
                -- other sessions waiting for the same lock can be so as well.


                lock.request_status in ('WAIT', 'GRANT')
                and datediff(millisecond, txn.transaction_begin_time, sysutcdatetime()) > (case
                    when lock.request_status = 'WAIT' then
                        -- If the status is WAIT, we immediately kill it and have the backend retry, to signal
                        -- that it is still up.
                        -- Will this cause a storm of re-connects? Yes, but it's not going to be inherently worse
                        -- than when Azure SQL has a failover, which happens regularly, so the systems should be
                        -- able to sustain this. Rough heuristic though: If they arrived in the past 100 ms, then
                        -- probably let them alone...
                        100
                    else
                        -- For the GRANT status, something is actively running in progress, so make
                        -- sure to only kill it if it has lived longer than the timeout
                        @timeout
                    end)
            );

        -- if the lock acquiring timed out because of a long queue of transactions, we don't kill it;
        -- only kill a session with the same transaction having lived longer than the timeout

        -- we only want to start killing sessions if we identified a blocking long-lived transaction
        declare @sql nvarchar(max)
        declare @session_id int
        if (select count(*) from @sessions where has_locked_beyond_timeout = 1) > 0
        begin
            -- iterate through @sessions to kill them; a "cursor" is an iterator...
            declare session_cursor cursor for
                select session_id from @sessions order by has_locked_beyond_timeout
                for read only;

            open session_cursor;
            while 1 = 1
            begin
                fetch next from session_cursor into @session_id;
                if @@fetch_status <> 0 break;

                -- the kill command doesn't take arguments, so need to do it the hard way..
                set @sql = concat('kill ', @session_id)
                exec sp_executesql @sql;

                set @kill_count = @kill_count + 1;
            end
            close session_cursor;
            deallocate session_cursor;
        end

        exec sp_releaseapplock @Resource = 'changefeed/kill_blocking_session', @LockOwner = 'Session';
    end try
    begin catch
        if @@trancount > 1 rollback;
        exec sp_releaseapplock @Resource = 'changefeed/kill_blocking_session', @LockOwner = 'Session';
        ;throw;
    end catch
end

go

-- See REFERENCE.md for documentation.
create procedure [changefeed].init_ulid
  ( @object_id int = null
  , @table nvarchar(max) = null
  , @shard_id int = 1
  -- TODO: previous_shard
  , @timeout int = -1  -- milliseconds; -1 is infinity
  , @time_hint datetime2(3) = null
  , @random_bytes binary(10) = null
  , @count bigint = 68719476736  -- 1 TB of ULIDs...

  , @time datetime2(3) = null output
  , @ulid binary(16) = null output
  , @ulid_high binary(8) = null output
  , @ulid_low bigint = null output
  , @did_timeout bit = 0 output
  )
as begin
    set nocount, xact_abort on

    begin try
        declare @msg nvarchar(max)

        if @@trancount = 0 throw 77000, 'Please call init_ulid in a database transaction, so that you avoid race conditions with consumers. See README.md.', 0;

        exec sp_set_session_context N'changefeed.ulid_high', null;
        exec sp_set_session_context N'changefeed.ulid_low', null;

        if @time_hint is null set @time_hint = sysutcdatetime();
        if @random_bytes is null set @random_bytes = crypt_gen_random(10);

        if @object_id is null
        begin
            set @object_id = object_id(@table, 'table');
            if @object_id is null
            begin
                set @msg = concat('Table not found: ', quotename(@table));
                ;throw 77001, @msg, 1;
            end
        end

        -- Compensating measure for the issue with client-managed transactions and sudden power-off
        -- being able to block a partition. Take a lock with a timeout, and if it times out, kill
        -- the session having the lock...
        declare @lock_result int
        exec [changefeed].lock_shard @object_id = @object_id, @shard_id = @shard_id, @timeout = @timeout, @result = @lock_result output
        if @lock_result = -1
        begin

            -- Ideally, we'd want to call [changefeed].kill_blocking_and_blocked_sessions here.
            -- BUT, issuing kill is not allowed inside transactions, so we need to just signal
            -- the caller. The protocol is that if @ulid is null after the call,
            -- and there wasn't en error, there's a timeout. There is an additional @did_timeout
            -- flag for good measure, depending on what caller is comfortable with trusting
            -- error return status or not
            set @did_timeout = 1
            return
        end
        else if @lock_result < -1
        begin
            -- non-timeout errors we throw an error

            set @msg = concat('Error locking shard (non-timeout). @object_id='
                , @object_id, '@shard_id=', @shard_id, ', @lock_result=', @lock_result);
            ;throw 77003, @msg, 0;
        end
        -- @lock_result is positive! We are past the guard for sudden pod power-offs, and can proceed
        -- with the rest of the logic. Lock is owned by transaction so will be automatically released.

        exec [changefeed].private_core_init_ulid
            @object_id = @object_id
          , @shard_id = @shard_id
          , @time_hint = @time_hint
          , @random_bytes = @random_bytes
          , @count = @count
          , @time = @time output
          , @ulid_high = @ulid_high output
          , @ulid_low = @ulid_low output;

        set @ulid = @ulid_high + convert(binary(8), @ulid_low);
        set @did_timeout = 0;

        -- Set some session context variables that will be used by [changefeed].get_ulid; for convenience.
        exec sp_set_session_context N'changefeed.ulid_high', @ulid_high;
        exec sp_set_session_context N'changefeed.ulid_low', @ulid_low;
    end try
    begin catch
        if @@trancount > 0 rollback;
        ;throw;
    end catch

end;

go

create procedure [changefeed].private_core_init_ulid
  ( @object_id int = null
  , @shard_id int = 1
  , @time_hint datetime2(3)
  , @random_bytes binary(10)
  , @count bigint = 68719476736  -- 1 TB of ULIDs...

  , @time datetime2(3) = null output
  , @ulid_high binary(8) = null output
  , @ulid_low bigint = null output
  )
as begin
    set nocount, xact_abort on

    begin try
        declare @msg nvarchar(max)

        if @@trancount = 0 throw 77300, 'Do not call private_core_init_ulid directly', 0;

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
          , ulid_low = ulid_low_range_end
        from [changefeed].shard_state_ulid shard_state with (serializable, rowlock)
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
            exec [changefeed].private_core_init_ulid
                @object_id = @object_id
              , @shard_id = @shard_id
              , @count = @count
              , @time_hint = @time_hint
              , @random_bytes = @random_bytes
              , @time = @time output
              , @ulid_high = @ulid_high output
              , @ulid_low = @ulid_low output;
            end

    end try
    begin catch
        if @@trancount > 0 rollback;
        ;throw;
    end catch
end;

go

create function [changefeed].get_ulid(@i bigint) returns binary(16)
as begin
    return convert(binary(8), session_context(N'changefeed.ulid_high'))
        + convert(binary(8), convert(bigint, session_context(N'changefeed.ulid_low')) + @i);
end
