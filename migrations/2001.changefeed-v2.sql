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

create table [changefeed].shard_v2
  ( feed_id uniqueidentifier not null
  , shard_id int not null
  , time datetime2(3) not null
        constraint def_shard_v2_time default '1970-01-01'

  -- We store the ULID split into 2 components of 64 bits each.
  -- The first part has the timestamp and 2 random bytes; the latter
  -- part is random, but one can use it as the basis of a sequence
  -- to generate millions of IDs within the same timestamp.
  -- Bit 63 always starts off as 0 to prevent overflow while doing this.
  , ulid_prefix binary(8) not null
        constraint def_shard_v2_ulid_prefix default 0x0
  -- ulid_suffix is the value *after* the last ULID that was generated;
  -- i.e., if you load state and the time is the same, you can use this
  -- as the first ULID
  , ulid_suffix bigint not null
        constraint def_shard_v2_ulid_suffix default 0
  -- For convenience, the ulid is displayable directly. Also serves as documentation:
  , ulid as ulid_prefix + convert(binary(8), ulid_suffix)
  , constraint pk_shard_v2 primary key (feed_id, shard_id)
  );

-- can't imagine locks escalating on this table, but would sure be in
-- trouble if they did, so just for good measure:
alter table [changefeed].shard_v2 set (lock_escalation = disable);

go

-- insert_shard will ensure that a shard exists, without causing an error if it does
create procedure [changefeed].insert_shard
  ( @feed_id uniqueidentifier,
    @shard_id int
  )
as begin
    insert into [changefeed].shard_v2 (feed_id, shard_id)
    select
        @feed_id
      , @shard_id
    where
        not exists (
            select 1 from [changefeed].shard_v2 with (updlock, serializable, rowlock)
            where feed_id = @feed_id and shard_id = @shard_id);
end;

go

create procedure [changefeed].generate_ulid_blocking
  ( @feed_id uniqueidentifier
  , @shard_id int
  , @count int
  , @random_bytes binary(10) = null
  , @time_hint datetime2(3) = null
  , @time datetime2(3) = null output
  , @ulid binary(16) = null output
  , @ulid_prefix binary(8) = null output
  , @ulid_suffix bigint = null output
  )
as begin
    if @@trancount = 0 throw 77000, 'Please call generate_ulid_blocking in a database transaction, so that you avoid race conditions with consumers. See README.md.', 0;

    if @count is null set @count = 1;
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
      , @ulid_prefix = ulid_prefix = new_ulid_prefix
      , @ulid_suffix = ulid_suffix_range_start
      , @ulid = new_ulid_prefix + convert(binary(8), ulid_suffix_range_start)
      , ulid_suffix = ulid_suffix_range_end
    from [changefeed].shard_v2 shard_state with (serializable, rowlock)
    cross apply (select
        new_time = iif(@time_hint > shard_state.time, @time_hint, shard_state.time)
    ) let1
    cross apply (select
        new_ulid_prefix = (case
            when @time_hint > shard_state.time then
                -- New timestamp, so generate new prefix. The prefix is simply the timestamp + 2 random bytes.
                convert(binary(6), datediff_big(millisecond, '1970-01-01 00:00:00', new_time)) + substring(@random_bytes, 1, 2)
            else
                shard_state.ulid_prefix
            end)
      , ulid_suffix_range_start = (case
          when @time_hint > shard_state.time then
              -- Start suffix in new place.
              -- The mask 0xbfffffffffffffff will zero out bit 63, ensuring that overflows will not happen
              -- as the caller adds numbers to this
              convert(bigint, substring(@random_bytes, 3, 8)) & 0xbfffffffffffffff
          else
              -- We store the *last* ULID used in shard_state, so the start of the range
              -- we allocate now is +1.
              shard_state.ulid_suffix + 1
          end)
    ) let2
    cross apply (select
        -- The result of this call allocates ULIDs in the range [ulid_suffix_range_start, ..., ulid_suffix_range_end),
        -- i.e. endpoint is exclusive
        ulid_suffix_range_end = ulid_suffix_range_start + @count
    ) let3
    where
        feed_id = @feed_id and shard_id = @shard_id;

    if @@rowcount = 0
    begin
        -- Note: In the event that @@rowcount = 0, then no @variables will have been changed by
        -- the above `update` statement. So we start from scratch.
        exec [changefeed].insert_shard @feed_id = @feed_id, @shard_id = @shard_id;
        -- Try again
        exec [changefeed].generate_ulid_blocking
            @feed_id = @feed_id
          , @shard_id = @shard_id
          , @count = @count
          , @time_hint = @time_hint
          , @random_bytes = @random_bytes
          , @time = @time output
          , @ulid = @ulid output
          , @ulid_prefix = @ulid_prefix output
          , @ulid_suffix = @ulid_suffix output
    end
end;

go

create procedure [changefeed].begin_ulid_transaction
  ( @feed_id uniqueidentifier
  , @shard_id int
  , @timeout int = 100
  , @time_hint datetime2(3) = null output
  , @ulid_prefix binary(8) = null output
  , @ulid_suffix bigint = null output
  )
as begin
    if @@trancount = 0 throw 55100, 'Please read the documentation and call commit_ulid_transaction in a *special kind of* database transaction.', 0;

    if @time_hint is null set @time_hint = sysutcdatetime();

    declare @msg nvarchar(max)
    declare @lock_name nvarchar(255) = concat('changefeed/', @feed_id, '/', @shard_id);
    declare @lock_result int

    exec @lock_result = sp_getapplock
        @Resource = @lock_name
      , @LockMode = 'Exclusive'
      , @LockOwner = 'Transaction'
      , @LockTimeout = @timeout;
    if @lock_result < 0
    begin
        if @lock_result = -1
        begin
            -- We have to assume that another session halted in the middle
            -- (e.g., due to client-managed transaction and client power-off or disconnection).
            -- Fall back to blocking, conservative mode. The only disadvantage of this mode
            -- is that if there is *another* client power-off/disconnection
            set @msg = concat('sp_getapplock result: ', @msg)
            rollback;
            throw 55103, @msg, 0;
        end
        else
        begin
            set @msg = concat('sp_getapplock result: ', @msg)
            rollback;
            throw 55103, @msg, 0;
        end
    end

    declare @previous_time datetime2(3);
    declare @random_bytes binary(10);

    select
        @ulid_prefix = ulid_prefix
      , @ulid_suffix = ulid_suffix
      , @previous_time = time
    from [changefeed].shard_v2
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
        -- new timestamp; re-seed entropy
        set @random_bytes = crypt_gen_random(10);
        set @ulid_prefix = convert(binary(6), datediff_big(millisecond, '1970-01-01 00:00:00', @time_hint)) + substring(@random_bytes, 1, 2);
        -- The mask 0xbfffffffffffffff will zero out bit 63, ensuring that overflows will not happen
        set @ulid_suffix = convert(bigint, substring(@random_bytes, 3, 8)) & 0xbfffffffffffffff
    end

    -- If there are several backend<->SQL transactions going on, it's hard to
    -- channel parameters to commit_ulid_transaction.
    -- See comment in transaction.go. So, while we are at it, just always use
    -- this method so that parameters don't have to be passed again
    -- to commit_ulid_transaction.
    exec sp_set_session_context N'changefeed.feed_id', @feed_id;
    exec sp_set_session_context N'changefeed.shard_id', @shard_id;
    exec sp_set_session_context N'changefeed.ulid_prefix', @ulid_prefix;
    exec sp_set_session_context N'changefeed.time', @time_hint;
end

go

create procedure [changefeed].commit_ulid_transaction
  ( @ulid_suffix bigint = null
  )
as begin
    declare @feed_id uniqueidentifier = convert(uniqueidentifier, session_context(N'changefeed.feed_id'));
    declare @shard_id int = convert(int, session_context(N'changefeed.shard_id'));
    declare @ulid_prefix binary(8) = convert(binary(8), session_context(N'changefeed.ulid_prefix'))
    declare @time datetime2(3) = convert(datetime2(3), session_context(N'changefeed.time'))

    if @@trancount = 0 or @feed_id is null throw 77001, 'Please read the documentation and call commit_ulid_transaction in a *special kind of* database transaction.', 0;

    update [changefeed].shard_v2
    set
        ulid_prefix = @ulid_prefix
        -- for good measure, just add 1; so that @ulid_suffix can be passed inclusive or exclusive, doesn't matter..
      , ulid_suffix = @ulid_suffix + 1
      , time = @time
    where
      feed_id = @feed_id and shard_id = @shard_id;
end


