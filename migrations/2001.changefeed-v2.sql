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

create table [changefeed].shard_state
  ( object_id int not null
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
  , constraint pk_shard_ulid primary key (object_id, shard_id)
  );

-- can't imagine locks escalating on this table, but would sure be in
-- trouble if they did, so just for good measure:
alter table [changefeed].shard_state set (lock_escalation = disable);

go


-- insert_shard will ensure that a shard exists, without causing an error if it does
create procedure [changefeed].insert_shard
  ( @object_id int
  , @shard_id int
  )
as begin
    insert into [changefeed].shard_state (object_id, shard_id)
    select
        @object_id
      , @shard_id
    where
        not exists (
            select 1 from [changefeed].shard_state with (updlock, serializable, rowlock)
            where object_id = @object_id and shard_id = @shard_id);
end;


go

create procedure [changefeed].lock_shard
  ( @object_id int
  , @shard_id int
  , @timeout int
)
as begin
    set nocount, xact_abort on
    begin try
        declare @msg nvarchar(max);

        -- The following lock is always Shared, never Exclusive. It simply puts an entry in the sys.dm_tran_lock
        -- table that can be inspected. This is to add a bit of forward-compatability with allowing longer
        -- workloads with longer timeouts or similar..
/*        declare @result int
        exec sp_getapplock @Resource = 'changefeed/timeout-opt-in', @LockMode = 'Shared', @LockOwner = 'Transaction', @LockTimeout = 0;
        if @result <> 0
        begin
            throw 77200, 'Could not get application lock "changefeed/timeout-opt-in", this is very unexpected', 0;
        end*/

        declare @result int;
        -- according to sp_getapplock, the first 32 characters can be retrieved in plain text;
        -- since we plan on scanning sys.dm_tran_locks, and we don't know the hash function, this
        -- is important. We use "changefeed/<object_id in hex>/<shard_id in hex>" as the lock name.
        declare @lockname nvarchar(32) = concat('changefeed/'
          , convert(nvarchar(8), convert(varbinary(4), @object_id), 2)
          , '/'
          , convert(nvarchar(8), convert(varbinary(4), @shard_id), 2)
          );

        exec sp_getapplock @Resource = @lockname, @LockMode = 'Exclusive', @LockOwner = 'Transaction', @LockTimeout = @timeout;
        if @result < -1
        begin
            set @msg = concat('Could not get lock: ', @lockname, ' , got error: ', @result);
            ;throw 77200, @msg, 0;
        end
        --else if @result = -1
        --begin
            -- timeout
        --end
    end try
    begin catch
        if @@trancount > 0 rollback;
        ;throw;
    end catch
end

go

create procedure [changefeed].kill_session_blocking_shard
( @object_id int
, @shard_id int
)
as begin
    set nocount, xact_abort on

    begin try
        -- resource_description will be for instance "0:[changefeed/01234567/89abcdef]:(0446bf9a)";
        -- make a pattern to match it using the "like" operator. % is wildcard, brackets escaped.
        declare @resource_description_pattern nvarchar(max) = concat('%[[changefeed/'
            , convert(nvarchar(8), convert(varbinary(4), @object_id), 2)
            , '/'
            , convert(nvarchar(8), convert(varbinary(4), @shard_id), 2)
            , ']]%'
            );

        declare @session_id int;
        select top(1) @session_id = request_session_id
        from sys.dm_tran_locks as lock
        where
                lock.resource_type = 'APPLICATION'
          and lock.request_status = 'GRANT'
          and lock.resource_description like @resource_description_pattern;

        -- the kill command doesn't take arguments, so need to do it the hard way..
        declare @sql nvarchar(max) = concat('kill ', @session_id)
        exec sp_executesql @sql;

    end try
    begin catch
        if @@trancount > 1 rollback;
        ;throw;
    end catch
end
