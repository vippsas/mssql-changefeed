-- mssql-changefeed migration 0001
--
-- THE ORIGINAL VERSION OF THIS FILE LIVES IN github.com/vippsas/mssql-changefeed.
--
-- IF YOU EDIT THIS COPY IN YOUR LOCAL REPOSITORY, PLEASE RECORD THE CHANGES HERE, TO HELP
-- YOURSELF WITH FUTURE UPGRADES:
--
-- END CHANGES
--
-- Upstream changelog:
--    v2: Removed changefeed.sweep from this file, it is introduced in 0002.changefeed.sql
--
create schema changefeed;

go

create table changefeed.change(
    feed_id smallint not null,
    shard smallint not null,
    change_id bigint not null,
    change_sequence_number bigint,

    constraint pk_change primary key (feed_id, shard, change_id)
) with (data_compression = page);


-- Create separate indexes for `null` and `not null`. This means one index will be
-- tiny/empty most of the time, while the other will only have inserts,
-- this has a better feel index-maintenance wise
create unique index ix_needs_seqnum on changefeed.change(feed_id, shard, change_id)
  where change_sequence_number is null;

create unique index ix_seqnum on changefeed.change(feed_id, shard, change_sequence_number)
  include (change_id)
  where change_sequence_number is not null
  with (data_compression = page);

create sequence changefeed.change_id as bigint
  -- start at a large number by default, giving plenty of room to write migration scripts to insert data below...
  start with 1000000000000000
  cache 1000;


go

create table changefeed.feed (
    feed_id smallint not null,
    name varchar(100) not null,
    comment nvarchar(max),

    constraint pk_feed primary key (feed_id),
);

create table changefeed.shard (
    feed_id smallint not null,
    shard smallint not null,
    sweep_group smallint not null
      -- The default sweep_group 0 is 'reserved' for all low-latency events in the DB
      -- where one does not want a dedicated sweeper
      constraint def_shard_sweep_group default 0,
    longpoll bit not null
      constraint def_shard_longpoll default 1,
    last_change_sequence_number bigint not null
        -- start at a large number by default, giving plenty of room to write migration scripts to insert data below
        -- also start on 2 instead of 1 to quickly visually distinguish from change_id..
        constraint def_feed_last_change_sequence_number default 2000000000000000,

    -- log data for returning an estimate of lag
    last_sweep_time datetime2(6) not null
        constraint def_last_sweep_time default sysutcdatetime(),

    constraint pk_shard primary key (feed_id, shard),
    constraint fk_shard_feed foreign key (feed_id) references changefeed.feed(feed_id),
);

create unique index ix_sweep_group on changefeed.shard(sweep_group, feed_id, shard);

go

-- changefeed.sweep had a bug; removing it from this migration file so that new
-- users of the library do not get the bug at any point.
-- 0002.changefeed.sql will add the bugfixed version; git history contains
-- for the buggy version. The diff:

-- +++ b/migrations/from0001/0001.changefeed.sql
-- @@ -153,7 +153,7 @@ as begin
--     set
--         change_sequence_number = r.last_change_sequence_number + x.row_number
--        from @toassign as x
-- -    join changefeed.change c on c.change_id = x.change_id
-- +    join changefeed.change c on c.change_id = x.change_id and c.feed_id = x.feed_id and c.shard = x.shard
--      join @assigned_range as r on r.feed_id = c.feed_id and r.shard = c.shard
--      where
--        change_sequence_number is null; -- protect against racing processes!


create function changefeed.sweep_lock_name(@sweep_group smallint)
returns nvarchar(255)
as begin
    return concat('changefeed.sweep_loop/', @sweep_group);
end

go

-- Block until the next change by the sweeper.
-- Note: THIS MUST BE USED WITH SOME CARE; there is a race between the time one blocks and when the sweeper
-- writes a change, and this race should be compensated with a call to changefeed.longpoll_guard in another
-- thread. See reference implementation in longpoll.go
create procedure changefeed.longpoll(
    @feed_id bigint,
    @shard smallint,
    @change_sequence_number bigint, -- only input..
    @timeout_milliseconds int
    )
as begin
    set nocount, xact_abort on

    declare @endtime datetime2 = dateadd(millisecond, @timeout_milliseconds, sysutcdatetime())
    declare @longpoll_lock nvarchar(255) = concat('changefeed.longpoll/', @feed_id, '/', @shard);
    declare @t datetime2
    declare @dt int
    declare @retval int
    declare @msg varchar(max)
    declare @new_sequence_number bigint

    declare @t0 datetime2 = sysutcdatetime()

    begin try
        while 1 = 1
        begin
            select @t = sysutcdatetime()
            select @dt = datediff(millisecond, @t, @endtime)

            if @dt < 0 break;

            -- Get, and immediately release, the lock. We assume the caller put us in a transaction...

            exec @retval = sp_getapplock @Resource = @longpoll_lock, @LockMode = 'Shared', @LockOwner = 'Transaction', @LockTimeout = @dt;

            if @retval = 1
            begin
                -- Happy day case, we got lock after a wait. Returning without checking updated sequence
                -- number (client could easily read to an even newer number by the time it reads data..)
                exec sp_releaseapplock @Resource = @longpoll_lock, @LockOwner = 'Transaction';

                return
            end
            else if @retval = 0
            begin
                -- Immediately got the lock. Something is probably fishy. If the sequence  Start to poll the
                -- sequence number and see if it changes, if we so we just got the lock at a *really* lucky
                -- moment in time. If it never changes, the sweep_loop is not running.
                exec sp_releaseapplock @Resource = @longpoll_lock, @LockOwner = 'Transaction';

                -- We want to always not read snapshot data, but latest committed state, hence the hint below.
                -- The reason we are in a transaction is really just to have the lock request have an easily
                -- identifiable owner for longpoll_guard.
                select
                    @new_sequence_number = last_change_sequence_number
                from changefeed.shard with (readcommittedlock)
                where feed_id = @feed_id and shard = @shard;

                if @change_sequence_number <> @new_sequence_number return;

                -- We'll try again after a wait of 100ms; but if this persists for a full second
                -- then the sweep_loop is not running and we throw an error.
                if datediff(millisecond, @t0, @t) > 1000
                begin
                    throw 55011, 'changefeed.longpoll assertion failed; perhaps changefeed.sweep_loop is not running?', 1;
                end
                waitfor delay '00:00:00.100'
            end
            else if @retval = -1
            begin
                -- Timeout
                return
            end
            else if @retval < 0
            begin
                -- Other error condition getting lock; throw error
                select @msg = concat('changefeed.longpoll: error while acquiring @longpoll_lock: ', @retval);
                throw 55010, @msg, 1;
            end

            -- As we loop around here we're in the same kind of race as before entering, which should
            -- be compensated for by using longpoll_guard.
        end
    end try
    begin catch
        -- Not releasing lock here; there really should never be a case where the lock is held on exit
        -- through an error, and anyway it is owned by the transaction.
        -- Also, since the caller did `begin transaction` we will let the caller roll it back too;
        ;throw
    end catch
end

go

-- Block until a given transaction_id has started on the longpolling block...
-- I.e., this will loop and poll for a *short* time (probably just one poll) so that we can confirm
-- that the call to longpoll did not race. Then the longpoll can block for a *long* time.
create procedure changefeed.longpoll_guard(@transaction_id bigint, @blocked bit output)
as begin
    set nocount, xact_abort on

    begin try
        declare @waitcase int = 1
        while 1 = 1
        begin
            -- Is the longpoll() call blocked yet?
            if exists(
                select 1 from sys.dm_tran_locks as lck
                where lck.resource_type = 'APPLICATION'
                    and lck.request_status = 'WAIT'
                    and lck.request_owner_type = 'TRANSACTION'
                    and lck.request_owner_id = @transaction_id
            )
            begin
                -- Success!
                select @blocked = 1
                return
            end
            -- No... does transaction still live?
            if not exists (select 1 from sys.dm_tran_active_transactions where transaction_id = @transaction_id)
            begin
                -- Non-success...
                select @blocked = 0
                return
            end
            -- Otherwise transaction is active but not yet blocked; wait and see.
            if @waitcase = 1 waitfor delay '00:00:00.05';
            else if @waitcase = 2 waitfor delay '00:00:00.010';
            else if @waitcase = 3 waitfor delay '00:00:00.020';
            else if @waitcase = 4 waitfor delay '00:00:00.050';
            else waitfor delay '00:00:00.100';

            select @waitcase = @waitcase + 1
        end
    end try
    begin catch
        ;throw
    end catch
end
