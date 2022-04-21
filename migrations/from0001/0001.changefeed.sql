-- mssql-changefeed VERSION 1
--
-- github.com/vippsas/mssql-changefeed
--
-- IF YOU EDIT THIS FILE FROM UPSTREAM, PLEASE RECORD THE CHANGES HERE TO HELP FUTURE UPGRADES:
--
-- END CHANGES
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

-- changefeed.sweep performs a single assignment of up to 1000 next change_sequence_number
-- in a given sweep_group. It will raise an error in the event of a race with a simultaneous
-- call; therefore it should usually be protected by a lock (see changefeed.sweep_loop).
-- It does a select (feed_id, shard_id, change_count, lag_milliseconds).
create procedure changefeed.sweep(@sweep_group smallint)
as begin
  set nocount, xact_abort on
  begin try

    declare @toassign table (
      feed_id smallint not null,
      shard smallint not null,
      change_id bigint not null,
      row_number bigint not null
    );

    set transaction isolation level read committed;
    begin transaction;

    insert into @toassign
    select top(1000)
      s.feed_id,
      s.shard,
      c.change_id,
      row_number = row_number() over (partition by s.feed_id, s.shard order by c.change_id)
    from changefeed.shard as s
    join changefeed.change c on c.feed_id = s.feed_id and c.shard = s.shard
    where
        c.change_sequence_number is null
      and s.sweep_group = @sweep_group
    order by c.change_id  -- important!, see "Consideration to partitioning and event ordering"
    option (maxdop 1);

    declare @t0 datetime2 = sysutcdatetime();

    declare @counts table (
        feed_id smallint not null,
        shard smallint not null,
        change_count int not null,
        primary key (feed_id, shard)
    );

    insert into @counts
    select feed_id, shard, count(*) as change_count
    from @toassign
    group by feed_id, shard;

    declare @assigned_range table (
        feed_id smallint not null,
        shard smallint not null,
        change_count int not null,
        last_change_sequence_number bigint not null,
        lag_milliseconds int not null,
        primary key (feed_id, shard)
    );

    update s
    set
      s.last_change_sequence_number = s.last_change_sequence_number + co.change_count,
      last_sweep_time = @t0
    output
        inserted.feed_id,
        inserted.shard,
        co.change_count,
        deleted.last_change_sequence_number,
        datediff(millisecond, deleted.last_sweep_time, @t0)
        into @assigned_range
    from changefeed.shard as s
    join @counts as co on co.shard = s.shard and co.feed_id = s.feed_id
    option (maxdop 1);

    if @@rowcount <> (select count(*) from @counts)
    begin
        throw 55201, 'unexpected update count in changefeed.shard', 1;
    end;

    update c
    set
      change_sequence_number = r.last_change_sequence_number + x.row_number
    from @toassign as x
    join changefeed.change c on c.change_id = x.change_id
    join @assigned_range as r on r.feed_id = c.feed_id and r.shard = c.shard
    where
      change_sequence_number is null; -- protect against racing processes!

    if @@rowcount <> (select sum(change_count) from @assigned_range)
    begin
      throw 55202, 'race, changefeed.sweep called at the same time for same feed_id, shard_id', 1;
    end

    commit transaction

    select
        feed_id,
        shard,
        change_count,
        lag_milliseconds
    from @assigned_range
    where change_count > 0
    order by feed_id, shard

  end try
  begin catch
    if @@trancount > 0 rollback transaction;
    -- produce zero rows in the right format as output
    select 0 as feed_id, 0 as shard, 0 as change_count, 0 as lag_milliseconds where 1 = 0;
    ;throw
  end catch
end

go

create function changefeed.sweep_lock_name(@sweep_group smallint)
returns nvarchar(255)
as begin
    return concat('changefeed.sweep_loop/', @sweep_group);
end

go
-- changefeed.sweep_loop acquires a lock and then keeps sweeping for the specified time
-- on a feed_id/shard. It also does some locking gymnastics in order to signal any
-- waiting long-pollers that new sequence numbers have been assigned.
--
-- The simple pattern is to set @waitmax_milliseconds and @duration_milliseconds to
-- the same value. The former can be set lower if you for some reason don't want to
-- wait but return immediately if a process is already sweeping.
--
-- Output args: To make use by some clients simpler, if @iterations=0, then max_lag_milliseconds is 0
-- even if it could technically be said to be NULL..
--
-- See example usage in README.md
create procedure changefeed.sweep_loop(
    @sweep_group smallint,
    @wait_milliseconds int,
    @duration_milliseconds int,
    @sleep_milliseconds int,
    @change_count int output,
    @max_lag_milliseconds int output,
    @iterations int output
    )
as begin
    set nocount, xact_abort on
    declare @sweep_lock nvarchar(255) = changefeed.sweep_lock_name(@sweep_group);
    declare @longpoll_lock nvarchar(255) = concat('changefeed.longpoll/', @sweep_group);

    begin try
        declare @endtime datetime2(3) = dateadd(millisecond, @duration_milliseconds, sysutcdatetime());

        -- `waitfor delay` is weird, it takes exactly type `datetime` but ignores the date and uses only the time component
        -- The date we use here in order to use the dateadd function to convert from integer milliseconds is arbitrary
        declare @waitfor_arg datetime = dateadd(millisecond, @sleep_milliseconds, '2000-01-01 00:00:00')

        declare @lockresult int
        declare @i int
        declare @longpoll_lock_count int

        declare @sweepresult table (
            feed_id smallint not null,
            shard smallint not null,
            change_count int not null,
            lag_milliseconds int not null
        );

        declare @longpoll_locks table (
            i int primary key,
            feed_id int,
            shard int,
            lock nvarchar(255)
        );

        select @change_count = 0, @iterations = 0, @max_lag_milliseconds = 0;

        insert into @longpoll_locks (i, feed_id, shard, lock)
        select
            row_number() over (order by feed_id, shard),
            feed_id,
            shard,
            concat('changefeed.longpoll/', feed_id, '/', shard)
        from changefeed.shard
        where sweep_group = @sweep_group and longpoll = 1;
        select @longpoll_lock_count = count(*) from @longpoll_locks;

        -- Outer lock: Protects the sweep-loop so that the process is only carried out by
        -- one session at the time.
        exec @lockresult = sp_getapplock @Resource = @sweep_lock, @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = @wait_milliseconds;
        -- Failure to get lock results in ordinary return with change_count=0
        if @lockresult < 0 return

        -- Inner locks: Lock system used to signal long-pollers that there is more data to read.
        -- This is not the same as the outer lock because that would cause control to jump between different
        -- sweeper threads all the time which would be more confusing (and potentially deadlocks? At least
        -- the outer lock makes it simple to prove we don't deadlock). Since these locks are protected by the
        -- lock above we hardcode a 1 second timeout and crash if we don't get them.

        select @i = 1;
        while @i <= @longpoll_lock_count
        begin
            select @longpoll_lock = lock from @longpoll_locks where i = @i;

            exec @lockresult = sp_getapplock @Resource = @longpoll_lock, @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 1000
            if @lockresult < 0
            begin
                throw 55001, 'changefeed.sweep_lock assertion failed: Not able to get initial @longpoll_lock', 1;
            end

            select @i = @i + 1
        end

        -- We do a locking dance to support long-polling. Fundamental behavour of Exclusive vs Shared
        -- locks (see demo_longpoll_test.go for tests of how these mechanism works, confirmed
        -- by some internet searches):
        --
        -- By releasing and re-taking an Exclusive ock in quick sequence, this will happen:
        -- 1) All readers currently blocked on getting a Shared lock (having called changefeed.longpoll) will get it.
        --    These then are unblocked and can quickly release it.
        -- 2) This procedure then re-take the Exclusive lock; this is prioritized over any new Shared lock
        --    requests coming in, so NEW calls to changefeed.longpoll will not stop us from getting the lock back.
        --

        while sysutcdatetime() < @endtime
        begin
            delete from @sweepresult;

            insert into @sweepresult
            exec changefeed.sweep @sweep_group = @sweep_group;
            select
                @change_count = @change_count + (select isnull(sum(change_count), 0) from @sweepresult),
                @max_lag_milliseconds = (select max(lag_milliseconds) from (
                  select lag_milliseconds from @sweepresult
                  union all select @max_lag_milliseconds as lag_milliseconds where @max_lag_milliseconds is not null
                ) t),
                @iterations = @iterations + 1;

            select @i = 1
            while @i < @longpoll_lock_count
            begin
                select @longpoll_lock = null;
                -- See comments in changefeed.longpoll for how this works:
                select
                    @longpoll_lock = lock
                from @longpoll_locks locks
                join @sweepresult result on result.feed_id = locks.feed_id and result.shard = locks.shard
                where result.change_count > 0 and locks.i = @i;

                if @longpoll_lock is not null
                begin

                    exec sp_releaseapplock @Resource = @longpoll_lock, @LockOwner = 'Session';
                    exec @lockresult = sp_getapplock @Resource = @longpoll_lock, @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 1000
                    if @lockresult < 0
                    begin
                        throw 55004, 'changefeed.sweep_lock assertion failed: Not able to re-acquire @longpoll_lock', 1;
                    end
                end

                select @i = @i + 1
            end

            waitfor delay @waitfor_arg;
        end

        select @i = @longpoll_lock_count;
        while @i >= 1
        begin
            select @longpoll_lock = lock from @longpoll_locks where i = @i;
            exec sp_releaseapplock @Resource = @longpoll_lock, @LockOwner = 'Session';

            select @i = @i - 1
        end

        exec sp_releaseapplock @Resource = @sweep_lock, @LockOwner = 'Session';
    end try
    begin catch

        select @i = @longpoll_lock_count;
        while @i >= 1
        begin
             select @longpoll_lock = lock from @longpoll_locks where i = @i;
             exec sp_releaseapplock @Resource = @longpoll_lock, @LockOwner = 'Session';

             select @i = @i - 1
        end

        exec sp_releaseapplock @Resource = @sweep_lock, @LockOwner = 'Session';

        ;throw
    end catch
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
