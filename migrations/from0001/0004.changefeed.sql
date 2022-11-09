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
create or alter procedure changefeed.sweep_loop(
    @sweep_group smallint,
    @wait_milliseconds int,
    @duration_milliseconds int,
    @sleep_milliseconds int,
    @change_count int output,
    @max_lag_milliseconds bigint output,
    @iterations int output
    )
as begin
    set nocount, xact_abort on
    declare @sweep_lock nvarchar(255) = changefeed.sweep_lock_name(@sweep_group);
    declare @longpoll_lock nvarchar(255) = concat('changefeed.longpoll/', @sweep_group);

    begin try
        set transaction isolation level read committed;
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
            last_change_sequence_number bigint not null,
            lag_milliseconds bigint not null
        );

        declare @longpoll_locks table (
            i int primary key,
            feed_id int,
            shard int,
            lock nvarchar(255)
        );

        declare @toassign table (
              feed_id smallint not null
            , shard smallint not null
            , change_id bigint not null
            , row_number bigint not null
        );

        declare @counts table(
            feed_id smallint not null
            , shard smallint not null
            , change_count int not null
            , primary key (feed_id, shard)
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

        -- If you pass duration=0, we want exactly 1 iteration, so doing the check
        -- for endtime at the end of the loop instead of the start.
        while 1 = 1
        begin
            delete from @sweepresult;
            delete from @toassign;
            delete from @counts;

            -- BEGIN INLINE changefeed.sweep()
            -- To avoid using #temp tables for communication or insert-exec,
            -- which has some issues with propagating errors, we inline changefeed.sweep()
            -- here. The changes in the inlined version is:
            --
            --    * setting transaction isolation level has been moved up to
            --    * no try/catch; instead added "if @@trancount > 0 rollback;" to this proc
            --    * @toassign/@counts/@sweepresult declarations moved up top, and instead deleting contents here
            --    * @sweepresult "escapes" from the procedure and is used afterwards


            begin transaction;

            insert into @toassign
            select top(1000)
                s.feed_id,
                s.shard,
                x.change_id,
                x.row_number
            from changefeed.shard as s
            cross apply (
                select top(1000)
                    c.change_id,
                    row_number = row_number() over (partition by c.feed_id, c.shard order by c.change_id)
                from changefeed.change as c with (index=ix_needs_seqnum)
                where c.feed_id = s.feed_id and c.shard = s.shard
                  and c.change_sequence_number is null
                order by c.change_id  -- important!, see "Consideration to partitioning and event ordering"
            ) x
            where s.sweep_group = @sweep_group
            option (maxdop 1);

            declare @t0 datetime2 = sysutcdatetime();


            insert into @counts
            select feed_id, shard, count(*) as change_count
            from @toassign
            group by feed_id, shard;

            update s
            set
                s.last_change_sequence_number = s.last_change_sequence_number + co.change_count,
                last_sweep_time = @t0
            output
                inserted.feed_id,
                inserted.shard,
                co.change_count,
                deleted.last_change_sequence_number,
                datediff_big(millisecond, deleted.last_sweep_time, @t0)
                into @sweepresult
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
            join changefeed.change c on c.change_id = x.change_id and c.feed_id = x.feed_id and c.shard = x.shard
            join @sweepresult as r on r.feed_id = c.feed_id and r.shard = c.shard
            where
                change_sequence_number is null; -- protect against racing processes!

            if @@rowcount <> (select sum(change_count) from @sweepresult)
            begin
                throw 55202, 'race, changefeed.sweep called at the same time for same feed_id, shard_id', 1;
            end

            commit transaction

            -- END INLINE

            select
                @change_count = @change_count + (select isnull(sum(change_count), 0) from @sweepresult),
                @max_lag_milliseconds = (select max(lag_milliseconds) from (
                  select lag_milliseconds from @sweepresult where change_count > 0
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

            if sysutcdatetime() > @endtime break;

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
        if @@trancount > 0 rollback;  -- due to inlined changefeed.sweep

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
