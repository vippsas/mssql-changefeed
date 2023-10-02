package main

//language=sql
const sweepSql = `
    begin try
        set transaction isolation level read committed;
        declare @endtime datetime2(3) = dateadd(millisecond, @duration_milliseconds, sysutcdatetime());

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
                1,
				0,
				c.change_id,
                row_number = row_number() over (partition by c.feed_id, c.shard order by c.change_id)
			from changefeed.change as c with (index=ix_needs_seqnum)
            where c.feed_id = 1 and c.shard = 0
            	and c.change_sequence_number is null
			order by c.change_id  -- important!, see "Consideration to partitioning and event ordering"
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

/*            select
                change_count = @change_count + (select isnull(sum(change_count), 0) from @sweepresult),
                max_lag_milliseconds = select max(lag_milliseconds) from @sweepresult where change_count > 0,
                iterations = @iterations + 1;*/


            if sysutcdatetime() > @endtime break;

			waitfor delay '00:00:00.005'
        end

		select 0, 0, 0;

    end try
    begin catch
        if @@trancount > 0 rollback;  -- due to inlined changefeed.sweep

        ;throw
    end catch
`
