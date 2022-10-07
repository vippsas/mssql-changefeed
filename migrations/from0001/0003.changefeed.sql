-- mssql-changefeed migration 0003
--
-- THE ORIGINAL VERSION OF THIS FILE LIVES IN github.com/vippsas/mssql-changefeed.
--
-- IF YOU EDIT THIS COPY IN YOUR LOCAL REPOSITORY, PLEASE RECORD THE CHANGES HERE, TO HELP
-- YOURSELF WITH FUTURE UPGRADES:
--
-- EDIT: TO ACCOMMODATE USING THE SAME DB AS PEONLINE IN CI, WE HAVE CREATED OUR OWN SCHEMA changefeed
-- END CHANGES
--

-- changefeed.sweep performs a single assignment of up to 1000 next change_sequence_number
-- in a given sweep_group. It will raise an error in the event of a race with a simultaneous
-- call; therefore it should usually be protected by a lock (see changefeed.sweep_loop).
-- It does a select (feed_id, shard_id, change_count, lag_milliseconds).
create or alter procedure changefeed.sweep(@sweep_group smallint)
    as begin
    set nocount, xact_abort on
begin try

        declare @toassign table
            ( feed_id smallint not null
            , shard smallint not null
            , change_id bigint not null
            , row_number bigint not null
            );

set transaction isolation level read committed;
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

        declare @counts table
            ( feed_id smallint not null
            , shard smallint not null
            , change_count int not null
            , primary key (feed_id, shard)
            );

insert into @counts
select feed_id, shard, count(*) as change_count
from @toassign
group by feed_id, shard;

declare @assigned_range table
            ( feed_id smallint not null
            , shard smallint not null
            , change_count int not null
            , last_change_sequence_number bigint not null
            , lag_milliseconds int not null
            , primary key (feed_id, shard)
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
                 join changefeed.change c on c.change_id = x.change_id and c.feed_id = x.feed_id and c.shard = x.shard
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
