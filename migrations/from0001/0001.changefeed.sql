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

-- PLEASE NOTE: changefeed.sweep_loop used to be here, but has moved to
-- 0004.changefeed.sql