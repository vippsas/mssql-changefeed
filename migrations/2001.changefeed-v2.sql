
drop procedure if exists [changefeed].read_feed;
drop table if exists [changefeed].shard_state;
go
drop schema if exists [changefeed];
go


create schema [changefeed];
go

create table [changefeed].shard_state(
    shard_id int not null,

    time datetime2(3) not null,

    -- See EXPERTS-GUIDE.md for description of ulid_prefix
    -- and ulid_low.
    ulid_high binary(8) not null,
    ulid_low bigint not null,

    -- For convenience, the ulid is displayable directly. Also serves as documentation:
    ulid as ulid_high + convert(binary(8), ulid_low),

    constraint pk_shard_ulid primary key (shard_id)
);


alter table [changefeed].shard_state set (lock_escalation = disable);

go

create function [changefeed].gen_read_feed_sql(
    @object_id int,
    @changefeed_schema nvarchar(max)
)
returns nvarchar(max) as begin
    declare @table nvarchar(max) = concat(
        quotename(object_schema_name(@object_id)),
        '.',
        quotename(object_name(@object_id)))

    -- re-generate table name in a certain format from sys tables;
    -- @table_name can be quoted in different ways. We use unquoted, but qualified,
    -- name; e.g. [myschema.with.dot].[table.with.dot] will turn into
    -- myschema.with.dot.table.with.dot. In theory this can be ambigious, but assume
    -- noone will use names like this.
    declare @unquoted_qualified_table_name nvarchar(max) = (
        select concat(s.name, N'.', o.name)
        from sys.objects as o
                 join sys.schemas as s on o.schema_id = s.schema_id
        where o.object_id = @object_id);

    declare @feed_table nvarchar(max) = concat(
        quotename(@changefeed_schema),
        '.',
        quotename(concat('feed:', @unquoted_qualified_table_name)))

    declare @outbox_table nvarchar(max) = concat(
            quotename(@changefeed_schema),
            '.',
            quotename(concat('outbox:', @unquoted_qualified_table_name)))

    declare @read_feed_proc nvarchar(max) = concat(
            quotename(@changefeed_schema),
            '.',
            quotename(concat('read_feed:', @unquoted_qualified_table_name)))

    return concat('
create procedure ', @read_feed_proc, '(
    @shard int,
    @cursor binary(16),
    @pagesize int = 1000
)
as begin
    set xact_abort, nocount on;
    begin try
        if @@trancount > 0 throw 77100, ''Please call this procedure outside of any transaction'', 0;

        delete from #read;

        insert into #read
        select top(@pagesize) ULID, AggregateID, Sequence from ', @feed_table, '
        where ULID > @cursor;

        if @@rowcount <> 0
        begin
            return;
        end

        -- Read to the current end of the feed; check the Outbox. If we read something
        -- we put it into the log, so enter transaction.
        begin transaction

        declare @lockresult int;
        declare @lockname varchar(max) = concat(''changefeed/', @object_id, '/'', @shard)
        exec @lockresult = sp_getapplock @Resource = @lockname, @LockMode = ''Exclusive'', @LockOwner = ''Transaction'';
        if @lockresult = 1
        begin
            rollback
            -- 1 means "got lock after timeout". This means someone else fetched from the Outbox;
            -- so, we try again to read from the end of the log.
            exec ', @read_feed_proc, ' @shard = @shard, @cursor = @cursor, @pagesize = @pagesize;
            return
        end

        if @lockresult < 0
        begin
            throw 77100, ''Error getting lock'', 1;
        end

        -- @lockresult = 0; got lock without waiting. Consume outbox.

        declare @takenFromOutbox as table (
            AggregateID uniqueidentifier not null,
            Sequence int not null,
            Time datetime2(3) not null
        );

        delete top(@pagesize) from outbox
        output
           deleted.AggregateID, deleted.Sequence, deleted.Time into @takenFromOutbox
        from ', @outbox_table, ' as outbox
        where outbox.Shard = @shard;

        if @@rowcount = 0
        begin
            -- Nothing in Outbox either, simply return.
            rollback
            return
        end

        declare @min_time datetime2(3);
        declare @max_time datetime2(3);
        declare @count bigint;
        declare @random_bytes binary(10) = crypt_gen_random(10);
        select @min_time = min(Time), @max_time = max(Time), @count = count(*) from @takenFromOutbox;

        -- To assign ULIDs to the events in @takenFromOutbox, we split into two cases:
        -- 1) The ones where time is <= shard_state.time. For this, the Time is adjusted up to the shard_state.time.
        --    We suffix these variables _current_time.
        --
        -- 2) The ones where time is > shard_state.time. We suffix these _next_time.
        --    For these, for efficency and simplicity, we use the *same* random component,
        --    even if the time component varies within this set.
        --
        -- In the case that @max_time <= shard_state.time, we will only have the first case hitting;
        -- in this case we set all values equal to the same; and we also set @max_time to shard_state.time,
        -- so in that case, @current_time = max_time.

        declare @current_time datetime2(3);
        declare @ulid_high_current_time binary(8), @ulid_high_next_time binary(8)
        declare @ulid_low_current_time bigint, @ulid_low_next_time bigint

        update shard_state
        set
            @current_time = shard_state.time,
            @max_time = shard_state.time = let1.new_time,
            @ulid_high_current_time = shard_state.ulid_high,
            @ulid_low_current_time = shard_state.ulid_low,
            @ulid_high_next_time = shard_state.ulid_high = let2.new_ulid_high,
            @ulid_low_next_time = let2.ulid_low_range_start,
            -- add @count to the value stored in ulid_low; the ulid_low stored is the *first* value to be used
            -- by the next iteration
            shard_state.ulid_low = let2.ulid_low_range_start + @count
        from ', @changefeed_schema, '.shard_state shard_state with (updlock, serializable, rowlock)
        cross apply (select
            new_time = iif(@max_time > shard_state.time, @max_time, shard_state.time)
        ) let1
        cross apply (select
            new_ulid_high = (case
                when @max_time > shard_state.time then
                    -- New timestamp, so generate new ulid_high (the timestamp + 2 random bytes).
                    convert(binary(6), datediff_big(millisecond, ''1970-01-01 00:00:00'', new_time)) + substring(@random_bytes, 1, 2)
                else
                    shard_state.ulid_high
                end)
          , ulid_low_range_start = (case
              when @max_time > shard_state.time then
                  -- Start ulid_low in new, random place.
                  -- The mask 0xbfffffffffffffff will zero out bit 63, ensuring that overflows will not happen
                  -- as the caller adds numbers to this
                  convert(bigint, substring(@random_bytes, 3, 8)) & 0xbfffffffffffffff
              else
                  shard_state.ulid_low
              end)
        ) let2
        where
            shard_id = @shard;

        if @@rowcount = 0
        begin
            -- First time we read from this shard; upsert behaviour. @current_time is null for "minus infinity".
            -- @max_time keeps current value. Also leave @ulid_high_current_time and @ulid_low_current_time as null,
            -- since they will never be used.
            set @ulid_high_next_time = convert(binary(6), datediff_big(millisecond, ''1970-01-01 00:00:00'', @max_time)) + substring(@random_bytes, 1, 2);
            set @ulid_low_next_time = convert(bigint, substring(@random_bytes, 3, 8)) & 0xbfffffffffffffff;

            insert into ', @changefeed_schema, '.shard_state (shard_id, time, ulid_high, ulid_low)
            values (@shard, @max_time, @ulid_high_next_time, @ulid_low_next_time);
        end

        insert into myservice.EventFeed (Shard, ULID, AggregateID, Sequence)
        output inserted.ULID, inserted.AggregateID, inserted.Sequence into #read
        select
            @shard,
            let.ulid_high + convert(binary(8), let.ulid_low - 1 + row_number() over (order by taken.Time, taken.Sequence)),
            taken.AggregateID,
            taken.Sequence
        from @takenFromOutbox as taken
        cross apply (select
            ulid_high = iif(@current_time is null or taken.Time > @current_time, @ulid_high_next_time, @ulid_high_current_time),
            ulid_low = iif(@current_time is null or taken.Time > @current_time, @ulid_high_next_time, @ulid_low_current_time)
        ) let;

        commit

    end try
    begin catch
        if @@trancount > 0 rollback;
        throw;
    end catch

end
');
end

go

create procedure [changefeed].setup_feed(
    @table_name nvarchar(max)
)
as begin
    declare @object_id int = object_id(@table_name, 'U');
    if @object_id is null throw 71000, 'Could not find @table_name', 0;

    -- in order to be able to search/replace [changefeed] in this script, this is bit weird:
    declare @quoted_changefeed_schema nvarchar(max) = '[changefeed]';
    declare @changefeed_schema nvarchar(max) = substring(@quoted_changefeed_schema, 2, len(@quoted_changefeed_schema) - 2);

    declare @sql nvarchar(max) = [changefeed].gen_read_feed_sql(
        @object_id,
        @changefeed_schema);

    exec sp_executesql @sql
end
