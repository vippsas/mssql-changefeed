
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

create function [changefeed].sql_unquoted_qualified_table_name(@object_id int)
returns nvarchar(max)
as begin
    return (select concat(s.name, N'.', o.name)
    from sys.objects as o
             join sys.schemas as s on o.schema_id = s.schema_id
    where o.object_id = @object_id);
end;


go

create function [changefeed].sql_outbox_table_name(
    @object_id int,
    @changefeed_schema nvarchar(max)
) returns nvarchar(max)
as begin
    return concat(
        quotename(@changefeed_schema),
        '.',
        quotename(concat('outbox:', [changefeed].sql_unquoted_qualified_table_name(@object_id))))
end

-- https://stackoverflow.com/questions/10977483/extended-type-name-function-that-includes-datalength

go

create function [changefeed].sql_fully_quoted_name(@object_id int)
returns nvarchar(max)
as begin
    return concat(
        quotename(object_schema_name(@object_id)),
        '.',
        quotename(object_name(@object_id)));
end

go

create function [changefeed].sql_primary_key_columns_joined_by_comma(
    @object_id int,
    @prefix nvarchar(max)  -- for instance, 'tablealias.'; to put this in front of every column
)
returns nvarchar(max)
as begin
    return (select
        string_agg(concat(@prefix, col.name), ', ') within group (order by col.column_id)
    from sys.key_constraints as pk
    join sys.index_columns as ic on
        ic.index_id = pk.unique_index_id
        and ic.object_id = pk.parent_object_id
    join sys.columns as col on
        col.object_id = pk.parent_object_id
        and col.column_id = ic.index_column_id
    where
        pk.parent_object_id = @object_id
        and pk.type = 'PK');
end

go

create function [changefeed].sql_primary_key_column_declarations(@object_id int, @prefix nvarchar(max))
returns nvarchar(max)
as begin
    -- returns a list of column declarations for the primary key of the given table;
    --     x int not null,
    --     y varchar(max) not null
    -- This requires us to construct a query and hand it to sys.dm_exec_describe_first_result_set
    declare @qry nvarchar(max) = concat(
        'select ',
        [changefeed].sql_primary_key_columns_joined_by_comma(@object_id, N''),
        ' from ',
        [changefeed].sql_fully_quoted_name(@object_id))
    return (
        select
            string_agg(
                concat(
                    @prefix, r.name, ' ', r.system_type_name,
                    iif(collation_name is not null, concat(' collate ', r.collation_name), ''),
                    ' not null'),
                concat(',', char(10)))
                within group (order by r.column_ordinal)
    from sys.dm_exec_describe_first_result_set(@qry, null, 0) r
    )
end

go

create function [changefeed].sql_create_feed_table(
    @object_id int,
    @changefeed_schema nvarchar(max)
) returns nvarchar(max)
as begin
    declare @table nvarchar(max) = concat(
        quotename(@changefeed_schema),
        '.',
        quotename(concat('feed:', [changefeed].sql_unquoted_qualified_table_name(@object_id))))
    declare @pkname nvarchar(max) = quotename(concat('pk:feed:', [changefeed].sql_unquoted_qualified_table_name(@object_id)))
    return concat('create table ', @table, '(
    shard_id int not null,
    ulid binary(16) not null,
', [changefeed].sql_primary_key_column_declarations(@object_id, '    '), ',
    constraint ', @pkname, ' primary key (shard_id, ulid)
) with (data_compression = page)');
end

go

create function [changefeed].sql_create_outbox_table(
    @object_id int,
    @shard_key_type nvarchar(max),
    @changefeed_schema nvarchar(max)
) returns nvarchar(max)
as begin
    declare @table nvarchar(max) = concat(
            quotename(@changefeed_schema),
            '.',
            quotename(concat('outbox:', [changefeed].sql_unquoted_qualified_table_name(@object_id))))
    declare @pkname nvarchar(max) = quotename(concat('pk:outbox:', [changefeed].sql_unquoted_qualified_table_name(@object_id)))
    return concat('create table ', @table, '(
    shard_id int not null,
    shard_key ', @shard_key_type, ' not null,
    ordering bigint not null,
    time_hint datetime2(3) not null,
', [changefeed].sql_primary_key_column_declarations(@object_id, '    '), ',
    constraint ', @pkname, ' primary key (shard_id, time_hint, shard_key, ordering, ', [changefeed].sql_primary_key_columns_joined_by_comma(@object_id, ''), ')
) with (data_compression = page)');
end

go

create function [changefeed].sql_create_read_type(
    @object_id int,
    @changefeed_schema nvarchar(max)
) returns nvarchar(max)
as begin
    declare @table nvarchar(max) = concat(
            quotename(@changefeed_schema),
            '.',
            quotename(concat('type:read:', [changefeed].sql_unquoted_qualified_table_name(@object_id))))
    return concat('create type ', @table, ' as table (
    ulid binary(16) not null,
', [changefeed].sql_primary_key_column_declarations(@object_id, '    '), '
)');
end

go

create function [changefeed].sql_create_read_procedure(
    @object_id int,
    @shard_key_type nvarchar(max),
    @changefeed_schema nvarchar(max)
)
returns nvarchar(max) as begin
    -- re-generate table name in a certain format from sys tables;
    -- @table_name can be quoted in different ways. We use unquoted, but qualified,
    -- name; e.g. [myschema.with.dot].[table.with.dot] will turn into
    -- myschema.with.dot.table.with.dot. In theory this can be ambigious, but assume
    -- noone will use names like this.
    declare @unquoted_qualified_table_name nvarchar(max) = [changefeed].sql_unquoted_qualified_table_name(@object_id)

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
    @shard_id int,
    @cursor binary(16),
    @pagesize int = 1000
)
as begin
    set xact_abort, nocount on;
    begin try
        if @@trancount > 0 throw 77100, ''Please call this procedure outside of any transaction'', 0;

        delete from #read;

        insert into #read(ulid, ', [changefeed].sql_primary_key_columns_joined_by_comma(@object_id, N''),')
        select top(@pagesize)
            ulid,
            ', [changefeed].sql_primary_key_columns_joined_by_comma(@object_id, N''), '
        from ', @feed_table, '
        where
            shard_id = @shard_id
            and ulid > @cursor;

        if @@rowcount <> 0
        begin
            return;
        end

        -- Read to the current end of the feed; check the Outbox. If we read something
        -- we put it into the log, so enter transaction.
        begin transaction

        declare @lockresult int;
        declare @lockname varchar(max) = concat(''changefeed/', @object_id, '/'', @shard_id)
        exec @lockresult = sp_getapplock @Resource = @lockname, @LockMode = ''Exclusive'', @LockOwner = ''Transaction'';
        if @lockresult = 1
        begin
            rollback
            -- 1 means "got lock after timeout". This means someone else fetched from the Outbox;
            -- so, we try again to read from the end of the log.
            exec ', @read_feed_proc, ' @shard_id = @shard_id, @cursor = @cursor, @pagesize = @pagesize;
            return
        end

        if @lockresult < 0
        begin
            throw 77100, ''Error getting lock'', 1;
        end

        -- @lockresult = 0; got lock without waiting. Consume outbox.

        declare @takenFromOutbox as table (
            time_hint datetime2(3) not null,
            shard_key ', @shard_key_type, ' not null,
            ordering bigint not null,
', [changefeed].sql_primary_key_column_declarations(@object_id, '            '), '

            -- benchmarks with 1000 rows indicate that things are not faster with primary key
            -- for some queries; but this can be re-visited more properly in the future
        );

        delete top(@pagesize) from outbox
        output
            deleted.time_hint, deleted.shard_key, deleted.ordering, ', [changefeed].sql_primary_key_columns_joined_by_comma(@object_id, N'deleted.'), '
        into @takenFromOutbox
            from ', @outbox_table, ' as outbox
        where outbox.shard_id = @shard_id;

        if @@rowcount = 0
        begin
            -- Nothing in Outbox either, simply return.
            rollback
            return
        end

        -- We want to support cases where time_hint is actually in a different order from event ordering
        -- caused by (shard_key, ordering); e.g., ordering=2 has lower timestamp than ordering=1 for the
        -- same shard_key. We can do this *locally* in the @takenFromOutbox chunk; *no* events will
        -- get a lower timestamp than the one in shard_state, before us -- and when we are done, @max_time
        -- will be written to shard_state. So: We simply change time_hint in a pre-processing step.
        -- Note: While this is an O(n^2) operation in principle, it seems fast enough; around 15-30 ms for
        -- 1000 rows in Azure SQL, and 0 to 15 ms for 100 rows.

        update t1
        set time_hint = x.max_time_hint
        from @takenFromOutbox t1
        cross apply (
            select max(time_hint) as max_time_hint from @takenFromOutbox t2
            where
                t1.shard_key = t2.shard_key  -- only search within same shard_key...
                and t2.ordering <= t1.ordering  -- search *everything* that has a smaller ordering; = is important to avoid null

                -- Note: At this point we *could* add short-circuit `and t2.time_hint > t1.time_hint`,
                -- but benchmarks says this is actually slower due to higher query complexity.
        ) x;

        -- From here on we know that time_hint is ordered the same as `ordering`, within each shard_key.

        declare @max_time datetime2(3);
        declare @count bigint;
        declare @random_bytes binary(10) = crypt_gen_random(10);
        select @max_time = max(time_hint), @count = count(*) from @takenFromOutbox;

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
            shard_id = @shard_id;

        if @@rowcount = 0
        begin
            -- First time we read from this shard; upsert behaviour. @current_time is null for "minus infinity".
            -- @max_time keeps current value. Also leave @ulid_high_current_time and @ulid_low_current_time as null,
            -- since they will never be used.
            set @ulid_high_next_time = convert(binary(6), datediff_big(millisecond, ''1970-01-01 00:00:00'', @max_time)) + substring(@random_bytes, 1, 2);
            set @ulid_low_next_time = convert(bigint, substring(@random_bytes, 3, 8)) & 0xbfffffffffffffff;

            insert into ', @changefeed_schema, '.shard_state (shard_id, time, ulid_high, ulid_low)
            values (@shard_id, @max_time, @ulid_high_next_time, @ulid_low_next_time);
        end

        insert into ', @feed_table, '(shard_id, ulid, ', [changefeed].sql_primary_key_columns_joined_by_comma(@object_id, '') , ')
        output inserted.ulid, ', [changefeed].sql_primary_key_columns_joined_by_comma(@object_id, 'inserted.'), ' into #read(ulid, ', [changefeed].sql_primary_key_columns_joined_by_comma(@object_id, N''), ')
        select
            @shard_id,
            let.ulid_high + convert(binary(8), let.ulid_low - 1 + row_number() over (order by taken.time_hint, taken.ordering, ', [changefeed].sql_primary_key_columns_joined_by_comma(@object_id, 'taken.'), ')),
            ', [changefeed].sql_primary_key_columns_joined_by_comma(@object_id, 'taken.'), '
        from @takenFromOutbox as taken
        cross apply (select
            ulid_high = iif(@current_time is null or taken.time_hint > @current_time, @ulid_high_next_time, @ulid_high_current_time),
            ulid_low = iif(@current_time is null or taken.time_hint > @current_time, @ulid_high_next_time, @ulid_low_current_time)
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
    @table_name nvarchar(max),
    @shard_key_type nvarchar(max) = N'bigint'
)
as begin
    declare @object_id int = object_id(@table_name, 'U');
    if @object_id is null throw 71000, 'Could not find @table_name', 0;

    -- in order to be able to search/replace [changefeed] in this script, this is bit weird:
    declare @quoted_changefeed_schema nvarchar(max) = '[changefeed]';
    declare @changefeed_schema nvarchar(max) = substring(@quoted_changefeed_schema, 2, len(@quoted_changefeed_schema) - 2);

    declare @sql nvarchar(max);


    -- create [feed:<tablename>]
    set @sql = [changefeed].sql_create_feed_table(
            @object_id,
            @changefeed_schema);

    exec sp_executesql @sql;

    -- create [outbox:read:<tablename>]
    set @sql = [changefeed].sql_create_outbox_table(
            @object_id,
        @shard_key_type,
            @changefeed_schema);
    exec sp_executesql @sql;

    -- create [type:read:<tablename>]
    set @sql = [changefeed].sql_create_read_type(
            @object_id,
            @changefeed_schema);

    exec sp_executesql @sql;

    -- create [read_feed:<tablename>]
    set @sql = [changefeed].sql_create_read_procedure(
        @object_id,
        @shard_key_type,
        @changefeed_schema);

    exec sp_executesql @sql;
end
