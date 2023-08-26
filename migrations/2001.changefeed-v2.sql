
create schema [changefeed];

go

create or alter function [changefeed].sql_unquoted_qualified_table_name(@object_id int)
returns nvarchar(max)
as begin
    return (select concat(s.name, N'.', o.name)
    from sys.objects as o
             join sys.schemas as s on o.schema_id = s.schema_id
    where o.object_id = @object_id);
end;


go

create or alter function [changefeed].sql_outbox_table_name(
    @object_id int,
    @changefeed_schema nvarchar(max)
) returns nvarchar(max)
as begin
    return concat(
        quotename(@changefeed_schema),
        '.',
        quotename(concat('outbox:', [changefeed].sql_unquoted_qualified_table_name(@object_id))))
end

go

create or alter function [changefeed].sql_fully_quoted_name(@object_id int)
returns nvarchar(max)
as begin
    return concat(
        quotename(object_schema_name(@object_id)),
        '.',
        quotename(object_name(@object_id)));
end

go

create or alter function [changefeed].sql_primary_key_columns_joined_by_comma(
    @object_id int,
    @prefix nvarchar(max)  -- for instance, 'tablealias.'; to put this in front of every column
)
returns nvarchar(max)
as begin
    return (select string_agg(concat(@prefix, col.name), ', ') within group ( order by col.name)
            from sys.tables tab
            inner join sys.indexes pk on tab.object_id = pk.object_id
            inner join sys.index_columns ic on ic.object_id = pk.object_id and ic.index_id = pk.index_id
            inner join sys.columns col on pk.object_id = col.object_id and col.column_id = ic.column_id
            where
                pk.object_id = @object_id and pk.is_primary_key = 1
            );
end

go

create or alter function [changefeed].sql_primary_key_column_declarations(@object_id int, @prefix nvarchar(max))
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

create or alter function [changefeed].sql_create_state_table(
    @object_id int,
    @changefeed_schema nvarchar(max)
) returns nvarchar(max)
as begin
    declare @table nvarchar(max) = concat(
            quotename(@changefeed_schema),
            '.',
            quotename(concat('state:', [changefeed].sql_unquoted_qualified_table_name(@object_id))))
    declare @pkname nvarchar(max) = quotename(concat('pk:state:', [changefeed].sql_unquoted_qualified_table_name(@object_id)))
    return concat('create table ', @table, '(
    shard_id int not null,

    time datetime2(3) not null,

    -- See EXPERTS-GUIDE.md for description of ulid_prefix
    -- and ulid_low.
    ulid_high binary(8) not null,
    ulid_low bigint not null,

    -- For convenience, the ulid is displayable directly. Also serves as documentation:
    ulid as ulid_high + convert(binary(8), ulid_low),

    constraint ', @pkname, ' primary key (shard_id)
);

alter table ', @table, ' set (lock_escalation = disable);
')

end

go

create or alter function [changefeed].sql_create_feed_table(
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

create or alter function [changefeed].sql_create_outbox_table(
    @object_id int,
    @changefeed_schema nvarchar(max)
) returns nvarchar(max)
as begin
    declare @table nvarchar(max) = concat(
            quotename(@changefeed_schema),
            '.',
            quotename(concat('outbox:', [changefeed].sql_unquoted_qualified_table_name(@object_id))))
    declare @sequence nvarchar(max) = concat(
            quotename(@changefeed_schema),
            '.',
            quotename(concat('sequence:', [changefeed].sql_unquoted_qualified_table_name(@object_id))))

    declare @pkname nvarchar(max) = quotename(concat('pk:outbox:', [changefeed].sql_unquoted_qualified_table_name(@object_id)))
    declare @seq_constraint_name nvarchar(max) = quotename(concat('def:outbox.order_sequence:', [changefeed].sql_unquoted_qualified_table_name(@object_id)))
    return concat('
create sequence ', @sequence,' as bigint start with 1 increment by 1 cache 100000;

create table ', @table, '(
    shard_id int not null,
    order_sequence bigint constraint ', @seq_constraint_name,' default (next value for ', @sequence, '),
    time_hint datetime2(3) not null,
', [changefeed].sql_primary_key_column_declarations(@object_id, '    '), ',
    constraint ', @pkname, ' primary key (shard_id, order_sequence)
) with (data_compression = page);
');
end

go

create or alter function [changefeed].sql_create_read_type(
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

create or alter function [changefeed].sql_create_feed_write_lock_procedure(
    @object_id int,
    @changefeed_schema nvarchar(max)
) returns nvarchar(max) as begin
    declare @unquoted_qualified_table_name nvarchar(max) = [changefeed].sql_unquoted_qualified_table_name(@object_id)

    declare @lock_proc nvarchar(max) = concat(
            quotename(@changefeed_schema),
            '.',
            quotename(concat('feed_write_lock:', @unquoted_qualified_table_name)))

    return concat('
-- feed_write_lock:* takes a lock, owned by the Transaction, indicating that
-- the feed will be written to. One should *also* do update_shard_state which
-- takes its own lock implicitly, but read_feed:* needs to also have a pre-lock
-- before updating the shard state (in order to know what timestamps are in the outbox)
create or alter procedure ', @lock_proc, '(
    @shard_id int,
    @lock_timeout int = -1,  -- same as sp_getallock
    @lock_result int output
) as begin
    declare @lockname varchar(max) = concat(''changefeed/', @object_id, '/'', @shard_id)
    exec @lock_result = sp_getapplock
        @Resource = @lockname,
        @LockMode = ''Exclusive'',
        @LockOwner = ''Transaction'',
        @LockTimeout = @lock_timeout;
end;
')
end;

go

create or alter function [changefeed].sql_create_lock_procedure(
    @object_id int,
    @changefeed_schema nvarchar(max)
) returns nvarchar(max) as begin
    declare @unquoted_qualified_table_name nvarchar(max) = [changefeed].sql_unquoted_qualified_table_name(@object_id)

    declare @state_table nvarchar(max) = concat(
            quotename(@changefeed_schema),
            '.',
            quotename(concat('state:', @unquoted_qualified_table_name)))

    declare @lock_proc nvarchar(max) = concat(
            quotename(@changefeed_schema),
            '.',
            quotename(concat('lock:', @unquoted_qualified_table_name)))

    return concat('
create or alter procedure ', @lock_proc, '(
    -- Default is to allocate a very large @count; the next lock within the same millisecond will start
    -- after this number added to the one we allocate first. We assume 2^40 ULIDs generated in each
    -- transaction (including waste due to concurrent use of changefeed.sequence), leaving room for
    -- 2^23 transactions to execute on the same shard in the same millisecond...
    @count bigint = 1099511627776,

) as begin


end;
')
end;

go

create or alter function [changefeed].sql_create_update_state_procedure(
    @object_id int,
    @changefeed_schema nvarchar(max)
) returns nvarchar(max) as begin
    declare @unquoted_qualified_table_name nvarchar(max) = [changefeed].sql_unquoted_qualified_table_name(@object_id)

    declare @state_table nvarchar(max) = concat(
            quotename(@changefeed_schema),
            '.',
            quotename(concat('state:', @unquoted_qualified_table_name)))

    declare @update_state_proc nvarchar(max) = concat(
            quotename(@changefeed_schema),
            '.',
            quotename(concat('update_state:', @unquoted_qualified_table_name)))

    return concat('
create or alter procedure ', @update_state_proc, '(
    @shard_id int,
    @time_hint datetime2(3),
    @count bigint,

    @previous_time datetime2(3) = null output,
    @previous_ulid_high binary(8) = null output,
    @previous_ulid_low bigint = null output,

    @next_time datetime2(3) = null output,
    @next_ulid_high binary(8) = null output,
    @next_ulid_low bigint = null output
)
as begin
    set xact_abort, nocount on;
    begin try
        if @@trancount = 0 throw 77100, ''Please call this procedure inside a transaction'', 0;

        declare @random_bytes binary(10) = crypt_gen_random(10);

        update shard_state
        set
            @previous_time = shard_state.time,
            @next_time = shard_state.time = let1.new_time,
            @previous_ulid_high = shard_state.ulid_high,
            @previous_ulid_low = shard_state.ulid_low,
            @next_ulid_high = shard_state.ulid_high = let2.new_ulid_high,
            @next_ulid_low = let2.ulid_low_range_start,
            -- add @count to the value stored in ulid_low; the ulid_low stored is the *first* value to be used
            -- by the next iteration
            shard_state.ulid_low = let2.ulid_low_range_start + @count
        from ', @state_table, ' shard_state with (updlock, serializable, rowlock)
        cross apply (select
            new_time = iif(@time_hint > shard_state.time, @time_hint, shard_state.time)
        ) let1
        cross apply (select
            new_ulid_high = (case
                when @time_hint > shard_state.time then
                    -- New timestamp, so generate new ulid_high (the timestamp + 2 random bytes).
                    convert(binary(6), datediff_big(millisecond, ''1970-01-01 00:00:00'', new_time)) + substring(@random_bytes, 1, 2)
                else
                    shard_state.ulid_high
                end)
          , ulid_low_range_start = (case
              when @time_hint > shard_state.time then
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
            -- First time we read from this shard; upsert behaviour.
            --
            -- Leave @previous_X to null since there wasn''t anything previously
            set @next_time = @time_hint;
            set @next_ulid_high = convert(binary(6), datediff_big(millisecond, ''1970-01-01 00:00:00'', @time_hint)) + substring(@random_bytes, 1, 2);
            set @next_ulid_low = convert(bigint, substring(@random_bytes, 3, 8)) & 0xbfffffffffffffff;;

            insert into ', @state_table, ' (shard_id, time, ulid_high, ulid_low)
            values (@shard_id, @next_time, @next_ulid_high, @next_ulid_low);
        end

    end try
    begin catch
        if @@trancount > 0 rollback;
        throw;
    end catch
end
');

end

go

create or alter function [changefeed].sql_create_read_procedure(
    @object_id int,
    @changefeed_schema nvarchar(max)
)
returns nvarchar(max) as begin
    -- re-generate table name in a certain format from sys tables;
    -- @table_name can be quoted in different ways. We use unquoted, but qualified,
    -- name; e.g. [myschema.with.dot].[table.with.dot] will turn into
    -- myschema.with.dot.table.with.dot. In theory this can be ambigious, but assume
    -- noone will use names like this.
    declare @unquoted_qualified_table_name nvarchar(max) = [changefeed].sql_unquoted_qualified_table_name(@object_id)

    declare @state_table nvarchar(max) = concat(
            quotename(@changefeed_schema),
            '.',
            quotename(concat('state:', @unquoted_qualified_table_name)))

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

    declare @rawlock_proc nvarchar(max) = concat(
            quotename(@changefeed_schema),
            '.',
            quotename(concat('rawlock:', @unquoted_qualified_table_name)))

    declare @feed_write_lock_proc nvarchar(max) = concat(
            quotename(@changefeed_schema),
            '.',
            quotename(concat('feed_write_lock:', @unquoted_qualified_table_name)))

    declare @update_state_proc nvarchar(max) = concat(
            quotename(@changefeed_schema),
            '.',
            quotename(concat('update_state:', @unquoted_qualified_table_name)))

    declare @pklist nvarchar(max) = [changefeed].sql_primary_key_columns_joined_by_comma(@object_id, N'');

    return concat('
create or alter procedure ', @read_feed_proc, '(
    @shard_id int,
    @cursor binary(16),
    @pagesize int = 1000
)
as begin
    set xact_abort, nocount on;
    begin try
        if @@trancount > 0 throw 77100, ''Please call this procedure outside of any transaction'', 0;

        delete from #read;

        insert into #read(ulid, ', @pklist,')
        select top(@pagesize)
            ulid,
            ', @pklist, '
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

        -- Use an application lock to make sure only one session will
        -- process the outbox at the time. However, the shard state itself
        -- is really protected by the `update` statement in rawlock:, not
        -- this lock. I.e. this lock *only* protects consumption of the outbox
        -- and that those using read_feed sees a consistent picture.

        declare @lock_result int;
        exec ', @feed_write_lock_proc, '
            @shard_id = @shard_id,
            @lock_timeout = -1,
            @lock_result = @lock_result output;
        if @lock_result = 1
        begin
            rollback
            -- 1 means "got lock after timeout". This means someone else has processed the outbox;
            -- so we try again once. If there are no results here, it just means the outbox was empty
            -- just now anyway .. the caller should sleep a bit before retrying.

            insert into #read(ulid, ', @pklist,')
            select top(@pagesize)
                ulid,
                ', @pklist, '
            from ', @feed_table, '
            where
                shard_id = @shard_id
                and ulid > @cursor;
            return
        end;

        if @lock_result < 0
        begin
            throw 77100, ''Error getting lock'', 1;
        end;

        -- @lock_result = 0; got lock without waiting. Consume outbox.

        declare @takenFromOutbox as table (
            order_sequence bigint not null primary key,
            time_hint datetime2(3) not null,
', [changefeed].sql_primary_key_column_declarations(@object_id, '            '), '

            -- benchmarks with 1000 rows indicate that things are not faster with primary key
            -- for some queries; but this can be re-visited more properly in the future
        );

        with totake as (
            select top(@pagesize) * from ', @outbox_table, ' as outbox
            order by outbox.order_sequence
        )
        delete top(@pagesize) from totake
        output
            deleted.order_sequence, deleted.time_hint, ', [changefeed].sql_primary_key_columns_joined_by_comma(@object_id, N'deleted.'), '
        into @takenFromOutbox;

        if @@rowcount = 0
        begin
            -- Nothing in Outbox either, simply return.
            rollback
            return
        end;

        -- order_sequence is what we use for main event ordering; this is a mechanism to ensure that
        -- ordering can be deterministic in cases where it matters for the application (e.g., between events in
        -- the same aggregate/for the same entity). See the experts guide for details.
        --
        -- So, we do not want to require the application to also keep track of time_hint, we want to
        -- support that having a different order, and we simply fix it up here.
        with patched_time as (
            select
                order_sequence,
                time_hint = max(time_hint) over (order by order_sequence rows between unbounded preceding and current row)
            from @takenFromOutbox
        )
        update t
        set time_hint = patched_time.time_hint
        from @takenFromOutbox as t
        join patched_time on patched_time.order_sequence = t.order_sequence;

        declare @max_time datetime2(3);
        declare @count bigint;
        declare @random_bytes binary(10) = crypt_gen_random(10);
        select @max_time = max(time_hint), @count = count(*) from @takenFromOutbox;

        -- To assign ULIDs to the events in @takenFromOutbox, we split into two cases:
        -- 1) The ones where time is <= shard_state.time. For this, adjust up to the shard_state.time
        --
        -- 2) The ones where time is > shard_state.time.
        --    For these, for efficency and simplicity, we use the *same* random component,
        --    even if the time component varies within this set.
        --
        -- In the case that @max_time <= shard_state.time, we will only have the first case hitting;
        -- in this case we set all values equal to the same.
        declare @previous_time datetime2(3);
        declare @previous_ulid_high binary(8);
        declare @previous_ulid_low bigint;

        declare @next_time datetime2(3);
        declare @next_ulid_high binary(8);
        declare @next_ulid_low bigint;

        exec ', @update_state_proc, '
            @shard_id = @shard_id,
            @time_hint = @max_time,
            @count = @count,
            @previous_time = @previous_time output,
            @previous_ulid_high = @previous_ulid_high output,
            @previous_ulid_low = @previous_ulid_low output,
            @next_ulid_high = @next_ulid_high output,
            @next_ulid_low = @next_ulid_low output;

        insert into ', @feed_table, '(shard_id, ulid, ', @pklist , ')
        output inserted.ulid, ', [changefeed].sql_primary_key_columns_joined_by_comma(@object_id, 'inserted.'), ' into #read(ulid, ', @pklist, ')
        select
            @shard_id,
            let.ulid_high + convert(binary(8), let.ulid_low - 1 + row_number() over (order by taken.order_sequence)),
            ', [changefeed].sql_primary_key_columns_joined_by_comma(@object_id, 'taken.'), '
        from @takenFromOutbox as taken
        cross apply (select
            ulid_high = iif(
                -- embed max(time_hint, @previous_time) in ulid_high
                @previous_time is null or taken.time_hint > @previous_time,
                convert(binary(6), datediff_big(millisecond, ''1970-01-01 00:00:00'', taken.time_hint)),
                @previous_ulid_high),
            ulid_low = iif(
                -- use ulid_low matching the cases above
                @previous_time is null or taken.time_hint > @previous_time,
                @next_ulid_low,
                @previous_ulid_low)
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

-- upgrade_feed is called if setup_feed has earlier been called to upgrade to a new version.
-- right now this only supports to re-run all stored procedures as `create or alter`, allowing
-- code updates in the stored procedures without affecting the tables created
create or alter procedure [changefeed].upgrade_feed(
    @table_name nvarchar(max)
)
as begin
    declare @object_id int = object_id(@table_name, 'U');
    if @object_id is null throw 71000, 'Could not find @table_name', 0;

    -- in order to be able to search/replace [changefeed] in this script, this is bit weird:
    declare @quoted_changefeed_schema nvarchar(max) = '[changefeed]';
    declare @changefeed_schema nvarchar(max) = substring(@quoted_changefeed_schema, 2, len(@quoted_changefeed_schema) - 2);

    declare @sql nvarchar(max);

    -- create [feed_write_lock:<tablename>]
    set @sql = [changefeed].sql_create_feed_write_lock_procedure(
            @object_id,
            @changefeed_schema);
    exec sp_executesql @sql;

    -- create [update_state:<tablename>]
    set @sql = [changefeed].sql_create_update_state_procedure(
            @object_id,
            @changefeed_schema);
    exec sp_executesql @sql;

    -- create [read_feed:<tablename>]
    set @sql = [changefeed].sql_create_read_procedure(
            @object_id,
            @changefeed_schema);
    exec sp_executesql @sql;
end

go

create or alter procedure [changefeed].setup_feed(
    @table_name nvarchar(max)
)
as begin
    declare @object_id int = object_id(@table_name, 'U');
    if @object_id is null throw 71000, 'Could not find @table_name', 0;

    -- in order to be able to search/replace [changefeed] in this script, this is bit weird:
    declare @quoted_changefeed_schema nvarchar(max) = '[changefeed]';
    declare @changefeed_schema nvarchar(max) = substring(@quoted_changefeed_schema, 2, len(@quoted_changefeed_schema) - 2);

    declare @sql nvarchar(max);

    -- create [state:<tablename>]
    set @sql = [changefeed].sql_create_state_table(
            @object_id,
            @changefeed_schema);
    exec sp_executesql @sql;

    -- create [feed:<tablename>]
    set @sql = [changefeed].sql_create_feed_table(
            @object_id,
            @changefeed_schema);
    exec sp_executesql @sql;

    -- create [outbox:read:<tablename>]
    set @sql = [changefeed].sql_create_outbox_table(
            @object_id,
            @changefeed_schema);
    exec sp_executesql @sql;

    -- create [type:read:<tablename>]
    set @sql = [changefeed].sql_create_read_type(
            @object_id,
            @changefeed_schema);
    exec sp_executesql @sql;

    -- Stored procedures done by upgrade_feed...
    exec [changefeed].upgrade_feed @table_name;
end


go

create or alter function [changefeed].ulid(@sequence_sample bigint) returns binary(16)
as begin
    return convert(binary(8), session_context(N'changefeed.ulid_high'))
        + convert(binary(8),
                       convert(bigint, session_context(N'changefeed.ulid_low'))
                       + (@sequence_sample - convert(bigint, session_context(N'changefeed.sequence_start')))
               );
end

