drop table if exists benchmark.Event
drop table if exists benchmark.EventOutbox
drop table if exists benchmark.ShardState;
drop schema if exists benchmark;

go

create schema benchmark;
go

create table benchmark.ShardState(
    shard_id int not null,
    time datetime2(3) not null
        constraint def_shard_ulid_time default '1970-01-01',

    -- See EXPERTS-GUIDE.md for description of ulid_prefix
    -- and ulid_low.
    ulid_high binary(8) not null
        constraint def_shard_state_ulid_ulid_prefix default 0x0,
    ulid_low bigint not null
        constraint def_shard_state_ulid_ulid_low default 0,
    -- For convenience, the ulid is displayable directly. Also serves as documentation:
    ulid as ulid_high + convert(binary(8), ulid_low),
    constraint pk_shard_ulid primary key (shard_id)
);


go

create table benchmark.Event(
    --AggregateID uniqueidentifier not null,
    --Version int not null,
    RowID bigint identity(1,1) not null primary key,
    Time datetime2(3) not null,
    Shard int not null,
    JsonData varchar(max) not null
);

create table benchmark.EventULID (
    Shard int not null,
    ULID binary(16) not null,
    RowID bigint not null,
)

create unique index ULID on benchmark.EventULID(Shard, ULID);

create table benchmark.EventOutbox (
    RowID bigint not null,
    Shard int not null,
    Time datetime2(3) not null,
    OrderNumber bigint not null default 0,

    primary key (Shard, Time, OrderNumber, RowID)
);

go

create trigger mytrig on benchmark.Event
after insert
as begin
    set nocount on;

    insert into benchmark.EventOutbox
    select RowID, Shard, Time, RowID
    from inserted;

end

go

create procedure benchmark.get(
    @shard int,
    @cursor bigint)
as begin

    begin try

        declare @result as table (
            RowID bigint not null
        );

        insert into @result
        select RowID from benchmark.EventULID
        where ULID > @cursor;

        if @@rowcount <> 0
        begin
            select * from @result;
            return;
        end

        -- Read to the current end of the feed; check the Outbox. If we read something
        -- we put it into the log, so enter transaction.
        begin transaction
            declare @lockname varchar(max) = concat('changefeed/benchmark/', @shard)
            exec @result = sp_getapplock @Resource = @lockname, @LockMode = 'Exclusive', @LockOwner = 'Transaction';
            if @result = 1
            begin
                -- 1 means "got lock after timeout". This means someone else fetched from the Outbox;
                -- so, we try again to read from the end of the log.
                exec benchmark.get @shard = @shard, @cursor = @cursor;
                return
            end

            if @result < 0
            begin
                throw 77100, 'Error getting lock', 1;
            end

            -- @result = 0; got lock without waiting. Consume outbox.

            declare @takenFromOutbox as table (
                RowID bigint not null,
                Time datetime2(3) not null,
                OrderNumber bigint not null default 0,
            );

            delete top(1000) from outbox
            output
                deleted.RowID, deleted.Time, deleted.OrderNumber into @takenFromOutbox
            from benchmark.EventOutbox as outbox
            where outbox.Shard = @shard;

            declare @max_time datetime2(3);
            declare @min_time datetime2(3);
            declare @random_bytes binary(10) = crypt_gen_random(10);

            select @min_time = min(Time), @max_time = max(Time) from @takenFromOutbox;

            update benchmark.ShardState
            set
                ulid_high = convert(binary(6), datediff_big(millisecond, '1970-01-01 00:00:00', @max_time)) + substring(@random_bytes, 1, 2),
                -- Start low in new place.
                -- The mask 0xbfffffffffffffff will zero out bit 63, ensuring that overflows will not happen
                -- as the caller adds numbers to this
                ulid_low = convert(bigint, substring(@random_bytes, 3, 8)) & 0xbfffffffffffffff

            where shard_id = @shard;

                shard_id int not null,
                time datetime2(3) not null
                constraint def_shard_ulid_time default '1970-01-01',

                -- See EXPERTS-GUIDE.md for description of ulid_prefix
                -- and ulid_low.
                ulid_high binary(8) not null
                constraint def_shard_state_ulid_ulid_prefix default 0x0,
                ulid_low bigint not null
                constraint def_shard_state_ulid_ulid_low default 0,
                -- For convenience, the ulid is displayable directly. Also serves as documentation:
                ulid as ulid_high + convert(binary(8), ulid_low),
                constraint pk_shard_ulid primary key (shard_id)


    end try
    begin catch
        if @@trancount > 0 rollback;

    end catch

end
