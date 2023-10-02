drop table if exists myservice.Event
drop table if exists myservice.EventFeed
drop table if exists myservice.EventOutbox
drop table if exists myservice.ShardState;
drop schema if exists myservice;
go

create schema myservice;


go

create table myservice.Event(
    AggregateID uniqueidentifier not null,
    Sequence int not null,
    Time datetime2(3) not null,
    Shard int not null,
    JsonData varchar(max) not null,

    primary key (AggregateID, Sequence)
);

create table myservice.EventOutbox (
    Shard int not null,
    Time datetime2(3) not null,
    AggregateID uniqueidentifier not null,
    Sequence int not null,

    primary key (Shard, Time, Sequence, AggregateID)
);

create table myservice.EventFeed(
    Shard int not null,
    ULID binary(16) not null,
    AggregateID uniqueidentifier not null,
    Sequence int not null,
    primary key (Shard, ULID)
);

create unique index Shard_ULID on myservice.EventFeed(Shard, ULID);


go

create trigger mytrig on myservice.Event
after insert
as begin
    set nocount on;

    insert into myservice.EventOutbox (Shard, Time, AggregateID, Sequence)
    select Shard, Time, AggregateID, Sequence
    from inserted;

end
