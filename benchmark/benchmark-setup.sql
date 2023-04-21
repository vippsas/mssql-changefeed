create schema benchmark;
go

create table benchmark.Event(
    AggregateID uniqueidentifer not null,
    Version int not null,
    JsonData varchar(max) not null,
    Shard int not null,
    EventID binary(16) not null,
    primary key (AggregateID, Version)
);

create unique index EventID on benchmark.Event(Shard, EventID);
