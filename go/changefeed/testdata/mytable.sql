
create schema myservice;

go

create table myservice.MyTable (
    MyAggregateID bigint not null,
    Version int not null,
    Datapoint1 int not null,
    Datapoint2 varchar(max) not null,
    ULID binary(16) not null,
    primary key (MyAggregateID, Version)
);

create table myservice.MultiPK (
    x int not null,
    y uniqueidentifier not null,
    z varchar(10) not null,
    v varchar(max) not null,
    primary key (x, y, z)
)
