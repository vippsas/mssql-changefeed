
create schema myservice;

go
create user myuser with password = 'UserPw1234';
grant select, insert on schema::myservice to myuser;

create user myreaduser with password = 'UserPw1234';
grant select, insert on schema::myservice to myreaduser;

go

create table myservice.TestHappyDay (
    AggregateID bigint not null,
    Version int not null,
    Data varchar(max) not null,
    primary key (AggregateID, Version)
);

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
);

create table myservice.MultiPK2 (
    x int not null,
    y uniqueidentifier not null,
    z varchar(10) not null,
    v varchar(max) not null,
    primary key (x, y, z)
);

create table myservice.TestSerializeWriters (
    EventID binary(16) primary key,
    Data varchar(max) not null,
);

