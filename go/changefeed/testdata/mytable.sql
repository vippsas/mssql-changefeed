
create schema myservice;

go

create table myservice.MyTable (
    id bigint not null primary key,
    value varchar(max)
);

create table myservice.MultiPK (
    x int not null,
    y uniqueidentifier not null,
    z varchar(10) not null,
    v varchar(max) not null,
    primary key (x, y, z)
)
