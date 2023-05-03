
create schema myservice;

go

create table myservice.MyTable (
    id bigint not null primary key,
    value varchar(max)
);
