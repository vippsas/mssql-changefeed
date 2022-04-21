# Installation

## About version numbers

This library is shipped as SQL migrations. Therefore the library is versioned
after the number of migrations; not after implied stability, backwards
compatability etc.

## Installation instructions
Deploy the SQL migration in `migrations/from0001/0001.changefeed.sql` using your
SQL migration pipeline. It assumes that `go` is available to denote
batch split; adjust if needed. The migration will create and populate the `changefeed` schema.

The main implementation is in SQL and using Go is **NOT** needed for using this
library; but the tests are written in Go, and also there is a small reference
implementation of the client side longpolling in Go. This is easily ported
to your favorite language.

In addition to running the migration, you may need to grant permissions
if your service does not run with all privileges:
```sql
-- Change producers:
grant select, insert on schema::changefeed to yourservice;
grant update on changefeed.change_id to yourservice;

-- Change consumers:
grant select on schema::changefeed to yourservice;

-- Sweeper loop:
grant execute on changefeed.sweep_loop to yourservice;

-- Longpolling -- this is a bit more involved; in the future we may package
-- this up more nicely.
grant execute on changefeed.longpoll to yourservice;
grant execute on changefeed.longpoll_guard to yourservice;

if ServerProperty('Edition') = 'SQL Azure'
begin
  alter role ##MS_ServerStateReader## add member yourservice;
end
else
begin
    declare @revert_use nvarchar(max) = concat('use [', db_name(), ']');
    use master;
    grant view server state to yourservicelogin;
    exec sp_executesql @revert_use;
end
```
