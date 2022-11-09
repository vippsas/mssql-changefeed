# Changefeed SQL library for SQL Server

This is a pure-SQL library you can use to implement "change feeds" in your service;
e.g. when you want to publish events from your service after something has committed,
or provide REST API of all your changes, or you want a pattern for internal workers
to react to changes ("async triggers").

Compared to other solutions, `mssql-changefeed` provides:

* Race-safety
* Low latency
* High flexibility, one solution is a building block to solve many different problems

On the other hand, it is not built into SQL Server, and you need to install and
use this library. You also need to set up a worker process that continually
"sweeps" for committed changes that have not yet been put into a feed
(but doing so in a very robust manner is easy with this library).

## Table of contents

* [MOTIVATION.md - description of problem and review of competing solutions](MOTIVATION.md)
* [USAGE.md - Usage instructions](USAGE.md)
* [INSTALL.md - Installation instructions](INSTALL.md)
* [CHANGELOG.md](CHANGELOG.md)