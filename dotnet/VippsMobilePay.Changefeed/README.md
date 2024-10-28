
# VippsMobilePay.Changefeed.Sql.DbUp

A NuGet package that provides SQL scripts for use with `mssql-changefeed` in SQL Server.

For more information on `mssql-changefeed` and its purpose, please visit the GitHub project here: [mssql-changefeed GitHub Repository](https://github.com/vippsas/mssql-changefeed).

## Changelog

Please refer to the [CHANGELOG.md](./CHANGELOG.md) file for a summary of changes and updates.

## Usage

This package is intended for projects that already use [DbUp](https://dbup.github.io/). To integrate the `mssql-changefeed` SQL scripts, add the `AddChangefeedMigrationSource` method to your `UpgradeEngineBuilder` setup as shown in the example below:

```csharp
var upgradeEngine = DeployChanges.To.SqlDatabase(_connectionString)
    .AddChangefeedMigrationSource()
    .WithScriptsEmbeddedInAssembly(typeof(IDbUpAssemblyMarker).Assembly)
    .Build();

var upgradeResult = upgradeEngine.PerformUpgrade();
```

### Important Notes

- This configuration only prepares your database for the required changefeed tables. It does **not** create any tables by itself.
- Each database requires a custom setup for changefeed functionality, including executing a stored procedure provided by `mssql-changefeed`.
- Ensure that `AddChangefeedMigrationSource` appears before `WithScriptsEmbeddedInAssembly` in your chain of commands, as this order is essential for proper execution.

To create the `mssql-changefeed` tables, add the setup script alongside other SQL scripts in your DbUp project. Here’s an example script to initialize changefeed tables:

```sql
DECLARE @RC int
DECLARE @table_name nvarchar(max) = 'EventSource'
DECLARE @outbox bit = 1
DECLARE @blocking bit = 0

EXECUTE @RC = [changefeed].[setup_feed] 
   @table_name
  ,@outbox
  ,@blocking
GO
```
