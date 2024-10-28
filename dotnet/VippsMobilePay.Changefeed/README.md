
# VippsMobilePay.Changefeed.Sql.DbUp

A NuGet package that provides SQL scripts for use with `mssql-changefeed` in SQL Server.

For more information on `mssql-changefeed` and its purpose, please visit the GitHub project here: [mssql-changefeed GitHub Repository](https://github.com/vippsas/mssql-changefeed).

## Changelog

Please refer to the [CHANGELOG.md](./CHANGELOG.md) file for a summary of changes and updates.

## Usage

This package is intended for projects that already use [DbUp](https://dbup.github.io/). To integrate the `mssql-changefeed` SQL scripts, add the `AddChangefeedMigrationSource` method to your `UpgradeEngineBuilder` setup as shown in the example below:

```csharp
var upgradeEngine = DeployChanges.To.SqlDatabase(_connectionString)
    .AddChangefeedMigrationSource(tableName: "EventSource", outbox: true, blocking: false)
    .WithScriptsEmbeddedInAssembly(typeof(IDbUpAssemblyMarker).Assembly)
    .Build();

var upgradeResult = upgradeEngine.PerformUpgrade();
```