using DbUp.Builder;
using DbUp.Engine;
using DbUp.Support;

namespace VippsMobilePay.Changefeed.Sql.DbUp;

public static class UpgradeEngineBuilderExtensions
{
    /// <summary>
    /// Adds migration scripts to set up and configure changefeed tables in an MSSQL database.
    /// This extension method enables the setup of a changefeed system by executing the necessary SQL scripts.
    /// See <a href="https://github.com/vippsas/mssql-changefeed">mssql-changefeed</a> for more details.
    /// </summary>
    /// <param name="upgradeEngineBuilder">The DbUp <see cref="UpgradeEngineBuilder"/> to which the changefeed migration scripts will be added.</param>
    /// <param name="tableName">The name of the target table for the changefeed, passed as a parameter to the `setup_feed` stored procedure.
    /// Note that the table creation script must be executed prior to using this method.</param>
    /// <param name="outbox">Indicates if an outbox mechanism should be configured (set to true to enable).</param>
    /// <param name="blocking">Determines whether to use a blocking setup in the changefeed process (set to true to enable).</param>
    /// <param name="runGroupOrderStart">The base value for the run group order of scripts, used to control the order of execution.
    /// The default value is 10000, incremented for each script executed within this method. This will ensure that the scripts here
    /// will be executed last in most cases.</param>
    /// <returns>The <see cref="UpgradeEngineBuilder"/> instance with the changefeed migration scripts configured.</returns>
    public static UpgradeEngineBuilder AddChangefeedMigrationSource(
        this UpgradeEngineBuilder upgradeEngineBuilder,
        string tableName,
        bool outbox,
        bool blocking,
        int runGroupOrderStart = 10000)
    {
        return upgradeEngineBuilder
            .WithScriptsEmbeddedInAssembly(
                typeof(UpgradeEngineBuilderExtensions).Assembly,
                new SqlScriptOptions
                {
                    RunGroupOrder = runGroupOrderStart + 1,
                    ScriptType = ScriptType.RunOnce
                })
            .WithScript("SetupChangefeed",
                $@"
                EXECUTE [changefeed].[setup_feed] 
                    @table_name = N'{tableName}',
                    @outbox = {(outbox ? 1 : 0)},
                    @blocking = {(blocking ? 1 : 0)};
                ",
                new SqlScriptOptions
                {
                    RunGroupOrder = runGroupOrderStart + 2,
                    ScriptType = ScriptType.RunOnce
                });
    }
}