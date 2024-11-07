using Dapper;
using DbUp;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.Data.SqlClient;
using Polly;
using VippsMobilePay.Changefeed.Sql.DbUp;
using Xunit;

namespace VippsMobilePay.Changefeed.AcceptanceTests.Setup;

public class SqlServerTestContainer : IAsyncLifetime
{
    public const string TableWithChangefeed = "EventSource";
    public static readonly string ConnectionString = $"Server=127.0.0.1,{ContainerPort};Initial Catalog={TestDatabaseName};User Id=SA;Password={MssqlSaPassword};TrustServerCertificate=true;Encrypt=False";

    private const int ContainerPort = 1433;
    private const string MssqlSaPassword = "Secret.00";
    private const string TestDatabaseName = "ChangefeedAcceptanceTests";
    private IContainer? _sqlContainer;

    private readonly ContainerBuilder _sqlContainerBuilder = new ContainerBuilder()
        .WithImage("mcr.microsoft.com/azure-sql-edge:latest")
        .WithPortBinding(1433, ContainerPort)
        .WithEnvironment("ACCEPT_EULA", "Y")
        .WithEnvironment("MSSQL_SA_PASSWORD", MssqlSaPassword)
        .WithEnvironment("MSSQL_PID", "Developer")
        .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(1433));

    private readonly Policy _retryPolicy = Policy
        .Handle<Exception>()
        .WaitAndRetry(10, _ => TimeSpan.FromSeconds(1));

    public async Task InitializeAsync()
    {
        if (await DbExists())
        {
            EnsureAndUpgradeDatabase();
            return;
        }

        _sqlContainer = _sqlContainerBuilder.Build();
        await _sqlContainer.StartAsync();
        _retryPolicy.Execute(EnsureAndUpgradeDatabase);
    }

    public static async Task CleanupDatabase(SqlConnection connection)
    {
        const string cleanupStatement =
            """
            TRUNCATE TABLE [changefeed].[feed:dbo.EventSource]
            TRUNCATE TABLE [changefeed].[outbox:dbo.EventSource]
            TRUNCATE TABLE [changefeed].[state:dbo.EventSource]
            TRUNCATE TABLE [dbo].[EventSource]
            """;

        await connection.ExecuteAsync(cleanupStatement);
    }
    
    private void EnsureAndUpgradeDatabase()
    {
        EnsureDatabase.For.SqlDatabase(ConnectionString);
        
        var upgradeEngine = DeployChanges.To.SqlDatabase(ConnectionString)
            .WithScriptsEmbeddedInAssembly(typeof(SqlServerTestContainer).Assembly)
            .AddChangefeedMigrationSource(TableWithChangefeed, true, false)
            .LogToConsole()
            .Build();

        var upgradeResult = upgradeEngine.PerformUpgrade();

        if (!upgradeResult.Successful)
        {
            throw new InvalidOperationException(
                "Sql database upgrade not successful",
                upgradeResult.Error
            );
        }
    }

    public async Task DisposeAsync()
    {
        if (_sqlContainer is not null)
        {
            await _sqlContainer.StopAsync();
        }
    }

    private async Task<bool> DbExists()
    {
        var connectionStringBuilder = new SqlConnectionStringBuilder(ConnectionString)
        {
            ConnectTimeout = 2,
            InitialCatalog = "master"
        };
        await using var connection = new SqlConnection(connectionStringBuilder.ConnectionString);
        try
        {
            connection.Open();
            connection.Close();
            return true;
        }
        catch (SqlException)
        {
            return false;
        }
    }
}