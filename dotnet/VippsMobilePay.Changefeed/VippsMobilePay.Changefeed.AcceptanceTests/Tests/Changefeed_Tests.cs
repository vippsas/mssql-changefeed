using AutoFixture.Xunit2;
using Dapper;
using Microsoft.Data.SqlClient;
using VippsMobilePay.Changefeed.AcceptanceTests.Setup;
using Xunit;

namespace VippsMobilePay.Changefeed.AcceptanceTests.Tests;

[Collection(nameof(TestCollection))]
public class Changefeed_Tests : IAsyncLifetime
{
    private readonly byte[] _startCursor = new byte[16]; 
    private readonly SqlConnection _connection;

    public Changefeed_Tests()
    {
        _connection = new SqlConnection(SqlServerTestContainer.ConnectionString);
    }

    public async Task InitializeAsync()
    {
        await _connection.OpenAsync();
        await SqlServerTestContainer.CleanupDatabase(_connection);
    }

    public async Task DisposeAsync()
    {
        await _connection.DisposeAsync();
    }
    
    [Theory, AutoData]
    public async Task Insert_In_Transaction_And_Read_Outside_Transaction(EventSourceDto eventSourceDto)
    {
        await InsertIntoDatabaseInTransaction(eventSourceDto, _connection);

        var result = await ReadFeed(_startCursor, 10, _connection);

        Assert.NotNull(result);
        var feedResult = result.ToList();
        Assert.Single(feedResult);
        Assert.Equal(eventSourceDto.AggregateId, feedResult.Single().AggregateId);
        Assert.Equal(eventSourceDto.Sequence, feedResult.Single().Sequence);
        Assert.Equal(eventSourceDto.Data, feedResult.Single().Data);
        Assert.Equal(eventSourceDto.Timestamp, feedResult.Single().Timestamp);
        Assert.NotEqual(_startCursor, feedResult.Single().Ulid);
    }

    [Fact]
    public async Task Insert_Multiple_And_Read_Pages()
    {
        await InsertIntoDatabaseInTransaction(new EventSourceDto{AggregateId = Guid.NewGuid(), Sequence = 0, Data = "0", Timestamp = DateTimeOffset.Now}, _connection);
        await InsertIntoDatabaseInTransaction(new EventSourceDto{AggregateId = Guid.NewGuid(), Sequence = 0, Data = "1", Timestamp = DateTimeOffset.Now}, _connection);
        await InsertIntoDatabaseInTransaction(new EventSourceDto{AggregateId = Guid.NewGuid(), Sequence = 0, Data = "2", Timestamp = DateTimeOffset.Now}, _connection);
        await InsertIntoDatabaseInTransaction(new EventSourceDto{AggregateId = Guid.NewGuid(), Sequence = 0, Data = "3", Timestamp = DateTimeOffset.Now}, _connection);
        await InsertIntoDatabaseInTransaction(new EventSourceDto{AggregateId = Guid.NewGuid(), Sequence = 0, Data = "4", Timestamp = DateTimeOffset.Now}, _connection);

        var result = await ReadFeed(_startCursor, 3, _connection);
        var feedResult = result.ToList();
        Assert.Equal(3, feedResult.Count);
        
        result = await ReadFeed(result.Last().Ulid, 3, _connection);
        feedResult = result.ToList();
        Assert.Equal(2, feedResult.Count);
    }

    [Fact]
    public async Task Events_Are_Ordered_By_Insert_Order_And_Ignoring_Timestamp()
    {
        var combinedReturnedEntries = new List<FeedResult>();

        var timestamp = DateTimeOffset.Now;
        await InsertIntoDatabaseInTransaction(new EventSourceDto{AggregateId = Guid.NewGuid(), Sequence = 0, Data = "0", Timestamp = timestamp}, _connection);
        await InsertIntoDatabaseInTransaction(new EventSourceDto{AggregateId = Guid.NewGuid(), Sequence = 0, Data = "1", Timestamp = timestamp}, _connection);
        var firstPageResult = await ReadFeed(_startCursor, 100, _connection);
        combinedReturnedEntries.AddRange(firstPageResult);

        timestamp = DateTimeOffset.Now;
        await InsertIntoDatabaseInTransaction(new EventSourceDto{AggregateId = Guid.NewGuid(), Sequence = 0, Data = "3", Timestamp = timestamp}, _connection);
        await InsertIntoDatabaseInTransaction(new EventSourceDto{AggregateId = Guid.NewGuid(), Sequence = 0, Data = "2", Timestamp = timestamp.AddSeconds(-1)}, _connection);
        var secondPageResult = await ReadFeed(firstPageResult.Last().Ulid, 100, _connection);
        combinedReturnedEntries.AddRange(secondPageResult);

        Assert.Equal(4, combinedReturnedEntries.Count);
        Assert.Equal("0", combinedReturnedEntries[0].Data);
        Assert.Equal("1", combinedReturnedEntries[1].Data);
        Assert.Equal("3", combinedReturnedEntries[2].Data);
        Assert.Equal("2", combinedReturnedEntries[3].Data);
        
        var thirdPageResult = await ReadFeed(secondPageResult.Last().Ulid, 100, _connection);
        Assert.Empty(thirdPageResult);
    }

    private static async Task InsertIntoDatabaseInTransaction(
        EventSourceDto eventSourceDto,
        SqlConnection connection)
    {
        await using var transaction = connection.BeginTransaction();
        await InsertIntoDatabase(eventSourceDto, connection, transaction);
        await transaction.CommitAsync();
    }

    private static async Task InsertIntoDatabase(
        EventSourceDto eventSourceDto,
        SqlConnection connection,
        SqlTransaction transaction)
    {
        await InsertIntoOutbox(eventSourceDto, connection, transaction);
        await InsertIntoEventSource(eventSourceDto, connection, transaction);
    }

    private static async Task InsertIntoOutbox(
        EventSourceDto eventSourceDto,
        SqlConnection connection,
        SqlTransaction transaction)
    {
        const string insertIntoOutboxStatement =
            """
            INSERT INTO [changefeed].[outbox:dbo.EventSource] (shard_id, time_hint, AggregateId, Sequence)
            VALUES (0, @TimeHint, @AggregateId, @Sequence);
            """;
        
        var results = await connection.ExecuteAsync(
            insertIntoOutboxStatement,
            new
            {
                TimeHint = eventSourceDto.Timestamp,
                AggregateId = eventSourceDto.AggregateId,
                Sequence = eventSourceDto.Sequence
            },
            transaction);
    }
    
    private static async Task InsertIntoEventSource(
        EventSourceDto eventSourceDto,
        SqlConnection connection,
        SqlTransaction transaction)
    {
        const string insertIntoEventSourceStatement =
            """
            INSERT INTO [EventSource] (AggregateId, Sequence, Data, Timestamp)
            VALUES (@AggregateId, @Sequence, @Data, @Timestamp);
            """;
        
        var results = await connection.ExecuteAsync(
            insertIntoEventSourceStatement,
            new
            {
                AggregateId = eventSourceDto.AggregateId,
                Sequence = eventSourceDto.Sequence,
                Data = eventSourceDto.Data,
                Timestamp = eventSourceDto.Timestamp
            },
            transaction);
    }
    
    private async Task<IEnumerable<FeedResult>> ReadFeed(
        byte[] cursor, 
        int pageSize, 
        SqlConnection connection)
    {
        const string readEventSourceStatement =
            """
            DECLARE @shard_id INT = 0;

            CREATE TABLE #read (
                [ulid] BINARY(16) NOT NULL,
                [AggregateId] [uniqueidentifier] NOT NULL,
                [Sequence] [int] NOT NULL);

            EXEC [changefeed].[read_feed:dbo.EventSource] @shard_id = @shard_id, @cursor = @cursor, @pagesize = @pagesize;

            SELECT
                [ChangefeedAcceptanceTests].[dbo].[EventSource].[AggregateId],
                [ChangefeedAcceptanceTests].[dbo].[EventSource].[Sequence],
                [Data],
                [Timestamp],
                [ulid]
            FROM
                [ChangefeedAcceptanceTests].[dbo].[EventSource]
            INNER JOIN #read AS R ON
                R.AggregateId = [ChangefeedAcceptanceTests].[dbo].[EventSource].AggregateId AND
                R.Sequence = [ChangefeedAcceptanceTests].[dbo].[EventSource].Sequence;
            """;

        
        var results = await connection.QueryAsync<FeedResult>(
            readEventSourceStatement,
            new
            {
                cursor = cursor,
                pagesize = pageSize
            });
    
        return results;
    }    
}