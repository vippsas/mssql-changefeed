using System.Data;
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
        await InsertIntoDatabaseInTransaction(new EventSourceDto{AggregateId = Guid.NewGuid(), Sequence = 0, Data = "2", Timestamp = timestamp}, _connection);
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

    [Fact]
    public async Task Back_Fill_Feed()
    {
        // Existing events - before back fill and changefeed is started
        var oldEvent0 = new EventSourceDto { AggregateId = Guid.NewGuid(), Sequence = 0, Data = "0", Timestamp = DateTimeOffset.Now.AddDays(-4) };
        var oldEvent1 = new EventSourceDto { AggregateId = Guid.NewGuid(), Sequence = 1, Data = "1", Timestamp = DateTimeOffset.Now.AddDays(-3) };
        var oldEvent2 = new EventSourceDto { AggregateId = Guid.NewGuid(), Sequence = 2, Data = "2", Timestamp = DateTimeOffset.Now.AddDays(-2) };
        var oldEvent3 = new EventSourceDto { AggregateId = Guid.NewGuid(), Sequence = 3, Data = "3", Timestamp = DateTimeOffset.Now.AddDays(-1) };
        await using var oldEventsTransaction1 = _connection.BeginTransaction();
        await InsertIntoEventSource(oldEvent0, _connection, oldEventsTransaction1);
        await InsertIntoEventSource(oldEvent1, _connection, oldEventsTransaction1);
        await InsertIntoEventSource(oldEvent2, _connection, oldEventsTransaction1);
        await InsertIntoEventSource(oldEvent3, _connection, oldEventsTransaction1);
        await oldEventsTransaction1.CommitAsync();

        // Starting to back fill feed 
        await using var backFillTransaction1 = _connection.BeginTransaction();
        await InsertIntoOutbox(oldEvent0, _connection, backFillTransaction1);
        await InsertIntoOutbox(oldEvent1, _connection, backFillTransaction1);
        await backFillTransaction1.CommitAsync();

        // Continue to back fill events - before changefeed is started
        await using var backFillTransaction2 = _connection.BeginTransaction();
        await InsertIntoOutbox(oldEvent2, _connection, backFillTransaction2);
        await InsertIntoOutbox(oldEvent3, _connection, backFillTransaction2);
        await backFillTransaction2.CommitAsync();
        
        // Starting changefeed - Simulate live events at the same time as back filling is running
        var timestamp = DateTimeOffset.Now;
        var liveEvent1 = new EventSourceDto { AggregateId = Guid.NewGuid(), Sequence = 4, Data = "4", Timestamp = timestamp };
        var liveEvent2 = new EventSourceDto { AggregateId = Guid.NewGuid(), Sequence = 5, Data = "5", Timestamp = timestamp };
        await InsertIntoDatabaseInTransaction(liveEvent1, _connection);
        await InsertIntoDatabaseInTransaction(liveEvent2, _connection);
        
        // Continue to back fill outbox with events already processed by changefeed is started
        await using var continueBackFillTransaction = _connection.BeginTransaction();
        await InsertIntoOutbox(liveEvent1, _connection, continueBackFillTransaction);
        await InsertIntoOutbox(liveEvent2, _connection, continueBackFillTransaction);
        await continueBackFillTransaction.CommitAsync();
        
        // Running changefeed - Back filling is stopped
        timestamp = DateTimeOffset.Now;
        var liveEvent3 = new EventSourceDto { AggregateId = Guid.NewGuid(), Sequence = 6, Data = "6", Timestamp = timestamp };
        var liveEvent4 = new EventSourceDto { AggregateId = Guid.NewGuid(), Sequence = 7, Data = "7", Timestamp = timestamp };
        await InsertIntoDatabaseInTransaction(liveEvent3, _connection);
        await InsertIntoDatabaseInTransaction(liveEvent4, _connection);
        
        // First read determines the order, so wait until back filling is done and stopped 
        // There should be a small overlap on back filling and live events processing to ensure all
        // events are added to the outbox,
        
        // Note that the first page only contains rows from the feed table. Only when the cursor is up to date
        // with all rows in the feed table it will start to look into the outbox table. The feed and outbox output
        // will not be combined in one read, but requires an additional read to get the events from the outbox.
        // In this case it does not matter as all events are in the outbox table and the secondPageResult will 
        // therefore be empty.
        var combinedReturnedEntries = new List<FeedResult>();
        var firstPageResult = await ReadFeed(_startCursor, 100, _connection);
        combinedReturnedEntries.AddRange(firstPageResult);

        var secondPageResult = await ReadFeed(firstPageResult.Last().Ulid, 100, _connection);
        combinedReturnedEntries.AddRange(secondPageResult);
        
        var feedResult = combinedReturnedEntries.ToList();
        Assert.Equal(8, feedResult.Count);
        Assert.Equal("0", feedResult[0].Data);
        Assert.Equal("1", feedResult[1].Data);
        Assert.Equal("2", feedResult[2].Data);
        Assert.Equal("3", feedResult[3].Data);
        Assert.Equal("4", feedResult[4].Data);
        Assert.Equal("5", feedResult[5].Data);
        Assert.Equal("6", feedResult[6].Data);
        Assert.Equal("7", feedResult[7].Data);
    }

    [Fact]
    public async Task Ignore_Inserting_Into_Outbox_If_It_Already_Exists()
    {
        var eventSourceDto = new EventSourceDto { AggregateId = Guid.NewGuid(), Sequence = 0, Data = "0", Timestamp = DateTimeOffset.Now };
        await InsertIntoDatabaseInTransaction(eventSourceDto, _connection);
        
        await using var transaction = _connection.BeginTransaction();
        await InsertIntoOutbox(eventSourceDto, _connection, transaction);
        await transaction.CommitAsync();
        
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
            IF NOT EXISTS (
                SELECT 1
                FROM [changefeed].[outbox:dbo.EventSource]
                WHERE
                    AggregateId = @AggregateId
                    AND Sequence = @Sequence)
            BEGIN
                INSERT INTO [changefeed].[outbox:dbo.EventSource] (shard_id, time_hint, AggregateId, Sequence)
                VALUES (0, @TimeHint, @AggregateId, @Sequence);
            END;
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
                R.Sequence = [ChangefeedAcceptanceTests].[dbo].[EventSource].Sequence
            ORDER BY
                ulid
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