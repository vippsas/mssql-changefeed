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
        //var timestamp = DateTimeOffset.Now.AddDays(-1);
        var oldEventSourceEntry1 = new EventSourceDto { AggregateId = Guid.NewGuid(), Sequence = 0, Data = "0", Timestamp = DateTimeOffset.Now.AddDays(-2) };
        var oldEventSourceEntry2 = new EventSourceDto { AggregateId = Guid.NewGuid(), Sequence = 1, Data = "1", Timestamp = DateTimeOffset.Now.AddDays(-1) };
        await using var oldEventsTransaction = _connection.BeginTransaction();
        await InsertIntoEventSource(oldEventSourceEntry1, _connection, oldEventsTransaction);
        await InsertIntoEventSource(oldEventSourceEntry2, _connection, oldEventsTransaction);
        await oldEventsTransaction.CommitAsync();

        // Starting to back fill feed 
        await using var backFillTransaction = _connection.BeginTransaction();
        await InsertIntoFeed(oldEventSourceEntry1, _connection, backFillTransaction);
        await InsertIntoFeed(oldEventSourceEntry2, _connection, backFillTransaction);
        await backFillTransaction.CommitAsync();
        
        // Starting changefeed - Simulate live events at the same time as back filling is running
        var timestamp = DateTimeOffset.Now;
        var liveEvent1 = new EventSourceDto { AggregateId = Guid.NewGuid(), Sequence = 2, Data = "2", Timestamp = timestamp };
        var liveEvent2 = new EventSourceDto { AggregateId = Guid.NewGuid(), Sequence = 3, Data = "3", Timestamp = timestamp };
        await InsertIntoDatabaseInTransaction(liveEvent1, _connection);
        await InsertIntoDatabaseInTransaction(liveEvent2, _connection);

        // Continue to back fill outbox after changefeed is started
        await using var continueBackFillTransaction = _connection.BeginTransaction();
        await InsertIntoFeed(liveEvent1, _connection, continueBackFillTransaction);
        await InsertIntoFeed(liveEvent2, _connection, continueBackFillTransaction);
        await continueBackFillTransaction.CommitAsync();
        
        // Running changefeed - Back filling is stopped
        timestamp = DateTimeOffset.Now;
        var liveEvent3 = new EventSourceDto { AggregateId = Guid.NewGuid(), Sequence = 4, Data = "4", Timestamp = timestamp };
        var liveEvent4 = new EventSourceDto { AggregateId = Guid.NewGuid(), Sequence = 5, Data = "5", Timestamp = timestamp };
        await InsertIntoDatabaseInTransaction(liveEvent3, _connection);
        await InsertIntoDatabaseInTransaction(liveEvent4, _connection);
        
        // First read determines the order, so wait until back filling is done and stopped 
        // There should be a small overlap on back filling and live events processing to ensure all
        // events are added to the outbox,
        
        // Note that the first page only contains the rows from the feed table. Only when the cursor is up to date
        // with all rows in the feed table it will start to look into the outbox table. The feed and outbox output
        // will not be combined in one read, but requires an additional read to get the last event from the outbox
        var combinedReturnedEntries = new List<FeedResult>();
        var firstPageResult = await ReadFeed(_startCursor, 100, _connection);
        combinedReturnedEntries.AddRange(firstPageResult);

        var secondPageResult = await ReadFeed(firstPageResult.Last().Ulid, 100, _connection);
        combinedReturnedEntries.AddRange(secondPageResult);
        
        var feedResult = combinedReturnedEntries.ToList();
        Assert.Equal(6, feedResult.Count);
        Assert.Equal("0", feedResult[0].Data);
        Assert.Equal("1", feedResult[1].Data);
        Assert.Equal("2", feedResult[2].Data);
        Assert.Equal("3", feedResult[3].Data);
        Assert.Equal("4", feedResult[4].Data);
        Assert.Equal("5", feedResult[5].Data);
    }

    [Fact]
    public async Task Ignore_Inserting_Into_Outbox_If_It_Already_Exists()
    {
        var eventSourceDto = new EventSourceDto { AggregateId = Guid.NewGuid(), Sequence = 0, Data = "0", Timestamp = DateTimeOffset.Now };
        await InsertIntoDatabaseInTransaction(eventSourceDto, _connection);
        
        await using var transaction = _connection.BeginTransaction();
        await InsertIntoFeed(eventSourceDto, _connection, transaction);
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
    
    private static async Task InsertIntoFeed(
        EventSourceDto eventSourceDto,
        SqlConnection connection,
        SqlTransaction transaction)
    {
        const string insertIntoFeedStatement =
            """
            INSERT INTO [changefeed].[feed:dbo.EventSource] (shard_id, ulid, AggregateId, Sequence)
            SELECT 0, @Ulid, @AggregateId, @Sequence
            WHERE NOT EXISTS (
                SELECT 1
                FROM [changefeed].[feed:dbo.EventSource]
                WHERE AggregateId = @AggregateId
                AND Sequence = @Sequence
            )
            AND NOT EXISTS (
                SELECT 1
                FROM [changefeed].[outbox:dbo.EventSource]
                WHERE AggregateId = @AggregateId
                AND Sequence = @Sequence
            );
            """;
        
        var results = await connection.ExecuteAsync(
            insertIntoFeedStatement,
            new
            {
                Ulid = GenerateUlidBinary(eventSourceDto.Timestamp),
                AggregateId = eventSourceDto.AggregateId,
                Sequence = eventSourceDto.Sequence
            },
            transaction);
    }
    
    public static byte[] GenerateUlidBinary(DateTimeOffset timestamp)
    {
        var ulid = Ulid.NewUlid();
        var ulidBytes = ulid.ToByteArray();

        var timestampMs = (long)(timestamp.UtcDateTime - DateTime.UnixEpoch).TotalMilliseconds;

        ulidBytes[0] = (byte)((timestampMs >> 40) & 0xFF);
        ulidBytes[1] = (byte)((timestampMs >> 32) & 0xFF);
        ulidBytes[2] = (byte)((timestampMs >> 24) & 0xFF);
        ulidBytes[3] = (byte)((timestampMs >> 16) & 0xFF);
        ulidBytes[4] = (byte)((timestampMs >> 8) & 0xFF);
        ulidBytes[5] = (byte)(timestampMs & 0xFF);

        return ulidBytes;
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