namespace VippsMobilePay.Changefeed.AcceptanceTests.Tests;

public record EventSourceDto
{
    public required Guid AggregateId { get; set; }
    public required int Sequence { get; set; }
    public required string Data { get; set; }
    public required DateTimeOffset Timestamp { get; set; }
}