namespace VippsMobilePay.Changefeed.AcceptanceTests.Tests;

public record FeedResult : EventSourceDto
{
    public required byte[] Ulid { get; set; }
}