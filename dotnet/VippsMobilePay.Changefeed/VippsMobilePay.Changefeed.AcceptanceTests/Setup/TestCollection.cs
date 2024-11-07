using Xunit;

namespace VippsMobilePay.Changefeed.AcceptanceTests.Setup;

[CollectionDefinition(nameof(TestCollection))]
public class TestCollection : ICollectionFixture<SqlServerTestContainer> 
{
}