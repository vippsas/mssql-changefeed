CREATE TABLE [dbo].[EventSource]
(
    [AggregateId] [uniqueidentifier] NOT NULL,
    [Sequence] [int] NOT NULL,
    [Data] [varchar](max) NOT NULL,
    [Timestamp] [datetime2](7) NOT NULL,
    CONSTRAINT [PK_EventSource] PRIMARY KEY ([AggregateId], [Sequence]),
);