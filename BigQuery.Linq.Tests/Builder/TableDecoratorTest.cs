using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace BigQuery.Linq.Tests.Builder
{
    [TestClass]
    public class TableDecoratorTest
    {
        [TestMethod]
        public void ToBigQueryTimestamp()
        {
            new DateTime(1970, 1, 1, 0, 0, 0).ToBigQueryTimestamp().Is(0);
            new DateTime(1970, 1, 1, 0, 0, 0, 1).ToBigQueryTimestamp().Is(1000);
            new DateTime(1970, 1, 1, 0, 0, 1).ToBigQueryTimestamp().Is(1000000);
            new DateTime(2014, 8, 8, 13, 20, 14).ToBigQueryTimestamp().Is(1407504014000000);
            new DateTime(2012, 10, 1, 2, 3, 4).ToBigQueryTimestamp().Is(1349056984000000);
        }
        [TestMethod]
        public void ToBigQueryTimestampOffset()
        {
            new DateTimeOffset(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).ToBigQueryTimestamp().Is(0);
            new DateTimeOffset(new DateTime(1970, 1, 1, 0, 0, 0, 1, DateTimeKind.Utc)).ToBigQueryTimestamp().Is(1000);
            new DateTimeOffset(new DateTime(1970, 1, 1, 0, 0, 1, DateTimeKind.Utc)).ToBigQueryTimestamp().Is(1000000);
            new DateTimeOffset(new DateTime(2014, 8, 8, 13, 20, 14, DateTimeKind.Utc)).ToBigQueryTimestamp().Is(1407504014000000);
            new DateTimeOffset(new DateTime(2012, 10, 1, 2, 3, 4, DateTimeKind.Utc)).ToBigQueryTimestamp().Is(1349056984000000);
        }

        [TestMethod]
        public void FromBigQueryTimestamp()
        {
            var l = 1409549072345L;
            l.FromTimestampMilliSeconds().DateTime.ToString().Is(new DateTime(2014, 9, 1, 5, 24, 32, DateTimeKind.Utc).ToString());
            1349056984000000.FromBigQueryTimestamp().DateTime.Is(new DateTime(2012, 10, 1, 2, 3, 4, DateTimeKind.Utc));
        }

        [TestMethod]
        public void WithSnapshot()
        {
            var context = new BigQuery.Linq.BigQueryContext();
            context.From<int>("tablewikipedia")
                .WithSnapshot()
                .Select()
                .ToString()
                .Is(@"
SELECT
  *
FROM
  [tablewikipedia@0]".TrimSmart());

            context.From<int>("tablewikipedia")
                .WithSnapshot(TimeSpan.FromHours(1))
                .Select()
                .ToString()
                .Is(@"
SELECT
  *
FROM
  [tablewikipedia@-3600000]".TrimSmart());

            context.From<int>("tablewikipedia")
                .WithSnapshot(new DateTime(2014, 8, 8, 13, 20, 14, DateTimeKind.Utc))
                .Select()
                .ToString()
                .Is(@"
SELECT
  *
FROM
  [tablewikipedia@1407504014000000]".TrimSmart());
        }

        [TestMethod]
        public void WithRange()
        {
            var context = new BigQuery.Linq.BigQueryContext();
            context.From<int>("tablewikipedia")
                .WithRange(new DateTime(2014, 8, 8, 13, 20, 14, DateTimeKind.Utc))
                .Select()
                .ToString()
                .Is(@"
SELECT
  *
FROM
  [tablewikipedia@1407504014000000-]".TrimSmart());

            context.From<int>("tablewikipedia")
                .WithRange(TimeSpan.FromHours(1))
                .Select()
                .ToString()
                .Is(@"
SELECT
  *
FROM
  [tablewikipedia@-3600000-]".TrimSmart());

            context.From<int>("tablewikipedia")
                .WithRange(new DateTime(2012, 10, 1, 2, 3, 4, DateTimeKind.Utc), new DateTime(2014, 8, 8, 13, 20, 14, DateTimeKind.Utc))
                .Select()
                .ToString()
                .Is(@"
SELECT
  *
FROM
  [tablewikipedia@1349056984000000-1407504014000000]".TrimSmart());

            context.From<int>("tablewikipedia")
                .WithRange(new DateTime(2012, 10, 1, 2, 3, 4, DateTimeKind.Utc), TimeSpan.FromHours(1))
                .Select()
                .ToString()
                .Is(@"
SELECT
  *
FROM
  [tablewikipedia@1349056984000000--3600000]".TrimSmart());

            context.From<int>("tablewikipedia")
                .WithRange(TimeSpan.FromHours(1), TimeSpan.FromHours(2))
                .Select()
                .ToString()
                .Is(@"
SELECT
  *
FROM
  [tablewikipedia@-3600000--7200000]".TrimSmart());

            context.From<int>("tablewikipedia")
                .WithRange(TimeSpan.FromHours(1), new DateTime(2014, 8, 8, 13, 20, 14, DateTimeKind.Utc))
                .Select()
                .ToString()
                .Is(@"
SELECT
  *
FROM
  [tablewikipedia@-3600000-1407504014000000]".TrimSmart());
        }

        [TestMethod]
        public void RealQuery()
        {
            var context = new BigQuery.Linq.BigQueryContext();
            context.From<int>("tablewikipedia")
                .WithRange(new DateTime(2014, 8, 8, 13, 20, 14, DateTimeKind.Utc))
                .Select()
                .ToString()
                .Is(@"
SELECT
  *
FROM
  [tablewikipedia@1407504014000000-]
".TrimSmart());
        }

        [TestMethod]
        public void FromIndent()
        {
            var context = new BigQuery.Linq.BigQueryContext();
            context.From<int>("tablewikipedia")
                .WithRange(new DateTime(2014, 8, 8, 13, 20, 14, DateTimeKind.Utc))
                .Select()
                .AsSubquery()
                .Select()
                .ToString()
                .Is(@"
SELECT
  *
FROM
(
  SELECT
    *
  FROM
    [tablewikipedia@1407504014000000-]
)".TrimSmart());
        }
    }
}
