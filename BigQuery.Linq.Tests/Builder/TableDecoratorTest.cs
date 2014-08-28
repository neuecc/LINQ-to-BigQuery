using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using BigQuery.Linq.Functions;

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
        public void WithSnapshot()
        {
            var context = new BigQuery.Linq.BigQueryContext();
            context.From<int>("tablewikipedia")
                .WithSnapshot()
                .ToString()
                .Replace(Environment.NewLine + "  ", " ")
                .Is("FROM [tablewikipedia@0]");

            context.From<int>("tablewikipedia")
                .WithSnapshot(TimeSpan.FromHours(1))
                .ToString()
                .Replace(Environment.NewLine + "  ", " ")
                .Is("FROM [tablewikipedia@-3600000]");

            context.From<int>("tablewikipedia")
                .WithSnapshot(new DateTime(2014, 8, 8, 13, 20, 14))
                .ToString()
                .Replace(Environment.NewLine + "  ", " ")
                .Is("FROM [tablewikipedia@1407504014000000]");
        }

        [TestMethod]
        public void WithRange()
        {
            var context = new BigQuery.Linq.BigQueryContext();
            context.From<int>("tablewikipedia")
                .WithRange(new DateTime(2014, 8, 8, 13, 20, 14))
                .ToString()
                .Replace(Environment.NewLine + "  ", " ")
                .Is("FROM [tablewikipedia@1407504014000000-]");

            context.From<int>("tablewikipedia")
                .WithRange(TimeSpan.FromHours(1))
                .ToString()
                .Replace(Environment.NewLine + "  ", " ")
                .Is("FROM [tablewikipedia@-3600000-]");

            context.From<int>("tablewikipedia")
                .WithRange(new DateTime(2012, 10, 1, 2, 3, 4), new DateTime(2014, 8, 8, 13, 20, 14))
                .ToString()
                .Replace(Environment.NewLine + "  ", " ")
                .Is("FROM [tablewikipedia@1349056984000000-1407504014000000]");

            context.From<int>("tablewikipedia")
                .WithRange(new DateTime(2012, 10, 1, 2, 3, 4), TimeSpan.FromHours(1))
                .ToString()
                .Replace(Environment.NewLine + "  ", " ")
                .Is("FROM [tablewikipedia@1349056984000000--3600000]");

            context.From<int>("tablewikipedia")
                .WithRange(TimeSpan.FromHours(1), TimeSpan.FromHours(2))
                .ToString()
                .Replace(Environment.NewLine + "  ", " ")
                .Is("FROM [tablewikipedia@-3600000--7200000]");

            context.From<int>("tablewikipedia")
                .WithRange(TimeSpan.FromHours(1), new DateTime(2014, 8, 8, 13, 20, 14))
                .ToString()
                .Replace(Environment.NewLine + "  ", " ")
                .Is("FROM [tablewikipedia@-3600000-1407504014000000]");
        }
    }
}
