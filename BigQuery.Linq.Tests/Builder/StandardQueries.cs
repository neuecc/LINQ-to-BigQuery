using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using BigQuery.Linq.Functions;

namespace BigQuery.Linq.Tests.Builder
{
    class Hoge
    {
        public double MyProperty { get; set; }
    }


    class Huga
    {
        public double MyProperty2 { get; set; }
    }

    class HogeMoge
    {
        public string Hoge { get; set; }
        public string Huga { get; set; }
        public int Tako { get; set; }
    }

    class Wikipedia
    {
        public string title { get; set; }

        public int wp_namespace { get; set; }
    }

    class Repository
    {
        public bool has_downloads { get; set; }
    }

    [TestClass]
    public class StandardQueries
    {
        [TestMethod]
        public void DirectSelect()
        {
            var context = new BigQuery.Linq.BigQueryContext();

            var s = context.Select(() => new
            {
                A = "aaa",
                B = Mathematical.Abs(-5),
                FROM = 100,
            }).ToString().TrimEnd();

            s.Is(@"SELECT
  'aaa' AS [A],
  ABS(-5) AS [B],
  100 AS [FROM]");
        }

        [TestMethod]
        public void WhereSelect()
        {
            var context = new BigQuery.Linq.BigQueryContext();

            var s = context.From<Wikipedia>("tablewikipedia")
                .Where(x => x.wp_namespace == 100)
                .Select(x => new { x.title, x.wp_namespace })
                .ToString().TrimEnd();

            s.Is(@"SELECT
  [title],
  [wp_namespace]
FROM
  tablewikipedia
WHERE
  [wp_namespace] = 100");
        }

        [TestMethod]
        public void WhereWhere()
        {
            var context = new BigQuery.Linq.BigQueryContext();

            var s = context.From<Wikipedia>("tablewikipedia")
                .Where(x => x.wp_namespace == 100)
                .Where(x => x.title != null)
                .Where(x => x.title == "AiUeo")
                .Select(x => new { x.title, x.wp_namespace })
                .ToString().TrimEnd();

            s.Is(@"SELECT
  [title],
  [wp_namespace]
FROM
  tablewikipedia
WHERE
  [wp_namespace] = 100 AND [title] IS NOT NULL AND [title] = 'AiUeo'");
        }

        [TestMethod]
        public void WithSnapshot()
        {
            var context = new BigQuery.Linq.BigQueryContext();
            context.From<Wikipedia>("tablewikipedia")
                .WithSnapshot()
                .ToString()
                .Replace(Environment.NewLine + "  ", " ")
                .Is("FROM [tablewikipedia@0]");

            context.From<Wikipedia>("tablewikipedia")
                .WithSnapshot(TimeSpan.FromHours(1))
                .ToString()
                .Replace(Environment.NewLine + "  ", " ")
                .Is("FROM [tablewikipedia@-3600000]");

            context.From<Wikipedia>("tablewikipedia")
                .WithSnapshot(new DateTime(2014, 8, 8, 13, 20, 14))
                .ToString()
                .Replace(Environment.NewLine + "  ", " ")
                .Is("FROM [tablewikipedia@1407504014000000]");
        }

        [TestMethod]
        public void WithRange()
        {
            var context = new BigQuery.Linq.BigQueryContext();
            context.From<Wikipedia>("tablewikipedia")
                .WithRange(new DateTime(2014, 8, 8, 13, 20, 14))
                .ToString()
                .Replace(Environment.NewLine + "  ", " ")
                .Is("FROM [tablewikipedia@1407504014000000-]");

            context.From<Wikipedia>("tablewikipedia")
                .WithRange(TimeSpan.FromHours(1))
                .ToString()
                .Replace(Environment.NewLine + "  ", " ")
                .Is("FROM [tablewikipedia@-3600000-]");

            context.From<Wikipedia>("tablewikipedia")
                .WithRange(new DateTime(2012, 10, 1, 2, 3, 4), new DateTime(2014, 8, 8, 13, 20, 14))
                .ToString()
                .Replace(Environment.NewLine + "  ", " ")
                .Is("FROM [tablewikipedia@1349056984000000-1407504014000000]");

            context.From<Wikipedia>("tablewikipedia")
                .WithRange(new DateTime(2012, 10, 1, 2, 3, 4), TimeSpan.FromHours(1))
                .ToString()
                .Replace(Environment.NewLine + "  ", " ")
                .Is("FROM [tablewikipedia@1349056984000000--3600000]");

            context.From<Wikipedia>("tablewikipedia")
                .WithRange(TimeSpan.FromHours(1), TimeSpan.FromHours(2))
                .ToString()
                .Replace(Environment.NewLine + "  ", " ")
                .Is("FROM [tablewikipedia@-3600000--7200000]");

            context.From<Wikipedia>("tablewikipedia")
                .WithRange(TimeSpan.FromHours(1), new DateTime(2014, 8, 8, 13, 20, 14))
                .ToString()
                .Replace(Environment.NewLine + "  ", " ")
                .Is("FROM [tablewikipedia@-3600000-1407504014000000]");
        }

        [TestMethod]
        public void ToBigQueryTimestamp()
        {
            new DateTime(1970, 1, 1, 0, 0, 0).ToBigQueryTimestamp().Is(0);
            new DateTime(1970, 1, 1, 0, 0, 0, 1).ToBigQueryTimestamp().Is(1000);
            new DateTime(1970, 1, 1, 0, 0, 1).ToBigQueryTimestamp().Is(1000000);
            new DateTime(2014, 8, 8, 13, 20, 14).ToBigQueryTimestamp().Is(1407504014000000);
            new DateTime(2012, 10, 1, 2, 3, 4).ToBigQueryTimestamp().Is(1349056984000000);
        }
    }
}
