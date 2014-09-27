using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace BigQuery.Linq.Tests.Builder
{
    [TestClass]
    public class SubQueryTest
    {
        [TestMethod]
        public void Sub()
        {
            var context = new BigQueryContext();
            context.Select(() => new { v = 100 })
                .AsSubquery()
                .Select()
                .ToString()
                .Is(@"
SELECT
  *
FROM
(
  SELECT
    100 AS [v]
)
".TrimSmart());
        }

        [TestMethod]
        public void SubWithoutFrom()
        {
            var context = new BigQueryContext();
            context.Select(() => new { v = 100 })
                .AsSubquery()
                .Join(context.Select(() => new { v4 = 200 }), (a, b) => new { a, b }, x => x.a.v == x.b.v4)
                .Select(x => new { vv = x.a.v })
                .ToString()
                .Is(@"
SELECT
  [a.v] AS [vv]
FROM
(
  SELECT
    100 AS [v]
) AS [a]
INNER JOIN
(
  SELECT
    200 AS [v4]
) AS [b] ON ([a.v] = [b.v4])
".TrimSmart());
        }

        [TestMethod]
        public void MultiFrom()
        {
            var context = new BigQueryContext();
            var sub = context.Select(() => new { v = 100 });
            context.From(sub, sub, sub).Select().Limit(1000).ToString()
                .Is(@"
SELECT
  *
FROM
(
  SELECT
    100 AS [v]
),
(
  SELECT
    100 AS [v]
),
(
  SELECT
    100 AS [v]
)
LIMIT 1000
".TrimSmart());
        }

    }
}
