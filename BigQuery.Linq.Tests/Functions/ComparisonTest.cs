using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Tests.Functions
{
    [TestClass]
    public class ComparisonTest
    {
        [TestMethod]
        public void Symbol()
        {
            var context = new BigQueryContext();
            var x = 1000;
            var y = "hoge";
            context.Select(() => x == 1000).ToFlatSql().Is("SELECT (1000 = 1000)");
            context.Select(() => x != 1000).ToFlatSql().Is("SELECT (1000 != 1000)");
            context.Select(() => x > 1000).ToFlatSql().Is("SELECT (1000 > 1000)");
            context.Select(() => x < 1000).ToFlatSql().Is("SELECT (1000 < 1000)");
            context.Select(() => x >= 1000).ToFlatSql().Is("SELECT (1000 >= 1000)");
            context.Select(() => x <= 1000).ToFlatSql().Is("SELECT (1000 <= 1000)");
            context.Select(() => y == null).ToFlatSql().Is("SELECT ('hoge' IS NULL)");
            context.Select(() => y != null).ToFlatSql().Is("SELECT ('hoge' IS NOT NULL)");

            long? x2 = 1000;
            context.Select(() => x2 == 1000).ToFlatSql().Is("SELECT (1000 = 1000)");
            context.Select(() => x2.Value == 1000).ToFlatSql().Is("SELECT (1000 = 1000)");
        }

        [TestMethod]
        public void Between()
        {
            var context = new BigQueryContext();
            var x = 1000;

            context.Select<bool>(() => BqFunc.Between(x, BqFunc.Abs(10), 100)).ToFlatSql()
                .Is("SELECT (1000 BETWEEN ABS(10) AND 100)");
        }

        [TestMethod]
        public void IfNull()
        {
            var context = new BigQueryContext();

            var x = "hogehoge";
            context.Select<string>(() => x ?? "hugahuga").ToFlatSql()
                .Is("SELECT IFNULL('hogehoge', 'hugahuga')");

            context.Select(() => BqFunc.Integer("a") ?? -1L).ToFlatSql()
                .Is("SELECT IFNULL(INTEGER('a'), -1)");
        }

        [TestMethod]
        public void In()
        {
            var context = new BigQueryContext();
            context.Select(() => new { value = 100 })
                .Into()
                .Select(x => (BqFunc.In(x.value, 10, 20, 50, 1000)) ? 10000 : -10)
                .ToString()
                .Is(@"
SELECT
  IF([value] IN(10, 20, 50, 1000), 10000, -10)
FROM
(
  SELECT
    100 AS [value]
)
".TrimSmart());

            // not in
            context.Select(() => new { value = 100 })
                .Into()
                .Select(x => (!BqFunc.In(x.value, 10, 20, 50, 1000)) ? 10000 : -10)
                .ToString()
                .Is(@"
SELECT
  IF(NOT [value] IN(10, 20, 50, 1000), 10000, -10)
FROM
(
  SELECT
    100 AS [value]
)
".TrimSmart());
        }

        [TestMethod]
        public void In_SubQuery()
        {
            var context = new BigQueryContext();
            context.Select(() => new { value = 100L })
                .Into()
                .Where(x => (BqFunc.In(x.value, context.From<wikipedia>().Select(y => y.id ?? -1).Limit(1000))))
                .Select(x => x.value)
                .ToString()
                .Is(@"
SELECT
  [value]
FROM
(
  SELECT
    100 AS [value]
)
WHERE
  [value] IN
  (
    SELECT
      IFNULL([id], -1)
    FROM
      [publicdata:samples.wikipedia]
    LIMIT 1000
  )
".TrimSmart());
        }

        [TestMethod]
        public void In_SubQuery2()
        {
            var context = new BigQueryContext();
            context.Select(() => new { value = 100L })
                .Into()
                .Select(x => new{x.value})
                .GroupBy(x => x.value)
                .Having(x => (BqFunc.In(x.value, context.From<wikipedia>().Select(y => y.id ?? -1).Limit(1000))))
                .ToString()
                .Is(@"
SELECT
  [value]
FROM
(
  SELECT
    100 AS [value]
)
GROUP BY
  [value]
HAVING
  [value] IN
  (
    SELECT
      IFNULL([id], -1)
    FROM
      [publicdata:samples.wikipedia]
    LIMIT 1000
  )
".TrimSmart());
        }

        [TestMethod]
        public void InWithArray()
        {
            var vals = new object[] {10, 20, 50, 1000};
            var context = new BigQueryContext();
            context.Select(() => new { value = 100 })
                .Into()
                .Select(x => (BqFunc.In(x.value, vals)) ? 10000 : -10)
                .ToString()
                .Is(@"
SELECT
  IF([value] IN(10, 20, 50, 1000), 10000, -10)
FROM
(
  SELECT
    100 AS [value]
)
".TrimSmart());
        }

        [TestMethod]
        public void InWithStringArray()
        {
            var vals = new object[] { "10", "20", "50", "1000" };
            var context = new BigQueryContext();
            context.Select(() => new { value = "100" })
                .Into()
                .Select(x => (BqFunc.In(x.value, vals)) ? "10000" : "-10")
                .ToString()
                .Is(@"
SELECT
  IF([value] IN('10', '20', '50', '1000'), '10000', '-10')
FROM
(
  SELECT
    '100' AS [value]
)
".TrimSmart());
        }

        [TestMethod]
        public void NotIn()
        {
            var context = new BigQueryContext();
            context.Select(() => new { value = 100 })
                .Into()
                .Select(x => (BqFunc.NotIn(x.value, 10, 20, 50, 1000)) ? 10000 : -10)
                .ToString()
                .Is(@"
SELECT
  IF([value] NOT IN(10, 20, 50, 1000), 10000, -10)
FROM
(
  SELECT
    100 AS [value]
)
".TrimSmart());
        }

        [TestMethod]
        public void NotIn_SubQuery()
        {
            var context = new BigQueryContext();
            context.Select(() => new { value = 100L })
                .Into()
                .Where(x => (BqFunc.NotIn(x.value, context.From<wikipedia>().Select(y => y.id ?? -1).Limit(1000))))
                .Select(x => x.value)
                .ToString()
                .Is(@"
SELECT
  [value]
FROM
(
  SELECT
    100 AS [value]
)
WHERE
  [value] NOT IN
  (
    SELECT
      IFNULL([id], -1)
    FROM
      [publicdata:samples.wikipedia]
    LIMIT 1000
  )
".TrimSmart());
        }

        [TestMethod]
        public void NotIn_SubQuery2()
        {
            var context = new BigQueryContext();
            context.Select(() => new { value = 100L })
                .Into()
                .Select(x => new { x.value })
                .GroupBy(x => x.value)
                .Having(x => (BqFunc.NotIn(x.value, context.From<wikipedia>().Select(y => y.id ?? -1).Limit(1000))))
                .ToString()
                .Is(@"
SELECT
  [value]
FROM
(
  SELECT
    100 AS [value]
)
GROUP BY
  [value]
HAVING
  [value] NOT IN
  (
    SELECT
      IFNULL([id], -1)
    FROM
      [publicdata:samples.wikipedia]
    LIMIT 1000
  )
".TrimSmart());
        }

        [TestMethod]
        public void In_Subquery_real()
        {
            var context = new BigQueryContext();
            var inTarget = context
                .From<github_timeline>("[githubarchive:github.timeline]")
                .Where(x => x.type=="CreateEvent" 
                             && BqFunc.ParseUtcUsec(x.repository_created_at) >= BqFunc.ParseUtcUsec(BqFunc.StrftimeUtcUsec(BqFunc.TimestampToUsec(BqFunc.DateAdd(BqFunc.UsecToTimestamp(BqFunc.Now()), -1, IntervalUnit.Day)), "%Y-%m-%d 20:00:00"))
                    && x.repository_fork == "false"
                    && x.payload_ref_type == "repository")
                .Select(x => new{ x.repository_url })
                .GroupBy(x => x.repository_url);

            context
                .From<github_timeline>("[githubarchive:github.timeline]")
                .Where(x => x.type == "WatchEvent"
                        && BqFunc.ParseUtcUsec(x.repository_created_at) >= BqFunc.ParseUtcUsec(BqFunc.StrftimeUtcUsec(BqFunc.TimestampToUsec(BqFunc.DateAdd(BqFunc.UsecToTimestamp(BqFunc.Now()), -1, IntervalUnit.Day)), "%Y-%m-%d 20:00:00"))
                    && BqFunc.In(x.repository_url, inTarget))
                .Select(x => new
                {
                    x.repository_name,
                    x.repository_language,
                    x.repository_description,
                    cnt = BqFunc.Count(x.repository_name),
                    x.repository_url
                })
                .GroupBy(x => new { x.repository_name, x.repository_language, x.repository_description, x.repository_url })
                .Having(x => x.cnt >= 5)
                .OrderByDescending(x => x.cnt)
                .Limit(25)
                .ToString()
                .Is(@"
SELECT
  [repository_name],
  [repository_language],
  [repository_description],
  COUNT([repository_name]) AS [cnt],
  [repository_url]
FROM
  [githubarchive:github.timeline]
WHERE
  ((([type] = 'WatchEvent') AND (PARSE_UTC_USEC([repository_created_at]) >= PARSE_UTC_USEC(STRFTIME_UTC_USEC(TIMESTAMP_TO_USEC(DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, 'DAY')), '%Y-%m-%d 20:00:00')))) AND [repository_url] IN
  (
    SELECT
      [repository_url]
    FROM
      [githubarchive:github.timeline]
    WHERE
      (((([type] = 'CreateEvent') AND (PARSE_UTC_USEC([repository_created_at]) >= PARSE_UTC_USEC(STRFTIME_UTC_USEC(TIMESTAMP_TO_USEC(DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -1, 'DAY')), '%Y-%m-%d 20:00:00')))) AND ([repository_fork] = 'false')) AND ([payload_ref_type] = 'repository'))
    GROUP BY
      [repository_url]
  ))
GROUP BY
  [repository_name],
  [repository_language],
  [repository_description],
  [repository_url]
HAVING
  ([cnt] >= 5)
ORDER BY
  [cnt] DESC
LIMIT 25".TrimSmart());
        }

        [TestMethod]
        public void Greatest()
        {
            var context = new BigQueryContext();
            context.Select(() => BqFunc.Greatest(1, 10000, 100, 20)).ToFlatSql().Is("SELECT GREATEST(1, 10000, 100, 20)");
            context.Select(() => BqFunc.Greatest(1.5, 3.54, 2.3, 0.3)).ToFlatSql().Is("SELECT GREATEST(1.5, 3.54, 2.3, 0.3)");
        }

        [TestMethod]
        public void Least()
        {
            var context = new BigQueryContext();
            context.Select(() => BqFunc.Least(1, 10000, 100, 20)).ToFlatSql().Is("SELECT LEAST(1, 10000, 100, 20)");
            context.Select(() => BqFunc.Least(1.5, 3.54, 2.3, 0.3)).ToFlatSql().Is("SELECT LEAST(1.5, 3.54, 2.3, 0.3)");
        }

        [TestMethod]
        public void IsInf_Nan()
        {
            var context = new BigQueryContext();
            context.Select(() => BqFunc.IsInfinity(10.5)).ToFlatSql().Is("SELECT IS_INF(10.5)");
            context.Select(() => BqFunc.IsNAN(10.5)).ToFlatSql().Is("SELECT IS_NAN(10.5)");

        }
    }
}
