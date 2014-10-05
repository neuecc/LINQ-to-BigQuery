using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace BigQuery.Linq.Tests.Builder
{
    [TestClass]
    public class JoinTest
    {
        [TableName("publicdata:samples.wikipedia")]
        class Wikipe
        {
            public string title { get; set; }
        }

        [TestMethod]
        public void MultiJoinIExecute()
        {
            var ctx = new BigQueryContext();
            ctx.From<Wikipe>()
               .Join(ctx.From<Wikipe>().Select().Limit(100), (A, B) => new { A, B }, x => x.A.title == x.B.title)
               .Join(ctx.From<Wikipe>().Select().Limit(100), (X, C) => new { X.A, X.B, C }, x => x.C.title == x.B.title)
               .Select(x => x.A.title)
               .Into()
               .Select()
               .ToString()
               .Is(@"
SELECT
  *
FROM
(
  SELECT
    [A.title]
  FROM
    [publicdata:samples.wikipedia] AS [A]
  INNER JOIN
  (
    SELECT
      *
    FROM
      [publicdata:samples.wikipedia]
    LIMIT 100
  ) AS [B] ON ([A.title] = [B.title])
  INNER JOIN
  (
    SELECT
      *
    FROM
      [publicdata:samples.wikipedia]
    LIMIT 100
  ) AS [C] ON ([C.title] = [B.title])
)".TrimSmart());
        }

        [TestMethod]
        public void MultiJoinITableName()
        {
            var ctx = new BigQueryContext();
            ctx.From<Wikipe>()
               .Join(ctx.From<Wikipe>(), (A, B) => new { A, B }, x => x.A.title == x.B.title)
               .Join(ctx.From<Wikipe>(), (X, C) => new { X.A, X.B, C }, x => x.C.title == x.B.title)
               .Select(x => x.A.title)
               .ToString()
               .Is(@"
SELECT
  [A.title]
FROM
  [publicdata:samples.wikipedia] AS [A]
INNER JOIN
  [publicdata:samples.wikipedia] AS [B] ON ([A.title] = [B.title])
INNER JOIN
  [publicdata:samples.wikipedia] AS [C] ON ([C.title] = [B.title])
".TrimSmart());
        }

        [TestMethod]
        public void IndentCheck()
        {
            var ctx = new BigQueryContext();
            ctx.From<wikipedia>()
                .Join(ctx.From<wikipedia>(), (A, B) => new { A, B }, x => x.A.title == x.B.title)
                .Select()
                .Into()
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
    [publicdata:samples.wikipedia] AS [A]
  INNER JOIN
    [publicdata:samples.wikipedia] AS [B] ON ([A.title] = [B.title])
)
".TrimSmart());
        }

        [TestMethod]
        public void IndentCheckWithFlatten()
        {
            var ctx = new BigQueryContext();
            ctx.From<wikipedia>()
                .Join(ctx.From<wikipedia>().Flatten(x => x.contributor_id), (A, B) => new { A, B }, x => x.A.title == x.B.title)
                .Select()
                .Into()
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
    [publicdata:samples.wikipedia] AS [A]
  INNER JOIN
    FLATTEN([publicdata:samples.wikipedia], [contributor_id]) AS [B] ON ([A.title] = [B.title])
)
".TrimSmart());
        }

        [TestMethod]
        public void IndentCheckWithFlatten2()
        {
            var ctx = new BigQueryContext();
            ctx.From<wikipedia>()
                .Join(ctx.From<wikipedia>().Select(x => new { x.title, v = x.contributor_id }).Into().Flatten(x => x.v), (A, B) => new { A, B }, x => x.A.title == x.B.title)
                .Select()
                .Into()
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
    [publicdata:samples.wikipedia] AS [A]
  INNER JOIN FLATTEN(
  (
    SELECT
      [title],
      [contributor_id] AS [v]
    FROM
      [publicdata:samples.wikipedia]
  ), [v]) AS [B] ON ([A.title] = [B.title])
)
".TrimSmart());
        }
    }
}
