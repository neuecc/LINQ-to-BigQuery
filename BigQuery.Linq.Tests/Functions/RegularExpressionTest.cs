using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Tests.Functions
{
    [TestClass]
    public class RegularExpressionTest
    {
        [TableName("publicdata:samples.shakespeare")]
        class Shakespeare
        {
            public string word { get; set; }
        }

        public BigQueryContext Ctx { get { return new BigQueryContext(); } }

        [TestMethod]
        public void RegexpMatch()
        {
            Ctx.From<Shakespeare>()
                .Where(x => BqFunc.RegexpMatch(x.word, @"\w\w'\w\w"))
                .Select(x => new { x.word, count = BqFunc.Count(x.word) })
                .GroupBy(x => x.word)
                .OrderByDescending(x => x.count)
                .Limit(3)
                .ToString()
                .Is(@"
SELECT
  [word],
  COUNT([word]) AS [count]
FROM
  [publicdata:samples.shakespeare]
WHERE
  REGEXP_MATCH([word], r'\w\w\'\w\w')
GROUP BY
  [word]
ORDER BY
  [count] DESC
LIMIT 3
".TrimSmart());
        }

        [TestMethod]
        public void RegexpExtract()
        {
            Ctx.From<Shakespeare>()
                .Select(x => new { fragment = BqFunc.RegexpExtract(x.word, @"(\w\w'\w\w)") })
                .GroupBy(x => x.fragment)
                .OrderBy(x => x.fragment)
                .Limit(3)
                .ToString()
                .Is(@"
SELECT
  REGEXP_EXTRACT([word], r'(\w\w\'\w\w)') AS [fragment]
FROM
  [publicdata:samples.shakespeare]
GROUP BY
  [fragment]
ORDER BY
  [fragment]
LIMIT 3
".TrimSmart());
        }

        [TestMethod]
        public void Replace()
        {
            Ctx.Select(() => BqFunc.RegexpReplace("Hello", "lo", "p")).ToFlatSql().Is(@"SELECT REGEXP_REPLACE('Hello', r'lo', 'p')");
        }
    }
}
