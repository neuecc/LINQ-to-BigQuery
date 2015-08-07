using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Linq.Expressions;

namespace BigQuery.Linq.Tests.Builder
{
    [TestClass]
    public class ContextTest
    {
        [TableName("aaa:bbb.ccc")]
        class MyClass
        {
            public string name { get; set; }
            public int age { get; set; }
        }

        [TablePrefix("mydata.people")]
        class People
        {
            public string name { get; set; }
            public int age { get; set; }
        }



        [TestMethod]
        public void From()
        {
            var ctx = new BigQueryContext();

            ctx.From<MyClass>().Select().ToString()
                .Is(@"
SELECT
  *
FROM
  [aaa:bbb.ccc]".TrimSmart());

            AssertEx.Throws<ArgumentException>(() => ctx.From<ContextTest>());

            ctx.From("abc", new { X = default(int), Y = default(string) })
                .Select(x => new { x.X, x.Y })
                .ToString()
                .Is(@"
SELECT
  [X],
  [Y]
FROM
  [abc]
".TrimSmart());

            ctx.From<int>("hogehoge").Select().ToString()
                .Is(@"
SELECT
  *
FROM
  [hogehoge]".TrimSmart());

            ctx.From<int>("[hogehoge]").Select().ToString()
                .Is(@"
SELECT
  *
FROM
  [hogehoge]".TrimSmart());
        }

        [TestMethod]
        public void FromSubQuery()
        {
            var bqcx = new BigQueryContext();

            bqcx.From<MyClass>()
                .Select(x => new { x.age, hoge = 10 })
                .Into()
                .Select(x => new { x.hoge })
                .ToString()
                .Is(@"
SELECT
  [hoge]
FROM
(
  SELECT
    [age],
    10 AS [hoge]
  FROM
    [aaa:bbb.ccc]
)".TrimSmart());
        }

        [TestMethod]
        public void FromDateRange()
        {
            var from = new DateTime(2014, 3, 25, 0, 0, 0, DateTimeKind.Utc);
            var to = from.AddDays(2);
            Expression<Func<DateTimeOffset>> fromExpr = () => BqFunc.Timestamp("2014-03-25");
            Expression<Func<DateTimeOffset>> toExpr = () => BqFunc.Timestamp("2014-03-27");

            var ctx = new BigQueryContext();

            AssertEx.Throws<ArgumentException>(() => ctx.FromDateRange<MyClass>(from, to));

            ctx.FromDateRange<People>(from, to)
                .Where(x => x.age >= 35)
                .Select(x => new { x.name })
                .ToString()
                .Is(@"
SELECT
  [name]
FROM
  TABLE_DATE_RANGE([mydata.people], TIMESTAMP('2014-03-25'), TIMESTAMP('2014-03-27'))
WHERE
  ([age] >= 35)
".TrimSmart());

            ctx.FromDateRange<People>(fromExpr, toExpr)
                .Where(x => x.age >= 35)
                .Select(x => new { x.name })
                .ToString()
                .Is(@"
SELECT
  [name]
FROM
  TABLE_DATE_RANGE([mydata.people], TIMESTAMP('2014-03-25'), TIMESTAMP('2014-03-27'))
WHERE
  ([age] >= 35)
".TrimSmart());

            ctx.FromDateRange("[mydata.people]", new { age = default(int), name = default(string) }, from, to)
                .Where(x => x.age >= 35)
                .Select(x => new { x.name })
                .ToString()
                .Is(@"
SELECT
  [name]
FROM
  TABLE_DATE_RANGE([mydata.people], TIMESTAMP('2014-03-25'), TIMESTAMP('2014-03-27'))
WHERE
  ([age] >= 35)
".TrimSmart());

            ctx.FromDateRange<MyClass>("[mydata.people]", from, to)
                .Where(x => x.age >= 35)
                .Select(x => new { x.name })
                .ToString()
                .Is(@"
SELECT
  [name]
FROM
  TABLE_DATE_RANGE([mydata.people], TIMESTAMP('2014-03-25'), TIMESTAMP('2014-03-27'))
WHERE
  ([age] >= 35)
".TrimSmart());
        }

        [TestMethod]
        public void FromDateRangeStrict()
        {
            var from = new DateTime(2014, 3, 25, 0, 0, 0, DateTimeKind.Utc);
            var to = from.AddDays(2);
            Expression<Func<DateTimeOffset>> fromExpr = () => BqFunc.Timestamp("2014-03-25");
            Expression<Func<DateTimeOffset>> toExpr = () => BqFunc.Timestamp("2014-03-27");

            var ctx = new BigQueryContext();

            AssertEx.Throws<ArgumentException>(() => ctx.FromDateRangeStrict<MyClass>(from, to));

            ctx.FromDateRangeStrict<People>(from, to)
                .Where(x => x.age >= 35)
                .Select(x => new { x.name })
                .ToString()
                .Is(@"
SELECT
  [name]
FROM
  TABLE_DATE_RANGE_STRICT([mydata.people], TIMESTAMP('2014-03-25'), TIMESTAMP('2014-03-27'))
WHERE
  ([age] >= 35)
".TrimSmart());

            ctx.FromDateRangeStrict<People>(fromExpr, toExpr)
                .Where(x => x.age >= 35)
                .Select(x => new { x.name })
                .ToString()
                .Is(@"
SELECT
  [name]
FROM
  TABLE_DATE_RANGE_STRICT([mydata.people], TIMESTAMP('2014-03-25'), TIMESTAMP('2014-03-27'))
WHERE
  ([age] >= 35)
".TrimSmart());

            ctx.FromDateRangeStrict("[mydata.people]", new { age = default(int), name = default(string) }, from, to)
                .Where(x => x.age >= 35)
                .Select(x => new { x.name })
                .ToString()
                .Is(@"
SELECT
  [name]
FROM
  TABLE_DATE_RANGE_STRICT([mydata.people], TIMESTAMP('2014-03-25'), TIMESTAMP('2014-03-27'))
WHERE
  ([age] >= 35)
".TrimSmart());

            ctx.FromDateRangeStrict<MyClass>("[mydata.people]", from, to)
                .Where(x => x.age >= 35)
                .Select(x => new { x.name })
                .ToString()
                .Is(@"
SELECT
  [name]
FROM
  TABLE_DATE_RANGE_STRICT([mydata.people], TIMESTAMP('2014-03-25'), TIMESTAMP('2014-03-27'))
WHERE
  ([age] >= 35)
".TrimSmart());
        }

        [TestMethod]
        public void FromTableQuery()
        {
            var ctx = new BigQueryContext();

            ctx.FromTableQuery<MyClass>("mydata", x => x.table_id.Contains("oo") && BqFunc.Length(x.table_id) >= 4)
                .Select(x => new { x.age })
                .ToString()
                .Is(@"
SELECT
  [age]
FROM
  TABLE_QUERY([mydata], ""([table_id] CONTAINS 'oo' AND (LENGTH([table_id]) >= 4))"")".TrimSmart());

            ctx.FromTableQuery("mydata", new { age = 0 }, x => x.table_id.Contains("oo") && BqFunc.Length(x.table_id) >= 4)
                .Select(x => new { x.age })
                .ToString()
                .Is(@"
SELECT
  [age]
FROM
  TABLE_QUERY([mydata], ""([table_id] CONTAINS 'oo' AND (LENGTH([table_id]) >= 4))"")".TrimSmart());

            // more example
            ctx.FromTableQuery<MetaTable>("mydata", x => BqFunc.RegexpMatch(x.table_id, @"^boo[\d]{3,5}"))
                .Select(x => new { x.table_id })
                .ToString()
                .Is(@"
SELECT
  [table_id]
FROM
  TABLE_QUERY([mydata], ""REGEXP_MATCH([table_id], r'^boo[\d]{3,5}')"")".TrimSmart());
        }
    }
}
