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
            context.Select(() => x == 1000).ToFlatSql().Is("SELECT ([x] = 1000)");
            context.Select(() => x != 1000).ToFlatSql().Is("SELECT ([x] != 1000)");
            context.Select(() => x > 1000).ToFlatSql().Is("SELECT ([x] > 1000)");
            context.Select(() => x < 1000).ToFlatSql().Is("SELECT ([x] < 1000)");
            context.Select(() => x >= 1000).ToFlatSql().Is("SELECT ([x] >= 1000)");
            context.Select(() => x <= 1000).ToFlatSql().Is("SELECT ([x] <= 1000)");
            context.Select(() => y == null).ToFlatSql().Is("SELECT ([y] IS NULL)");
            context.Select(() => y != null).ToFlatSql().Is("SELECT ([y] IS NOT NULL)");

            long? x2 = 1000;
            context.Select(() => x2 == 1000).ToFlatSql().Is("SELECT ([x2] = 1000)");
            context.Select(() => x2.Value == 1000).ToFlatSql().Is("SELECT ([x2] = 1000)");
        }

        [TestMethod]
        public void Between()
        {
            var context = new BigQueryContext();
            var x = 1000;

            context.Select<bool>(() => BqFunc.Between(x, BqFunc.Abs(10), 100)).ToFlatSql()
                .Is("SELECT [x] BETWEEN ABS(10) AND 100");
        }

        [TestMethod]
        public void IfNull()
        {
            var context = new BigQueryContext();

            var x = "hogehoge";
            context.Select<string>(() => x ?? "hugahuga").ToFlatSql()
                .Is("SELECT IFNULL([x], 'hugahuga')");

            context.Select(() => BqFunc.Integer("a") ?? -1L).ToFlatSql()
                .Is("SELECT IFNULL(INTEGER('a'), -1)");
        }

        [TestMethod]
        public void In()
        {
            var context = new BigQueryContext();
            context.Select(() => new { value = 100 })
                .AsSubquery()
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
                .AsSubquery()
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
