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
        }
    }
}
