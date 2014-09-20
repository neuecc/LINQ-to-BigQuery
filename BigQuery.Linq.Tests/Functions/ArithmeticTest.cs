using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Tests.Functions
{
    [TestClass]
    public class ArithmeticTest
    {
        [TestMethod]
        public void AddSubMulDivModulo()
        {
            var context = new BigQueryContext();

            var one = 1;
            var two = 2;
            // var three = 3;
            var four = 4;
            var five = 5;
            var six = 6;
            context.Select(() => new
            {
                A = six + (five - one),
                B = six - (four + one),
                C = six * (five - one),
                D = six / (two + two),
                E = six % (two + two)
            }).ToString()
                .Is(@"
SELECT
  ([six] + ([five] - [one])) AS [A],
  ([six] - ([four] + [one])) AS [B],
  ([six] * ([five] - [one])) AS [C],
  ([six] / ([two] + [two])) AS [D],
  ([six] % ([two] + [two])) AS [E]
".TrimSmart());
        }


        [TestMethod]
        public void BitCount()
        {
            var context = new BigQueryContext();

            context.Select<long>(() => BqFunc.BitCount(1000)).ToString().TrimFlat()
                .Is("SELECT BIT_COUNT(1000)");
        }
    }
}