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
  (6 + (5 - 1)) AS [A],
  (6 - (4 + 1)) AS [B],
  (6 * (5 - 1)) AS [C],
  (6 / (2 + 2)) AS [D],
  (6 % (2 + 2)) AS [E]
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