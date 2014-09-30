using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Tests.Functions
{
    [TestClass]
    public class BitwiseTest
    {
        public BigQueryContext Ctx { get { return new BigQueryContext(); } }

        [TestMethod]
        public void AndOrEtc()
        {
            var x = int.Parse("1");
            var y = int.Parse("3");
            var z = int.Parse("1");
            Ctx.Select<int>(() => (x + y) & z).ToFlatSql().Is("SELECT ((1 + 3) & 1)");
            Ctx.Select<int>(() => x | y).ToFlatSql().Is("SELECT (1 | 3)");
            Ctx.Select<int>(() => x ^ y).ToFlatSql().Is("SELECT (1 ^ 3)");
            Ctx.Select<int>(() => x << (y + z)).ToFlatSql().Is("SELECT (1 << (3 + 1))");
            Ctx.Select<int>(() => (x + y) >> z).ToFlatSql().Is("SELECT ((1 + 3) >> 1)");
        }

        [TestMethod]
        public void Not()
        {
            var x = int.Parse("1");
            var y = int.Parse("1");
            Ctx.Select<int>(() => ~x).ToFlatSql().Is("SELECT ~1");

            // not bitwise but test...
            Ctx.Select<bool>(() => !(x == y)).ToFlatSql().Is("SELECT NOT (1 = 1)");
        }
    }
}
