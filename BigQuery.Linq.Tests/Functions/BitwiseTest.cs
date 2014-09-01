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
            Ctx.Select<int>(() => (x + y) & z).ToSql().Is("SELECT (([x] + [y]) & [z])");
            Ctx.Select<int>(() => x | y).ToSql().Is("SELECT ([x] | [y])");
            Ctx.Select<int>(() => x ^ y).ToSql().Is("SELECT ([x] ^ [y])");
            Ctx.Select<int>(() => x << (y + z)).ToSql().Is("SELECT ([x] << ([y] + [z]))");
            Ctx.Select<int>(() => (x + y) >> z).ToSql().Is("SELECT (([x] + [y]) >> [z])");
        }

        [TestMethod]
        public void Not()
        {
            var x = int.Parse("1");
            var y = int.Parse("1");
            Ctx.Select<int>(() => ~x).ToSql().Is("SELECT ~[x]");

            // not bitwise but test...
            Ctx.Select<bool>(() => !(x == y)).ToSql().Is("SELECT NOT ([x] = [y])");
        }
    }
}
