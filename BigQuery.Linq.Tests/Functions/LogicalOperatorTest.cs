using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Tests.Functions
{
    [TestClass]
    public class LogicalOperatorTest
    {
        public BigQueryContext Ctx { get { return new BigQueryContext(); } }

        [TestMethod]
        public void AndOrNot()
        {
            var v1 = "aaa";
            var v2 = "bbb";
            Ctx.Select<bool>(() => v1 == v2 && v1 == v2).ToSql().Is("SELECT [v1] = [v2] AND [v1] = [v2]");
            Ctx.Select<bool>(() => v1 == v2 || v1 == v2).ToSql().Is("SELECT [v1] = [v2] OR [v1] = [v2]");
            Ctx.Select<bool>(() => !(v1 == v2)).ToSql().Is("SELECT NOT([v1] = [v2])");
        }
    }
}
