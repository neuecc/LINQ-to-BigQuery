using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Tests.Functions
{
    [TestClass]
    public class StringTest
    {
        public BigQueryContext Ctx { get { return new BigQueryContext(); } }

        [TestMethod]
        public void Contains()
        {
            Ctx.Select<bool>(() => "aaa".Contains("bbb")).ToFlatSql().Is("SELECT 'aaa' CONTAINS 'bbb'");
        }

    }
}
