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
        public BigQueryContext Ctx { get { return new BigQueryContext(); } }

        [TestMethod]
        public void RegexpMatch()
        {
            var word = "";
            Ctx.Select<bool>(() => BqFunc.RegexpMatch(word, @"\w\w\'\w\w"))
                .ToFlatSql()
                .Is(@"SELECT REGEXP_MATCH([word], r'\w\w\'\w\w')");
        }
    }
}
