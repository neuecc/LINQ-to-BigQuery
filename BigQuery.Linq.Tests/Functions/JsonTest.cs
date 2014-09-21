using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Tests.Functions
{
    [TestClass]
    public class JsonTest
    {

        public BigQueryContext Ctx { get { return new BigQueryContext(); } }

        [TestMethod]
        public void Json()
        {
            Ctx.Select(() => new { str = BqFunc.JsonExtract("{\"a\":1, \"b\": [4, 5]}", "$.b") }).ToFlatSql()
                .Is(@"SELECT JSON_EXTRACT('{""a"":1, ""b"": [4, 5]}', '$.b') AS [str]");
            Ctx.Select(() => new { str = BqFunc.JsonExtractScalar(@"{""a"": [""x"", {""b"":3}]}", "$.a[1].b") }).ToFlatSql()
                .Is(@"SELECT JSON_EXTRACT_SCALAR('{""a"": [""x"", {""b"":3}]}', '$.a[1].b') AS [str]");
        }
    }
}
