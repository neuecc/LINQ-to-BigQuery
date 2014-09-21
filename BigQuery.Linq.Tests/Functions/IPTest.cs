using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Tests.Functions
{
    [TestClass]
    public class IPTest
    {

        public BigQueryContext Ctx { get { return new BigQueryContext(); } }

        [TestMethod]
        public void IP()
        {
            Ctx.Select<string>(() => BqFunc.FormatIP(1)).ToFlatSql()
                .Is(@"SELECT FORMAT_IP(1)");
            Ctx.Select<long>(() => BqFunc.ParseIP("0.0.0.1")).ToFlatSql()
                .Is(@"SELECT PARSE_IP('0.0.0.1')");
            Ctx.Select<string>(() => BqFunc.FormatPackedIP("0123456789@ABCDE")).ToFlatSql()
                .Is(@"SELECT FORMAT_PACKED_IP('0123456789@ABCDE')");
            Ctx.Select<string>(() => BqFunc.ParsePackedIP("3031:3233:3435:3637:3839:4041:4243:4445")).ToFlatSql()
                .Is(@"SELECT PARSE_PACKED_IP('3031:3233:3435:3637:3839:4041:4243:4445')");
        }
    }
}
