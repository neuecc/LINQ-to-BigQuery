using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Tests.Functions
{
    [TestClass]
    public class CastingTest
    {
        public BigQueryContext Ctx { get { return new BigQueryContext(); } }

        [TestMethod]
        public void Boolean()
        {
            Ctx.Select<bool?>(() => BqFunc.Boolean(100)).ToFlatSql().Is(@"SELECT BOOLEAN(100)");
            Ctx.Select<bool?>(() => BqFunc.Boolean(100.1)).ToFlatSql().Is(@"SELECT BOOLEAN(100.1)");
        }

        [TestMethod]
        public void Float()
        {
            Ctx.Select<double?>(() => BqFunc.Float("45.78")).ToFlatSql().Is(@"SELECT FLOAT('45.78')");
            Ctx.Select<double?>(() => BqFunc.Float("aaa")).ToFlatSql().Is(@"SELECT FLOAT('aaa')");
            Ctx.Select<double?>(() => BqFunc.Float(10)).ToFlatSql().Is(@"SELECT FLOAT(10)");
            Ctx.Select<double?>(() => BqFunc.Float((string)null)).ToFlatSql().Is(@"SELECT FLOAT(NULL)");
        }

        [TestMethod]
        public void HexString()
        {
            Ctx.Select<string>(() => BqFunc.HexString(10)).ToFlatSql().Is(@"SELECT HEX_STRING(10)");
            Ctx.Select<string>(() => BqFunc.HexString(10.1)).ToFlatSql().Is(@"SELECT HEX_STRING(10.1)");
            var d = new Nullable<double>();
            Ctx.Select<string>(() => BqFunc.HexString(d.Value)).ToFlatSql().Is(@"SELECT HEX_STRING(NULL)");
        }

        [TestMethod]
        public void Integer()
        {
            Ctx.Select<long?>(() => BqFunc.Integer("45")).ToFlatSql().Is(@"SELECT INTEGER('45')");
            Ctx.Select<long?>(() => BqFunc.Integer("45.49")).ToFlatSql().Is(@"SELECT INTEGER('45.49')");
            Ctx.Select<long?>(() => BqFunc.Integer(100)).ToFlatSql().Is(@"SELECT INTEGER(100)");
            Ctx.Select<long?>(() => BqFunc.Integer(100.32)).ToFlatSql().Is(@"SELECT INTEGER(100.32)");
            Ctx.Select<long?>(() => BqFunc.Integer((string)null)).ToFlatSql().Is(@"SELECT INTEGER(NULL)");
        }

        [TestMethod]
        public void String()
        {
            Ctx.Select<string>(() => BqFunc.String(10)).ToFlatSql().Is(@"SELECT STRING(10)");
            Ctx.Select<string>(() => BqFunc.String(45.49)).ToFlatSql().Is(@"SELECT STRING(45.49)");
        }

        [TestMethod]
        public void Cast()
        {
            Ctx.Select<int>(() => BqFunc.Cast<int>(10)).ToFlatSql().Is(@"SELECT CAST(10 AS INTEGER)");
            Ctx.Select<string>(() => BqFunc.Cast<string>(10)).ToFlatSql().Is(@"SELECT CAST(10 AS STRING)");
            Ctx.Select<double>(() => BqFunc.Cast<double>(10)).ToFlatSql().Is(@"SELECT CAST(10 AS FLOAT)");
            Ctx.Select<bool>(() => BqFunc.Cast<bool>(10)).ToFlatSql().Is(@"SELECT CAST(10 AS BOOLEAN)");
            Ctx.Select<DateTimeOffset>(() => BqFunc.Cast<DateTimeOffset>(10)).ToFlatSql().Is(@"SELECT CAST(10 AS TIMESTAMP)");
            Ctx.Select<DateTime>(() => BqFunc.Cast<DateTime>(10)).ToFlatSql().Is(@"SELECT CAST(10 AS TIMESTAMP)");
        }
    }
}
