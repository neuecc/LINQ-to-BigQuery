using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Tests.Functions
{
    [TestClass]
    public class DateAndTimeTest
    {
        public BigQueryContext Ctx { get { return new BigQueryContext(); } }

        [TestMethod]
        public void Current()
        {
            Ctx.Select<string>(() => BqFunc.CurrentDate()).ToFlatSql().Is("SELECT CURRENT_DATE()");
            Ctx.Select<string>(() => BqFunc.CurrentTime()).ToFlatSql().Is("SELECT CURRENT_TIME()");
            Ctx.Select<DateTimeOffset>(() => BqFunc.CurrentTimestamp()).ToFlatSql().Is("SELECT CURRENT_TIMESTAMP()");
        }

        [TestMethod]
        public void Date()
        {
            Ctx.Select<string>(() => BqFunc.Date(BqFunc.Timestamp("2012-10-01 02:03:04"))).ToFlatSql().Is("SELECT DATE(TIMESTAMP('2012-10-01 02:03:04'))");
            Ctx.Select<DateTimeOffset>(() => BqFunc.DateAdd(BqFunc.Timestamp("2012-10-01 02:03:04"), 5, IntervalUnit.YEAR)).ToFlatSql().Is("SELECT DATE_ADD(TIMESTAMP('2012-10-01 02:03:04'), 5, 'YEAR')");
            Ctx.Select<DateTimeOffset>(() => BqFunc.DateAdd(BqFunc.Timestamp("2012-10-01 02:03:04"), -5, IntervalUnit.YEAR)).ToFlatSql().Is("SELECT DATE_ADD(TIMESTAMP('2012-10-01 02:03:04'), -5, 'YEAR')");
            Ctx.Select<long>(() => BqFunc.DateDiff(BqFunc.Timestamp("2012-10-02 05:23:48"), BqFunc.Timestamp("2011-06-24 12:18:35"))).ToFlatSql().Is("SELECT DATEDIFF(TIMESTAMP('2012-10-02 05:23:48'), TIMESTAMP('2011-06-24 12:18:35'))");
        }
    }
}
