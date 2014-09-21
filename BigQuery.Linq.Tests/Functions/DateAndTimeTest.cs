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
        [TableName("publicdata:samples.wikipedia")]
        class Wikipedia
        {
            public string contributor_username { get; set; }
            public string comment { get; set; }
            public long timestamp { get; set; }
            public long month { get; set; }
        }

        public BigQueryContext Ctx { get { return new BigQueryContext(); } }

        [TestMethod]
        public void Current()
        {
            Ctx.Select(() => new
            {
                A = BqFunc.CurrentDate(),
                B = BqFunc.CurrentTime(),
                C = BqFunc.CurrentTimestamp()
            }).ToString().Is(@"
SELECT
  CURRENT_DATE() AS [A],
  CURRENT_TIME() AS [B],
  CURRENT_TIMESTAMP() AS [C]
".TrimSmart());
        }

        [TestMethod]
        public void Date()
        {
            Ctx.Select(() => new
            {
                A = BqFunc.Date(BqFunc.Timestamp("2012-10-01 02:03:04")),
                A2 = BqFunc.Date(new DateTime(2012, 10, 1, 2, 3, 4, DateTimeKind.Utc)),
                B = BqFunc.DateAdd(BqFunc.Timestamp("2012-10-01 02:03:04"), 5, IntervalUnit.YEAR),
                B2 = BqFunc.DateAdd(new DateTime(2012, 10, 1, 2, 3, 4, DateTimeKind.Utc), 5, IntervalUnit.DAY),
                C = BqFunc.DateAdd(BqFunc.Timestamp("2012-10-01 02:03:04"), -5, IntervalUnit.YEAR),
                C2 = BqFunc.DateAdd(new DateTime(2012, 10, 1, 2, 3, 4, DateTimeKind.Utc), -5, IntervalUnit.SECOND),
                D = BqFunc.DateDiff(BqFunc.Timestamp("2012-10-02 05:23:48"), BqFunc.Timestamp("2011-06-24 12:18:35"))
            }).ToString().Is(@"
SELECT
  DATE(TIMESTAMP('2012-10-01 02:03:04')) AS [A],
  DATE('2012-10-01 02:03:04.000000') AS [A2],
  DATE_ADD(TIMESTAMP('2012-10-01 02:03:04'), 5, 'YEAR') AS [B],
  DATE_ADD('2012-10-01 02:03:04.000000', 5, 'DAY') AS [B2],
  DATE_ADD(TIMESTAMP('2012-10-01 02:03:04'), -5, 'YEAR') AS [C],
  DATE_ADD('2012-10-01 02:03:04.000000', -5, 'SECOND') AS [C2],
  DATEDIFF(TIMESTAMP('2012-10-02 05:23:48'), TIMESTAMP('2011-06-24 12:18:35')) AS [D]
".TrimSmart());
        }

        [TestMethod]
        public void Day()
        {
            Ctx.Select(() => new
            {
                A = BqFunc.Day(BqFunc.Timestamp("2012-10-02 05:23:48")),
                B = BqFunc.DayOfWeek(BqFunc.Timestamp("2012-10-01 02:03:04")),
                C = BqFunc.DayOfYear(BqFunc.Timestamp("2012-10-01 02:03:04")),
            }).ToString().Is(@"
SELECT
  DAY(TIMESTAMP('2012-10-02 05:23:48')) AS [A],
  DAYOFWEEK(TIMESTAMP('2012-10-01 02:03:04')) AS [B],
  DAYOFYEAR(TIMESTAMP('2012-10-01 02:03:04')) AS [C]
".TrimSmart());
        }

        [TestMethod]
        public void FormatUtcUsec()
        {
            Ctx.Select(() => new
            {
                A = BqFunc.FormatUtcUsec(1274259481071200),
            }).ToString().Is(@"
SELECT
  FORMAT_UTC_USEC(1274259481071200) AS [A]
".TrimSmart());
        }

        [TestMethod]
        public void HourMinuteMonthSecondYear()
        {
            Ctx.Select(() => new
            {
                A = BqFunc.Hour(BqFunc.Timestamp("2012-10-02 05:23:48")),
                B = BqFunc.Minute(BqFunc.Timestamp("2012-10-02 05:23:48")),
                C = BqFunc.Month(BqFunc.Timestamp("2012-10-02 05:23:48")),
                D = BqFunc.Second(BqFunc.Timestamp("2012-10-02 05:23:48")),
                E = BqFunc.Year(BqFunc.Timestamp("2012-10-02 05:23:48")),
            }).ToString().Is(@"
SELECT
  HOUR(TIMESTAMP('2012-10-02 05:23:48')) AS [A],
  MINUTE(TIMESTAMP('2012-10-02 05:23:48')) AS [B],
  MONTH(TIMESTAMP('2012-10-02 05:23:48')) AS [C],
  SECOND(TIMESTAMP('2012-10-02 05:23:48')) AS [D],
  YEAR(TIMESTAMP('2012-10-02 05:23:48')) AS [E]
".TrimSmart());
        }

        [TestMethod]
        public void MSecto()
        {
            Ctx.Select<DateTimeOffset>(() => BqFunc.MsecToTimestamp(1349053323000)).ToFlatSql().Is(@"SELECT MSEC_TO_TIMESTAMP(1349053323000)");
        }

        [TestMethod]
        public void Now()
        {
            Ctx.Select<long>(() => BqFunc.Now()).ToFlatSql().Is(@"SELECT NOW()");
        }

        [TestMethod]
        public void ParseUtcUsec()
        {
            Ctx.Select<long>(() => BqFunc.ParseUtcUsec("2012-10-01 02:03:04")).ToFlatSql().Is(@"SELECT PARSE_UTC_USEC('2012-10-01 02:03:04')");
        }

        [TestMethod]
        public void Quater()
        {
            Ctx.Select<long>(() => BqFunc.Quarter(BqFunc.Timestamp("2012-10-01 02:03:04"))).ToFlatSql().Is("SELECT QUARTER(TIMESTAMP('2012-10-01 02:03:04'))");
        }

        [TestMethod]
        public void SecToTimestamp()
        {
            Ctx.Select<DateTimeOffset>(() => BqFunc.SecToTimestamp(1355968987)).ToFlatSql().Is("SELECT SEC_TO_TIMESTAMP(1355968987)");
        }

        [TestMethod]
        public void Strftime()
        {
            Ctx.Select<string>(() => BqFunc.StrftimeUtcUsec(1274259481071200, "%Y-%m-%d")).ToFlatSql().Is("SELECT STRFTIME_UTC_USEC(1274259481071200, '%Y-%m-%d')");
        }

        [TestMethod]
        public void Time()
        {
            Ctx.Select(() => new
            {
                A = BqFunc.Time(BqFunc.Timestamp("2012-10-01 02:03:04")),
                B = BqFunc.Timestamp("2012-10-01 01:02:03"),
                C = BqFunc.TimestampToMsec(BqFunc.Timestamp("2012-10-01 01:02:03")),
                D = BqFunc.TimestampToSec(BqFunc.Timestamp("2012-10-01 01:02:03")),
                E = BqFunc.TimestampToUsec(BqFunc.Timestamp("2012-10-01 01:02:03"))
            }).ToString().Is(@"
SELECT
  TIME(TIMESTAMP('2012-10-01 02:03:04')) AS [A],
  TIMESTAMP('2012-10-01 01:02:03') AS [B],
  TIMESTAMP_TO_MSEC(TIMESTAMP('2012-10-01 01:02:03')) AS [C],
  TIMESTAMP_TO_SEC(TIMESTAMP('2012-10-01 01:02:03')) AS [D],
  TIMESTAMP_TO_USEC(TIMESTAMP('2012-10-01 01:02:03')) AS [E]
".TrimSmart());
        }

        [TestMethod]
        public void USEC_()
        {
            Ctx.Select(() => new
            {
                A = BqFunc.UsecToTimestamp(1349053323000000),
                B = BqFunc.UtcUsecToDay(1274259481071200),
                C = BqFunc.UtcUsecToHour(1274259481071200),
                D = BqFunc.UtcUsecToMonth(1274259481071200),
                E = BqFunc.UtcUsecToWeek(1274259481071200, (long)System.DayOfWeek.Tuesday),
                F = BqFunc.UtcUsecToYear(1274259481071200)
            }).ToString().Is(@"
SELECT
  USEC_TO_TIMESTAMP(1349053323000000) AS [A],
  UTC_USEC_TO_DAY(1274259481071200) AS [B],
  UTC_USEC_TO_HOUR(1274259481071200) AS [C],
  UTC_USEC_TO_MONTH(1274259481071200) AS [D],
  UTC_USEC_TO_WEEK(1274259481071200, 2) AS [E],
  UTC_USEC_TO_YEAR(1274259481071200) AS [F]
".TrimSmart());
        }

        [TestMethod]
        public void AdvancedExample_1()
        {
            Ctx.From<Wikipedia>()
                .Select(x => new
                {
                    top_revision_time = BqFunc.Top(BqFunc.FormatUtcUsec(x.timestamp * 1000000), 5),
                    revision_count = BqFunc.Count()
                })
                .ToString()
                .Is(@"
SELECT
  TOP(FORMAT_UTC_USEC(([timestamp] * 1000000)), 5) AS [top_revision_time],
  COUNT(*) AS [revision_count]
FROM
  [publicdata:samples.wikipedia]
".TrimSmart());
        }

        [TestMethod]
        public void AdvancedExample_2()
        {
            Ctx.From<Wikipedia>()
                .Where(x => (x.contributor_username != "" && x.contributor_username != null)
                         && x.timestamp > 1133395200
                         && x.timestamp < 1157068800)
                .Select(x => new
                {
                    x.contributor_username,
                    month = BqFunc.Left(BqFunc.FormatUtcUsec(BqFunc.UtcUsecToMonth(x.timestamp * 1000000)), 7),
                    total_chars_used = BqFunc.Sum(BqFunc.Length(x.comment))
                })
                .GroupBy(x => new { x.contributor_username, x.month })
                .OrderByDescending(x => x.total_chars_used)
                .ToString()
                .Is(@"
SELECT
  [contributor_username],
  LEFT(FORMAT_UTC_USEC(UTC_USEC_TO_MONTH(([timestamp] * 1000000))), 7) AS [month],
  SUM(LENGTH([comment])) AS [total_chars_used]
FROM
  [publicdata:samples.wikipedia]
WHERE
  (((([contributor_username] != '') AND ([contributor_username] IS NOT NULL)) AND ([timestamp] > 1133395200)) AND ([timestamp] < 1157068800))
GROUP BY
  [contributor_username],
  [month]
ORDER BY
  [total_chars_used] DESC
".TrimSmart());
        }
    }
}
