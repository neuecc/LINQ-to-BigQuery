using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Tests.Functions
{
    [TestClass]
    public class AggreagteTest
    {
        [TableName("[publicdata:samples.shakespeare]")]
        class Shakespeare
        {
            public string word { get; set; }
            public string corpus { get; set; }
            public long word_count { get; set; }
            public long corpus_date { get; set; }
        }
        [TableName("[publicdata:samples.natality]")]
        class Natality
        {
            public bool cigarette_use { get; set; }
            public bool is_male { get; set; }
            public string state { get; set; }
            public long year { get; set; }
            public long weight_pounds { get; set; }
            public long mother_age { get; set; }
        }

        IFromBigQueryable<Shakespeare> shakespear = new BigQueryContext().From<Shakespeare>();

        [TestMethod]
        public void Average()
        {
            shakespear.Select(x => BqFunc.Average(x.word_count)).ToString()
                .Is(@"
SELECT
  AVG([word_count])
FROM
  [publicdata:samples.shakespeare]
".TrimSmart());
        }

        [TestMethod]
        public void Corr()
        {
            shakespear.Select(x => BqFunc.Correlation(x.word_count, x.corpus_date)).ToString()
                .Is(@"
SELECT
  CORR([word_count], [corpus_date])
FROM
  [publicdata:samples.shakespeare]
".TrimSmart());
        }

        [TestMethod]
        public void Count()
        {
            shakespear.Select(x => new
            {
                A = BqFunc.Count(),
                B = BqFunc.Count(x.corpus_date),
                C = BqFunc.CountDistinct(x.word_count),
                D = BqFunc.CountDistinct(x.word_count, 10000)
            }).ToString().Is(@"
SELECT
  COUNT(*) AS [A],
  COUNT([corpus_date]) AS [B],
  COUNT(DISTINCT [word_count]) AS [C],
  COUNT(DISTINCT [word_count], 10000) AS [D]
FROM
  [publicdata:samples.shakespeare]
".TrimSmart());
        }

        [TestMethod]
        public void Covariance()
        {
            shakespear.Select(x => new
          {
              A = BqFunc.CovariancePopulation(x.word_count, x.corpus_date),
              B = BqFunc.CovarianceSample(x.word_count, x.corpus_date)
          }).ToString().Is(@"
SELECT
  COVAR_POP([word_count], [corpus_date]) AS [A],
  COVAR_SAMP([word_count], [corpus_date]) AS [B]
FROM
  [publicdata:samples.shakespeare]
".TrimSmart());
        }

        [TestMethod]
        public void Variance()
        {
            shakespear.Select(x => new
          {
              A = BqFunc.VariancePopulation(x.word_count),
              B = BqFunc.VarianceSample(x.word_count),
              C = BqFunc.Variance(x.word_count)
          }).ToString().Is(@"
SELECT
  VAR_POP([word_count]) AS [A],
  VAR_SAMP([word_count]) AS [B],
  VARIANCE([word_count]) AS [C]
FROM
  [publicdata:samples.shakespeare]
".TrimSmart());
        }

        [TestMethod]
        public void StandardDeviation()
        {
            shakespear.Select(x => new
            {
                A = BqFunc.StandardDeviationPopulation(x.word_count),
                B = BqFunc.StandardDeviationSample(x.word_count),
                C = BqFunc.StandardDeviation(x.word_count)
            }).ToString().Is(@"
SELECT
  STDDEV_POP([word_count]) AS [A],
  STDDEV_SAMP([word_count]) AS [B],
  STDDEV([word_count]) AS [C]
FROM
  [publicdata:samples.shakespeare]
".TrimSmart());
        }

        [TestMethod]
        public void First()
        {
            shakespear.Select(x => BqFunc.First(x.word_count)).ToString().Is(@"
SELECT
  FIRST([word_count])
FROM
  [publicdata:samples.shakespeare]
".TrimSmart());
        }

        [TestMethod]
        public void GroupCount()
        {

            shakespear.Select(x => new
            {
                A = BqFunc.GroupConcat(x.word),
                B = BqFunc.GroupConcat(x.word, "hoge"),
            }).ToString().Is(@"
SELECT
  GROUP_CONCAT([word]) AS [A],
  GROUP_CONCAT([word], 'hoge') AS [B]
FROM
  [publicdata:samples.shakespeare]
".TrimSmart());
        }

        [Ignore]
        [TestMethod]
        public void Last()
        {
            // not implemented yet.
            Assert.Fail();
        }

        [Ignore]
        [TestMethod]
        public void Nest()
        {
            // not implemented yet.
            Assert.Fail();
        }

        [TestMethod]
        public void MaxMin()
        {
            shakespear.Select(x => new
            {
                A = BqFunc.Max(x.word),
                B = BqFunc.Max(x.word_count),
                C = BqFunc.Min(x.word),
                D = BqFunc.Min(x.word_count)
            }).ToString().Is(@"
SELECT
  MAX([word]) AS [A],
  MAX([word_count]) AS [B],
  MIN([word]) AS [C],
  MIN([word_count]) AS [D]
FROM
  [publicdata:samples.shakespeare]
".TrimSmart());
        }

        [TestMethod]
        public void Nth()
        {
            var context = new BigQueryContext();
            var r = context.From<wikipedia>()
                .Where(x => x.contributor_ip != null)
                .Select(x => new
                {
                    clientIpNum = BqFunc.Integer(BqFunc.ParseIP(x.contributor_ip)),
                    classB = BqFunc.Integer(BqFunc.ParseIP(x.contributor_ip) / (256 * 256))
                })
                .AsSubquery()
                .Join(JoinType.InnerEach, context.From<geolite_city_bq_b2b>(), (a, b) => new { a, b }, x => x.a.classB == x.b.classB)
                .Where(x => BqFunc.Between(x.a.clientIpNum, x.b.startIpNum, x.b.endIpNum) && x.b.city != "")
                .Select(x => new
                {
                    c = BqFunc.Count(),
                    x.b.city,
                    x.b.countryLabel,
                    lat = BqFunc.Nth(1, x.b.latitude),
                    lng = BqFunc.Nth(1, x.b.longitude)
                })
                .GroupBy(x => new { x.city, x.countryLabel })
                .OrderByDescending(x => x.c)
                .ToString();

            r.Is(@"
SELECT
  COUNT(*) AS [c],
  [b.city] AS [city],
  [b.countryLabel] AS [countryLabel],
  NTH(1, [b.latitude]) AS [lat],
  NTH(1, [b.longitude]) AS [lng]
FROM
(
  SELECT
    INTEGER(PARSE_IP([contributor_ip])) AS [clientIpNum],
    INTEGER((PARSE_IP([contributor_ip]) / 65536)) AS [classB]
  FROM
    [publicdata:samples.wikipedia]
  WHERE
    ([contributor_ip] IS NOT NULL)
) AS [a]
INNER JOIN EACH
  [fh-bigquery:geocode.geolite_city_bq_b2b] AS [b] ON ([a.classB] = [b.classB])
WHERE
  (([a.clientIpNum] BETWEEN [b.startIpNum] AND [b.endIpNum]) AND ([b.city] != ''))
GROUP BY
  [city],
  [countryLabel]
ORDER BY
  [c] DESC
".TrimSmart());

        }

        [TestMethod]
        public void Quantiles()
        {
            shakespear.Select(x => new
            {
                A = BqFunc.Quantiles(x.word),
                B = BqFunc.Quantiles(x.word_count, 10),
            }).ToString().Is(@"
SELECT
  QUANTILES([word]) AS [A],
  QUANTILES([word_count], 10) AS [B]
FROM
  [publicdata:samples.shakespeare]
".TrimSmart());
        }

        [TestMethod]
        public void Sum()
        {
            new BigQueryContext().Select(() => new
            {
                A = 10,
                B = 10.5,
                C = BqFunc.Timestamp("2013-12-12 01:01:01")
            })
            .AsSubquery()
            .Select(x => new
            {
                S1 = BqFunc.Sum(x.A),
                S2 = BqFunc.Sum(x.B),
                S3 = BqFunc.Sum(x.C),
            }).ToString().Is(@"
SELECT
  SUM([A]) AS [S1],
  SUM([B]) AS [S2],
  SUM([C]) AS [S3]
FROM
(
  SELECT
    10 AS [A],
    10.5 AS [B],
    TIMESTAMP('2013-12-12 01:01:01') AS [C]
)
".TrimSmart());
        }

        [TestMethod]
        public void Top()
        {
            shakespear.Select(x => new
            {
                A = BqFunc.Top(x.word),
                B = BqFunc.Count(),
            }).ToString().Is(@"
SELECT
  TOP([word]) AS [A],
  COUNT(*) AS [B]
FROM
  [publicdata:samples.shakespeare]
".TrimSmart());
        }

        [TestMethod]
        public void Advanced1()
        {
            var context = new BigQueryContext();
            context.From<Natality>()
                .Where(x => x.year == 2003 && x.state == "OH")
                .Select(x => new
                {
                    x.cigarette_use,
                    baby_weight = BqFunc.Average(x.weight_pounds),
                    baby_weight_stdev = BqFunc.StandardDeviation(x.weight_pounds),
                    mother_age = BqFunc.Average(x.mother_age)
                })
                .GroupBy(x => x.cigarette_use)
                .ToString()
                .Is(@"
SELECT
  [cigarette_use],
  AVG([weight_pounds]) AS [baby_weight],
  STDDEV([weight_pounds]) AS [baby_weight_stdev],
  AVG([mother_age]) AS [mother_age]
FROM
  [publicdata:samples.natality]
WHERE
  (([year] = 2003) AND ([state] = 'OH'))
GROUP BY
  [cigarette_use]
".TrimSmart());
        }

        [TestMethod]
        public void Advanced2()
        {
            var context = new BigQueryContext();
            context.From<Natality>()
                .Where(x => x.state != "")
                .Select(x => new
                {
                    x.state,
                    sex = (x.is_male) ? "Male" : "Female",
                    cnt = BqFunc.Count()
                })
                .GroupBy(x => new { x.state, x.sex })
                .Having(x => x.cnt > 3000000)
                .OrderByDescending(x => x.cnt)
                .ToString()
                .Is(@"
SELECT
  [state],
  IF([is_male], 'Male', 'Female') AS [sex],
  COUNT(*) AS [cnt]
FROM
  [publicdata:samples.natality]
WHERE
  ([state] != '')
GROUP BY
  [state],
  [sex]
HAVING
  ([cnt] > 3000000)
ORDER BY
  [cnt] DESC
".TrimSmart());
        }
    }
}
