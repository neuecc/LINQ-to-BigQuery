using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Tests.Functions
{
    [TestClass]
    public class MathematicalTest
    {
        [TableName("weather_geo.table")]
        class WeatherGeoTable
        {
            public double mean_temp { get; set; }
            public double min_temperature { get; set; }
            public double max_temperature { get; set; }
            public double lat { get; set; }
            public double @long { get; set; }
            public long year { get; set; }
            public long month { get; set; }
        }

        public BigQueryContext Ctx { get { return new BigQueryContext(); } }

        [TestMethod]
        public void Abs()
        {
            Ctx.Select(() => BqFunc.Abs(-100L)).ToFlatSql().Is("SELECT ABS(-100)");
            Ctx.Select(() => BqFunc.Abs(-100.5)).ToFlatSql().Is("SELECT ABS(-100.5)");
        }

        [TestMethod]
        public void Exp()
        {
            Ctx.Select(() => BqFunc.Exp(100L)).ToFlatSql().Is("SELECT EXP(100)");
            Ctx.Select(() => BqFunc.Exp(100.5)).ToFlatSql().Is("SELECT EXP(100.5)");
        }

        [TestMethod]
        public void Trigonometric()
        {
            Ctx.Select(() => BqFunc.Acos(0.5)).ToFlatSql().Is("SELECT ACOS(0.5)");
            Ctx.Select(() => BqFunc.Acosh(2.4)).ToFlatSql().Is("SELECT ACOSH(2.4)");
            Ctx.Select(() => BqFunc.Asin(1.2)).ToFlatSql().Is("SELECT ASIN(1.2)");
            Ctx.Select(() => BqFunc.Asinh(2.3)).ToFlatSql().Is("SELECT ASINH(2.3)");
            Ctx.Select(() => BqFunc.Atan(2.3)).ToFlatSql().Is("SELECT ATAN(2.3)");
            Ctx.Select(() => BqFunc.Atanh(0.4)).ToFlatSql().Is("SELECT ATANH(0.4)");
            Ctx.Select(() => BqFunc.Cos(2.3)).ToFlatSql().Is("SELECT COS(2.3)");
            Ctx.Select(() => BqFunc.Cosh(2.3)).ToFlatSql().Is("SELECT COSH(2.3)");
            Ctx.Select(() => BqFunc.Sin(2.3)).ToFlatSql().Is("SELECT SIN(2.3)");
            Ctx.Select(() => BqFunc.Sinh(2.3)).ToFlatSql().Is("SELECT SINH(2.3)");
            Ctx.Select(() => BqFunc.Tan(2.3)).ToFlatSql().Is("SELECT TAN(2.3)");
            Ctx.Select(() => BqFunc.Tanh(2.3)).ToFlatSql().Is("SELECT TANH(2.3)");

            Ctx.Select(() => BqFunc.Atan2(2.4, 1.2)).ToFlatSql().Is("SELECT ATAN2(2.4, 1.2)");
        }

        [TestMethod]
        public void CeilFloorRound()
        {
            Ctx.Select(() => BqFunc.Ceil(15.123)).ToFlatSql().Is("SELECT CEIL(15.123)");
            Ctx.Select(() => BqFunc.Floor(15.123)).ToFlatSql().Is("SELECT FLOOR(15.123)");
            Ctx.Select(() => BqFunc.Round(15.123)).ToFlatSql().Is("SELECT ROUND(15.123)");
            Ctx.Select(() => BqFunc.Round(15.123, 2)).ToFlatSql().Is("SELECT ROUND(15.123, 2)");
        }

        [TestMethod]
        public void DegreeRadian()
        {
            Ctx.Select(() => BqFunc.Degrees(1.4)).ToFlatSql().Is("SELECT DEGREES(1.4)");
            Ctx.Select(() => BqFunc.Radians(80.2140913183152)).ToFlatSql().Is("SELECT RADIANS(80.2140913183152)");
        }

        [TestMethod]
        public void Log()
        {
            Ctx.Select(() => BqFunc.Log(1.4)).ToFlatSql().Is("SELECT LOG(1.4)");
            Ctx.Select(() => BqFunc.Log2(1.4)).ToFlatSql().Is("SELECT LOG2(1.4)");
            Ctx.Select(() => BqFunc.Log10(1.4)).ToFlatSql().Is("SELECT LOG10(1.4)");
        }

        [TestMethod]
        public void PI()
        {
            Ctx.Select(() => BqFunc.PI()).ToFlatSql().Is("SELECT PI()");
        }

        [TestMethod]
        public void Pow()
        {
            Ctx.Select(() => BqFunc.Pow(2, 4)).ToFlatSql().Is("SELECT POW(2, 4)");
        }

        [TestMethod]
        public void Random()
        {
            Ctx.Select(() => BqFunc.Random()).ToFlatSql().Is("SELECT RAND()");
            Ctx.Select(() => BqFunc.Random(1213)).ToFlatSql().Is("SELECT RAND(1213)");
        }

        [TestMethod]
        public void Sqrt()
        {
            Ctx.Select(() => BqFunc.Sqrt(2.4)).ToFlatSql().Is("SELECT SQRT(2.4)");
        }

        [TestMethod]
        public void AdvancedExample1_BoundingBoxQuery()
        {
            Ctx.From<WeatherGeoTable>()
               .Where(x => x.lat / 1000 > 37.46
                        && x.lat / 1000 < 37.65
                        && x.@long / 1000 > -122.5
                        && x.@long / 1000 < -122.3)
               .Select(x => new
               {
                   x.year,
                   x.month,
                   avg_temp = BqFunc.Average(x.mean_temp),
                   min_temp = BqFunc.Min(x.min_temperature),
                   max_temp = BqFunc.Max(x.max_temperature)
               })
               .GroupBy(x => new { x.year, x.month })
               .OrderBy(x => x.year)
               .ThenBy(x => x.month)
               .ToString()
               .Is(@"
SELECT
  [year],
  [month],
  AVG([mean_temp]) AS [avg_temp],
  MIN([min_temperature]) AS [min_temp],
  MAX([max_temperature]) AS [max_temp]
FROM
  [weather_geo.table]
WHERE
  ((((([lat] / 1000) > 37.46) AND (([lat] / 1000) < 37.65)) AND (([long] / 1000) > -122.5)) AND (([long] / 1000) < -122.3))
GROUP BY
  [year],
  [month]
ORDER BY
  [year], [month]
".TrimSmart());
        }

        [TestMethod]
        public void AdvancedExample2_ApproximateBoundingCircleQuery()
        {
            Ctx.From<WeatherGeoTable>()
               .Where(x => x.month == 1)
               .Select(x => new
               {
                   distance = (BqFunc.Acos(BqFunc.Sin(39.737567 * BqFunc.PI() / 180)
                                        * BqFunc.Sin((x.lat / 1000) * BqFunc.PI() / 180)
                                        + BqFunc.Cos(39.737567 * BqFunc.PI() / 180)
                                        * BqFunc.Cos((x.lat / 1000) * BqFunc.PI() / 180)
                                        * BqFunc.Cos((-104.9847179 - (x.@long / 1000)) * BqFunc.PI() / 180)) * 180 / BqFunc.PI())
                                * 60 * 1.1515,
                   temp = BqFunc.Average(x.mean_temp),
                   lat = BqFunc.Average(x.lat / 1000),
                   @long = BqFunc.Average(x.@long / 1000)
               })
               .GroupBy(x => x.distance)
               .Into()
               .Where(x => x.distance < 100)
               .Select(x => new { x.distance, x.lat, x.@long, x.temp })
               .OrderBy(x => x.distance)
               .Limit(100)
               .ToString()
               .Is(@"
SELECT
  [distance],
  [lat],
  [long],
  [temp]
FROM
(
  SELECT
    ((((ACOS(((SIN(((39.737567 * PI()) / 180)) * SIN(((([lat] / 1000) * PI()) / 180))) + ((COS(((39.737567 * PI()) / 180)) * COS(((([lat] / 1000) * PI()) / 180))) * COS((((-104.9847179 - ([long] / 1000)) * PI()) / 180))))) * 180) / PI()) * 60) * 1.1515) AS [distance],
    AVG([mean_temp]) AS [temp],
    AVG(([lat] / 1000)) AS [lat],
    AVG(([long] / 1000)) AS [long]
  FROM
    [weather_geo.table]
  WHERE
    ([month] = 1)
  GROUP BY
    [distance]
)
WHERE
  ([distance] < 100)
ORDER BY
  [distance]
LIMIT 100
".TrimSmart());
        }
    }
}
