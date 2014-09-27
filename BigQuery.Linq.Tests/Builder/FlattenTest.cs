using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace BigQuery.Linq.Tests.Builder
{
    [TestClass]
    public class FlattenTest
    {

        [TableName("publicdata:samples.trigrams")]
        public class trigrams
        {
            public cell cell { get; set; }
        }

        public class cell
        {
            public string value { get; set; }
        }

        [TestMethod]
        public void From()
        {
            new BigQueryContext().From<trigrams>()
                .Flatten(x => x.cell.value)
                .Select()
                .ToString()
                .Is(@"
SELECT
  *
FROM
  (FLATTEN([publicdata:samples.trigrams], [cell.value]))
".TrimSmart());
        }

        [TestMethod]
        public void FromTableDecorator()
        {
            new BigQueryContext().From<trigrams>().WithRange(TimeSpan.FromHours(1))
                .Flatten(x => x.cell.value)
                .Select()
                .ToString()
                .Is(@"
SELECT
  *
FROM
  (FLATTEN([publicdata:samples.trigrams@-3600000-], [cell.value]))
".TrimSmart());

            new BigQueryContext().From<trigrams>().WithSnapshot(TimeSpan.FromHours(1))
                .Flatten(x => x.cell.value)
                .Select()
                .ToString()
                .Is(@"
SELECT
  *
FROM
  (FLATTEN([publicdata:samples.trigrams@-3600000], [cell.value]))
".TrimSmart());
        }

        [TestMethod]
        public void FromTableQuery()
        {

            new BigQueryContext().FromTableQuery<trigrams>("publicdata:samples", x => x.table_id == "trigrams")
                .Flatten(x => x.cell.value)
                .Select()
                .ToString()
                .Is(@"
SELECT
  *
FROM
  (FLATTEN((TABLE_QUERY([publicdata:samples], ""([table_id] = 'trigrams')"")), [cell.value]))
".TrimSmart());
        }

        [TestMethod]
        public void FromSubquery()
        {
            new BigQueryContext().From<trigrams>()
                .Select(x => new { v = x.cell.value })
                .AsSubquery()
                .Flatten(x => x.v)
                .Select()
                .ToString()
                .Is(@"
SELECT
  *
FROM (FLATTEN(
(
  SELECT
    [cell.value] AS [v]
  FROM
    [publicdata:samples.trigrams]
), [v]))
".TrimSmart());
        }

        [TestMethod]
        public void FlattenDigitMaker()
        {
            var digits = new BigQueryContext()
               .Select(() => new { seed = "0123456789" })
               .AsSubquery()
               .Select(x => new { digit = BqFunc.Integer(BqFunc.Split(x.seed, "")) })
               .AsSubquery()
               .Flatten(x => x.digit);

            var d = digits
                .JoinCross(JoinType.Inner, digits, (d1, d2) => new { d1, d2 })
                .Select(x => new { seq = x.d1.digit + (x.d2.digit * 10)})
                .OrderBy(x => x.seq)
                .ToString();

            d.Is(@"
SELECT
  ([d1.digit] + ([d2.digit] * 10)) AS [seq]
FROM (FLATTEN(
(
  SELECT
    INTEGER(SPLIT([seed], '')) AS [digit]
  FROM
  (
    SELECT
      '0123456789' AS [seed]
  )
), [digit])) AS [d1]
CROSS JOIN (FLATTEN(
(
  SELECT
    INTEGER(SPLIT([seed], '')) AS [digit]
  FROM
  (
    SELECT
      '0123456789' AS [seed]
  )
), [digit])) AS [d2]
ORDER BY
  [seq]
".TrimSmart());
        }
    }
}
