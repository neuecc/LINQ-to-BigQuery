using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Tests.Functions
{
    [TestClass]
    public class WindowTest
    {
        [TableName("[publicdata:samples.shakespeare]")]
        class Shakespeare
        {
            public string word { get; set; }
            public string corpus { get; set; }
            public int word_count { get; set; }
        }

        [TestMethod]
        public void CUME_DIST()
        {
            var context = new BigQuery.Linq.BigQueryContext();

            var query1 = context.From<Shakespeare>()
                .Where(x => x.corpus == "othello" && BqFunc.Length(x.word) > 10)
                .Select(x => new
                {
                    x.word,
                    x.word_count,
                    cume_dist = BqFunc.CumulativeDistribution(x)
                        .PartitionBy(y => y.corpus)
                        .OrderByDescending(y => y.word_count)
                        .Value
                })
                .Limit(5)
                .ToString();

            query1.Is(@"
SELECT
  [word],
  [word_count],
  CUME_DIST() OVER (PARTITION BY [corpus] ORDER BY [word_count] DESC) AS [cume_dist]
FROM
  [publicdata:samples.shakespeare]
WHERE
  (([corpus] = 'othello') AND (LENGTH([word]) > 10))
LIMIT 5
".TrimSmart());
        }

        [TestMethod]
        public void DenseRANK()
        {
            var context = new BigQuery.Linq.BigQueryContext();

            var query1 = context.From<Shakespeare>()
                .Where(x => x.corpus == "othello" && BqFunc.Length(x.word) > 10)
                .Select(x => new
                {
                    x.word,
                    x.word_count,
                    dense_rank = BqFunc.DenseRank(x)
                        .PartitionBy(y => y.corpus)
                        .OrderByDescending(y => y.word_count)
                        .Value
                })
                .Limit(5)
                .ToString();

            query1.Is(@"
SELECT
  [word],
  [word_count],
  DENSE_RANK() OVER (PARTITION BY [corpus] ORDER BY [word_count] DESC) AS [dense_rank]
FROM
  [publicdata:samples.shakespeare]
WHERE
  (([corpus] = 'othello') AND (LENGTH([word]) > 10))
LIMIT 5
".TrimSmart());
        }

        [TestMethod]
        public void Lag()
        {
            var context = new BigQuery.Linq.BigQueryContext();

            var query1 = context.From<Shakespeare>()
                .Where(x => x.corpus == "othello" && BqFunc.Length(x.word) > 10)
                .Select(x => new
                {
                    x.word,
                    x.word_count,
                    lag = BqFunc.Lag(x, y => y.word, 1)
                        .PartitionBy(y => y.corpus)
                        .OrderByDescending(y => y.word_count)
                        .Value
                })
                .Limit(5)
                .ToString();

            query1.Is(@"
SELECT
  [word],
  [word_count],
  LAG([word], 1) OVER (PARTITION BY [corpus] ORDER BY [word_count] DESC) AS [lag]
FROM
  [publicdata:samples.shakespeare]
WHERE
  (([corpus] = 'othello') AND (LENGTH([word]) > 10))
LIMIT 5
".TrimSmart());

            var query2 = context.From<Shakespeare>()
                .Where(x => x.corpus == "othello" && BqFunc.Length(x.word) > 10)
                .Select(x => new
                {
                    x.word,
                    x.word_count,
                    lag = BqFunc.Lag(x, y => y.word, 1, "defv")
                        .PartitionBy(y => y.corpus)
                        .OrderByDescending(y => y.word_count)
                        .Value
                })
                .Limit(5)
                .ToString();

            query2.Is(@"
SELECT
  [word],
  [word_count],
  LAG([word], 1, 'defv') OVER (PARTITION BY [corpus] ORDER BY [word_count] DESC) AS [lag]
FROM
  [publicdata:samples.shakespeare]
WHERE
  (([corpus] = 'othello') AND (LENGTH([word]) > 10))
LIMIT 5
".TrimSmart());
        }

        [TestMethod]
        public void Lead()
        {
            var context = new BigQuery.Linq.BigQueryContext();

            var query1 = context.From<Shakespeare>()
                .Where(x => x.corpus == "othello" && BqFunc.Length(x.word) > 10)
                .Select(x => new
                {
                    x.word,
                    x.word_count,
                    lead = BqFunc.Lead(x, y => y.word, 1)
                        .PartitionBy(y => y.corpus)
                        .OrderByDescending(y => y.word_count)
                        .Value
                })
                .Limit(5)
                .ToString();

            query1.Is(@"
SELECT
  [word],
  [word_count],
  LEAD([word], 1) OVER (PARTITION BY [corpus] ORDER BY [word_count] DESC) AS [lead]
FROM
  [publicdata:samples.shakespeare]
WHERE
  (([corpus] = 'othello') AND (LENGTH([word]) > 10))
LIMIT 5
".TrimSmart());

            var query2 = context.From<Shakespeare>()
                .Where(x => x.corpus == "othello" && BqFunc.Length(x.word) > 10)
                .Select(x => new
                {
                    x.word,
                    x.word_count,
                    lead = BqFunc.Lead(x, y => y.word, 1, "defv")
                        .PartitionBy(y => y.corpus)
                        .OrderByDescending(y => y.word_count)
                        .Value
                })
                .Limit(5)
                .ToString();

            query2.Is(@"
SELECT
  [word],
  [word_count],
  LEAD([word], 1, 'defv') OVER (PARTITION BY [corpus] ORDER BY [word_count] DESC) AS [lead]
FROM
  [publicdata:samples.shakespeare]
WHERE
  (([corpus] = 'othello') AND (LENGTH([word]) > 10))
LIMIT 5
".TrimSmart());
        }

        [TestMethod]
        public void NthValue()
        {
            var context = new BigQuery.Linq.BigQueryContext();

            var query1 = context.From<Shakespeare>()
                .Where(x => x.corpus == "othello" && BqFunc.Length(x.word) > 10)
                .Select(x => new
                {
                    x.word,
                    x.word_count,
                    nth = BqFunc.NthValue(x, y => y.word, 1)
                        .PartitionBy(y => y.corpus)
                        .OrderByDescending(y => y.word_count)
                        .Value
                })
                .Limit(5)
                .ToString();

            query1.Is(@"
SELECT
  [word],
  [word_count],
  NTH_VALUE([word], 1) OVER (PARTITION BY [corpus] ORDER BY [word_count] DESC) AS [nth]
FROM
  [publicdata:samples.shakespeare]
WHERE
  (([corpus] = 'othello') AND (LENGTH([word]) > 10))
LIMIT 5
".TrimSmart());
        }

        [TestMethod]
        public void Ntile()
        {
            var context = new BigQuery.Linq.BigQueryContext();

            var query1 = context.From<Shakespeare>()
                .Where(x => x.corpus == "othello" && BqFunc.Length(x.word) > 10)
                .Select(x => new
                {
                    x.word,
                    x.word_count,
                    ntile = BqFunc.Ntile(x, 2)
                        .PartitionBy(y => y.corpus)
                        .OrderByDescending(y => y.word_count)
                        .Value
                })
                .Limit(5)
                .ToString();

            query1.Is(@"
SELECT
  [word],
  [word_count],
  NTILE(2) OVER (PARTITION BY [corpus] ORDER BY [word_count] DESC) AS [ntile]
FROM
  [publicdata:samples.shakespeare]
WHERE
  (([corpus] = 'othello') AND (LENGTH([word]) > 10))
LIMIT 5
".TrimSmart());
        }

        [TestMethod]
        public void PercentRank()
        {
            var context = new BigQuery.Linq.BigQueryContext();

            var query1 = context.From<Shakespeare>()
                .Where(x => x.corpus == "othello" && BqFunc.Length(x.word) > 10)
                .Select(x => new
                {
                    x.word,
                    x.word_count,
                    prank = BqFunc.PercentRank(x)
                        .PartitionBy(y => y.corpus)
                        .OrderByDescending(y => y.word_count)
                        .Value
                })
                .Limit(5)
                .ToString();

            query1.Is(@"
SELECT
  [word],
  [word_count],
  PERCENT_RANK() OVER (PARTITION BY [corpus] ORDER BY [word_count] DESC) AS [prank]
FROM
  [publicdata:samples.shakespeare]
WHERE
  (([corpus] = 'othello') AND (LENGTH([word]) > 10))
LIMIT 5
".TrimSmart());
        }

        [TestMethod]
        public void Percentile()
        {
            var context = new BigQuery.Linq.BigQueryContext();

            var query1 = context.From<Shakespeare>()
                .Where(x => x.corpus == "othello" && BqFunc.Length(x.word) > 10)
                .Select(x => new
                {
                    x.word,
                    x.word_count,
                    p_cont = BqFunc.PercentileCont(x, 0.5)
                        .PartitionBy(y => y.corpus)
                        .OrderByDescending(y => y.word_count)
                        .Value
                })
                .Limit(5)
                .ToString();

            query1.Is(@"
SELECT
  [word],
  [word_count],
  PERCENTILE_CONT(0.5) OVER (PARTITION BY [corpus] ORDER BY [word_count] DESC) AS [p_cont]
FROM
  [publicdata:samples.shakespeare]
WHERE
  (([corpus] = 'othello') AND (LENGTH([word]) > 10))
LIMIT 5
".TrimSmart());

            var query2 = context.From<Shakespeare>()
                .Where(x => x.corpus == "othello" && BqFunc.Length(x.word) > 10)
                .Select(x => new
                {
                    x.word,
                    x.word_count,
                    p_disc = BqFunc.PercentileDisc(x, 0.5)
                        .PartitionBy(y => y.corpus)
                        .OrderByDescending(y => y.word_count)
                        .Value
                })
                .Limit(5)
                .ToString();

            query2.Is(@"
SELECT
  [word],
  [word_count],
  PERCENTILE_DISC(0.5) OVER (PARTITION BY [corpus] ORDER BY [word_count] DESC) AS [p_disc]
FROM
  [publicdata:samples.shakespeare]
WHERE
  (([corpus] = 'othello') AND (LENGTH([word]) > 10))
LIMIT 5
".TrimSmart());
        }

        [TestMethod]
        public void Rank()
        {
            var context = new BigQuery.Linq.BigQueryContext();

            var query1 = context.From<Shakespeare>()
                .Where(x => x.corpus == "othello" && BqFunc.Length(x.word) > 10)
                .Select(x => new
                {
                    x.word,
                    x.word_count,
                    rank = BqFunc.Rank(x)
                        .PartitionBy(y => y.corpus)
                        .OrderByDescending(y => y.word_count)
                        .Value
                })
                .Limit(5)
                .ToString();

            query1.Is(@"
SELECT
  [word],
  [word_count],
  RANK() OVER (PARTITION BY [corpus] ORDER BY [word_count] DESC) AS [rank]
FROM
  [publicdata:samples.shakespeare]
WHERE
  (([corpus] = 'othello') AND (LENGTH([word]) > 10))
LIMIT 5
".TrimSmart());
        }

        [TestMethod]
        public void RatioToReport()
        {
            var context = new BigQuery.Linq.BigQueryContext();

            var query1 = context.From<Shakespeare>()
                .Where(x => x.corpus == "othello" && BqFunc.Length(x.word) > 10)
                .Select(x => new
                {
                    x.word,
                    x.word_count,
                    r_to_r = BqFunc.RatioToReport(x, y => y.word_count)
                        .PartitionBy(y => y.corpus)
                        .OrderByDescending(y => y.word_count)
                        .Value
                })
                .Limit(5)
                .ToString();

            query1.Is(@"
SELECT
  [word],
  [word_count],
  RATIO_TO_REPORT([word_count]) OVER (PARTITION BY [corpus] ORDER BY [word_count] DESC) AS [r_to_r]
FROM
  [publicdata:samples.shakespeare]
WHERE
  (([corpus] = 'othello') AND (LENGTH([word]) > 10))
LIMIT 5
".TrimSmart());
        }

        [TestMethod]
        public void RatioToReport2()
        {

        }

        [TestMethod]
        public void Rownumber()
        {
            var context = new BigQuery.Linq.BigQueryContext();

            var query1 = context.From<Shakespeare>()
                .Where(x => x.corpus == "othello" && BqFunc.Length(x.word) > 10)
                .Select(x => new
                {
                    x.word,
                    x.word_count,
                    row_num = BqFunc.RowNumber(x)
                        .PartitionBy(y => y.corpus)
                        .OrderByDescending(y => y.word_count)
                        .Value
                })
                .Limit(5)
                .ToString();

            query1.Is(@"
SELECT
  [word],
  [word_count],
  ROW_NUMBER() OVER (PARTITION BY [corpus] ORDER BY [word_count] DESC) AS [row_num]
FROM
  [publicdata:samples.shakespeare]
WHERE
  (([corpus] = 'othello') AND (LENGTH([word]) > 10))
LIMIT 5
".TrimSmart());
        }

        [TestMethod]
        public void DescendingOrderingOnMultipleColumnsWhenPartitioning()
        {
            var context = new BigQuery.Linq.BigQueryContext();

            var query1 = context.From<Shakespeare>()
                .Where(x => x.corpus == "othello" && BqFunc.Length(x.word) > 10)
                .Select(x => new
                {
                    x.word,
                    x.word_count,
                    row_num = BqFunc.RowNumber(x)
                        .PartitionBy(y => y.corpus)
                        .OrderBy(y => y.word)
                        .ThenByDescending(y => y.word_count)
                        .Value
                })
                .Limit(5)
                .ToString();

            query1.Is(@"
SELECT
  [word],
  [word_count],
  ROW_NUMBER() OVER (PARTITION BY [corpus] ORDER BY [word], [word_count] DESC) AS [row_num]
FROM
  [publicdata:samples.shakespeare]
WHERE
  (([corpus] = 'othello') AND (LENGTH([word]) > 10))
LIMIT 5
".TrimSmart());
        }

        [TestMethod]
        public void DescendingOrderingOnMultipleColumns()
        {
            var context = new BigQuery.Linq.BigQueryContext();

            var query1 = context.From<Shakespeare>()
                .Where(x => x.corpus == "othello" && BqFunc.Length(x.word) > 10)
                .Select(x => new
                {
                    x.word,
                    x.word_count,
                    row_num = BqFunc.RowNumber(x)
                        .OrderByDescending(y => y.word)
                        .ThenBy(y => y.word_count)
                        .ThenByDescending(y => y.corpus)
                        .Value
                })
                .Limit(5)
                .ToString();

            query1.Is(@"
SELECT
  [word],
  [word_count],
  ROW_NUMBER() OVER (ORDER BY [word] DESC, [word_count], [corpus] DESC) AS [row_num]
FROM
  [publicdata:samples.shakespeare]
WHERE
  (([corpus] = 'othello') AND (LENGTH([word]) > 10))
LIMIT 5
".TrimSmart());
        }
    }
}
