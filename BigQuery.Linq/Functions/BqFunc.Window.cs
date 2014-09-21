using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    public static partial class BqFunc
    {
        // https://developers.google.com/bigquery/query-reference#windowfunctions

        // Window functions enable calculations on a specific partition, or "window", of a result set. Each window function expects an OVER clause that specifies the partition, in the following syntax:
        // OVER ([PARTITION BY <expr>] [ORDER BY <expr>])

        /// <summary>
        /// Returns a double that indicates the cumulative distribution of a value in a group of values, calculated using the formula &lt;number of rows preceding or tied with the current row&gt; / &lt;total rows&gt;.
        /// <para>Tied values return the same cumulative distribution value.</para>
        /// <para>This window function requires ORDER BY in the OVER clause.</para>
        /// </summary>
        [WindowFunction]
        public static FirstWindowFunction<T> CumulativeDistribution<T>(T fieldSource)
        {
            return new FirstWindowFunction<T>("CUME_DIST", "");
        }

        /// <summary>
        /// Returns the integer rank of a value in a group of values. The rank is calculated based on comparisons with other values in the group.
        /// <para>Tied values display as the same rank. The rank of the next value is incremented by 1. For example, if two values tie for rank 2, the next ranked value is 3. If you prefer a gap in the ranking list, use rank().</para>
        /// <para>This window function requires ORDER BY in the OVER clause.</para>
        /// </summary>
        [WindowFunction]
        public static FirstWindowFunction<T> DenseRank<T>(T fieldSource)
        {
            return new FirstWindowFunction<T>("DENSE_RANK", "");
        }

        /// <summary>
        /// Returns the value of [expr] for the row located [offset] rows before the current row. If the row doesn't exist, [default_value] returns.
        /// </summary>
        [WindowFunction]
        public static FirstWindowFunction<T> Lag<T, TK>(T fieldSource, Expression<Func<T, TK>> expr, int offset)
        {
            var arg1 = BigQueryTranslateVisitor.BuildQuery(0, 0, expr);
            return new FirstWindowFunction<T>("LAG", arg1 + ", " + offset);
        }

        /// <summary>
        /// Returns the value of [expr] for the row located [offset] rows before the current row. If the row doesn't exist, [default_value] returns.
        /// </summary>
        [WindowFunction]
        public static FirstWindowFunction<T> Lag<T, TK>(T fieldSource, Expression<Func<T, TK>> expr, int offset, TK defaultValue)
        {
            var arg1 = BigQueryTranslateVisitor.BuildQuery(0, 0, expr);
            return new FirstWindowFunction<T>("LAG", arg1 + ", " + offset + ", " + DataTypeFormatter.Format(defaultValue));
        }

        /// <summary>
        /// Returns the value of [expr] for the row located [offset] rows after the current row. If the row doesn't exist, [default_value] returns.
        /// </summary>
        [WindowFunction]
        public static FirstWindowFunction<T> Lead<T, TK>(T fieldSource, Expression<Func<T, TK>> expr, int offset)
        {
            var arg1 = BigQueryTranslateVisitor.BuildQuery(0, 0, expr);
            return new FirstWindowFunction<T>("LEAD", arg1 + ", " + offset);
        }

        /// <summary>
        /// Returns the value of [expr] for the row located [offset] rows after the current row. If the row doesn't exist, [default_value] returns.
        /// </summary>
        [WindowFunction]
        public static FirstWindowFunction<T> Lead<T, TK>(T fieldSource, Expression<Func<T, TK>> expr, int offset, TK defaultValue)
        {
            var arg1 = BigQueryTranslateVisitor.BuildQuery(0, 0, expr);
            return new FirstWindowFunction<T>("LEAD", arg1 + ", " + offset + ", " + DataTypeFormatter.Format(defaultValue));
        }

        /// <summary>
        /// Returns the value of [expr] at position [n] of the window frame, where [n] is a one-based index.
        /// </summary>
        [WindowFunction]
        public static FirstWindowFunction<T> NthValue<T, TK>(T fieldSource, Expression<Func<T, TK>> expr, int n)
        {
            var arg1 = BigQueryTranslateVisitor.BuildQuery(0, 0, expr);
            return new FirstWindowFunction<T>("NTH_VALUE", arg1 + ", " + n);
        }

        /// <summary>
        /// Divides a sequence of rows into [num_buckets] buckets and assigns a corresponding bucket number, as an integer, with each row. The ntile() function assigns the bucket numbers as equally as possible and returns a value from 1 to [num_buckets] for each row.
        /// </summary>
        [WindowFunction]
        public static FirstWindowFunction<T> Ntile<T>(T fieldSource, int numBuckets)
        {
            return new FirstWindowFunction<T>("NTILE", numBuckets.ToString());
        }

        /// <summary>
        /// Returns the rank of the current row, relative to the other rows in the partition. Returned values range between 0 and 1, inclusively. The first value returned is 0.0.
        /// <para>This window function requires ORDER BY in the OVER clause.</para>
        /// </summary>
        [WindowFunction]
        public static FirstWindowFunction<T> PercentRank<T>(T fieldSource)
        {
            return new FirstWindowFunction<T>("PERCENT_RANK", "");
        }

        /// <summary>
        /// Returns values that are based upon linear interpolation between the values of the group, after ordering them per the ORDER BY clause.
        /// <para>[percentile] must be between 0 and 1.</para>
        /// <para>This window function requires ORDER BY in the OVER clause.</para>
        /// </summary>
        [WindowFunction]
        public static FirstWindowFunction<T> PercentileCont<T>(T fieldSource, double percentile)
        {
            return new FirstWindowFunction<T>("PERCENTILE_CONT", percentile.ToString());
        }

        /// <summary>
        /// Returns the value with the smallest cumulative distribution that's greater or equal to [percentile].
        /// <para>[percentile] must be between 0 and 1.</para>
        /// <para>This window function requires ORDER BY in the OVER clause.</para>
        /// </summary>
        [WindowFunction]
        public static FirstWindowFunction<T> PercentileDisc<T>(T fieldSource, double percentile)
        {
            return new FirstWindowFunction<T>("PERCENTILE_DISC", percentile.ToString());
        }

        /// <summary>
        /// <para>Returns the integer rank of a value in a group of values. The rank is calculated based on comparisons with other values in the group.</para>
        /// <para>Tied values display as the same rank. The rank of the next value is incremented according to how many tied values occurred before it. For example, if two values tie for rank 2, the next ranked value is 4, not 3. If you prefer no gaps in the ranking list, use dense_rank().</para>
        /// <para>This window function requires ORDER BY in the OVER clause.</para>
        /// </summary>
        [WindowFunction]
        public static FirstWindowFunction<T> Rank<T>(T fieldSource)
        {
            return new FirstWindowFunction<T>("RANK", "");
        }

        /// <summary>
        /// Returns the ratio of each value to the sum of the values, as a double between 0 and 1.
        /// </summary>
        [WindowFunction]
        public static FirstWindowFunction<T> RatioToReport<T, TK>(T fieldSource, Expression<Func<T, TK>> columnSelector)
        {
            var arg1 = BigQueryTranslateVisitor.BuildQuery(0, 0, columnSelector);
            return new FirstWindowFunction<T>("RATIO_TO_REPORT", arg1);
        }

        /// <summary>
        /// Returns the current row number of the query result, starting with 1.
        /// </summary>
        [WindowFunction]
        public static FirstWindowFunction<T> RowNumber<T>(T fieldSource)
        {
            return new FirstWindowFunction<T>("ROW_NUMBER", "");
        }
    }

    public sealed class FirstWindowFunction<TSource>
    {
        readonly string methodName;
        readonly string argument;

        internal FirstWindowFunction(string methodName, string argument)
        {
            this.methodName = methodName;
            this.argument = argument;
        }

        [WindowFunction]
        public SecondWindowFunction<TSource, TPartitionKey> PartitionBy<TPartitionKey>(Expression<Func<TSource, TPartitionKey>> partitionKeySelector)
        {
            return new SecondWindowFunction<TSource, TPartitionKey>(methodName, argument, partitionKeySelector);
        }

        [WindowFunction]
        public FullWindowFunction<TSource, TSource, TOrderKey> OrderBy<TOrderKey>(Expression<Func<TSource, TOrderKey>> keySelector)
        {
            return new FullWindowFunction<TSource, TSource, TOrderKey>(methodName, argument, null, keySelector, false);
        }

        [WindowFunction]
        public FullWindowFunction<TSource, TSource, TOrderKey> OrderByDescending<TOrderKey>(Expression<Func<TSource, TOrderKey>> keySelector)
        {
            return new FullWindowFunction<TSource, TSource, TOrderKey>(methodName, argument, null, keySelector, true);
        }

        public override string ToString()
        {
            var translator = new BigQueryTranslateVisitor();
            var s = string.Format("{0}({1}) OVER ()", methodName, argument);

            return s;
        }
    }

    public sealed class SecondWindowFunction<TSource, TPartitionKey>
    {
        readonly string methodName;
        readonly string argument;
        readonly Expression<Func<TSource, TPartitionKey>> partitionKeySelector;

        internal SecondWindowFunction(string methodName, string argument, Expression<Func<TSource, TPartitionKey>> partitionKeySelector)
        {
            this.methodName = methodName;
            this.argument = argument;
            this.partitionKeySelector = partitionKeySelector;
        }

        [WindowFunction]
        public FullWindowFunction<TSource, TPartitionKey, TOrderKey> OrderBy<TOrderKey>(Expression<Func<TSource, TOrderKey>> keySelector)
        {
            return new FullWindowFunction<TSource, TPartitionKey, TOrderKey>(methodName, argument, partitionKeySelector, keySelector, false);
        }

        [WindowFunction]
        public FullWindowFunction<TSource, TPartitionKey, TOrderKey> OrderByDescending<TOrderKey>(Expression<Func<TSource, TOrderKey>> keySelector)
        {
            return new FullWindowFunction<TSource, TPartitionKey, TOrderKey>(methodName, argument, partitionKeySelector, keySelector, true);
        }

        public override string ToString()
        {
            var translator = new BigQueryTranslateVisitor();
            var expr1 = translator.VisitAndClearBuffer(partitionKeySelector);
            var s = string.Format("{0}({1}) OVER (PARTITION BY {2})", methodName, argument, expr1);

            return s;
        }
    }

    public sealed class FullWindowFunction<TSource, TPartitionKey, TOrderKey>
    {
        readonly string methodName;
        readonly string argument;
        readonly Expression<Func<TSource, TPartitionKey>> partitionKeySelector;
        readonly Expression<Func<TSource, TOrderKey>> orderKeySelector;
        readonly bool isDescending;

        internal FullWindowFunction(string methodName, string argument, Expression<Func<TSource, TPartitionKey>> partitionKeySelector, Expression<Func<TSource, TOrderKey>> orderKeySelector, bool isDescending)
        {
            this.methodName = methodName;
            this.argument = argument;
            this.partitionKeySelector = partitionKeySelector;
            this.orderKeySelector = orderKeySelector;
            this.isDescending = isDescending;
        }

        public override string ToString()
        {
            var translator = new BigQueryTranslateVisitor();
            if (partitionKeySelector != null)
            {
                var expr1 = translator.VisitAndClearBuffer(partitionKeySelector);
                var expr2 = translator.VisitAndClearBuffer(orderKeySelector);
                var s = string.Format("{0}({1}) OVER (PARTITION BY {2} ORDER BY {3}{4})", methodName, argument, expr1, expr2, (isDescending) ? " DESC" : "");
                return s;
            }
            else
            {
                var expr2 = translator.VisitAndClearBuffer(orderKeySelector);
                var s = string.Format("{0}({1}) OVER (ORDER BY {2}{3})", methodName, argument, expr2, (isDescending) ? " DESC" : "");
                return s;
            }
        }
    }
}