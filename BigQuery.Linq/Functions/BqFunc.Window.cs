using System;
using System.Collections.Generic;
using System.Globalization;
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
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, double> CumulativeDistribution<T>(T fieldSource)
        {
            return new FirstWindowFunction<T, double>("CUME_DIST", "");
        }

        /// <summary>
        /// Returns the integer rank of a value in a group of values. The rank is calculated based on comparisons with other values in the group.
        /// <para>Tied values display as the same rank. The rank of the next value is incremented by 1. For example, if two values tie for rank 2, the next ranked value is 3. If you prefer a gap in the ranking list, use rank().</para>
        /// <para>This window function requires ORDER BY in the OVER clause.</para>
        /// </summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, long> DenseRank<T>(T fieldSource)
        {
            return new FirstWindowFunction<T, long>("DENSE_RANK", "");
        }

        /// <summary>
        /// Returns the value of [expr] for the row located [offset] rows before the current row. If the row doesn't exist, [default_value] returns.
        /// </summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, TColumn> Lag<T, TColumn>(T fieldSource, Expression<Func<T, TColumn>> columnSelector, int offset)
        {
            var arg1 = BigQueryTranslateVisitor.BuildQuery(0, 0, columnSelector);
            return new FirstWindowFunction<T, TColumn>("LAG", arg1 + ", " + offset);
        }

        /// <summary>
        /// Returns the value of [expr] for the row located [offset] rows before the current row. If the row doesn't exist, [default_value] returns.
        /// </summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, TColumn> Lag<T, TColumn>(T fieldSource, string columnName, int offset)
        {
            var arg1 = columnName.EscapeBq();
            return new FirstWindowFunction<T, TColumn>("LAG", columnName + ", " + offset);
        }

        /// <summary>
        /// Returns the value of [expr] for the row located [offset] rows before the current row. If the row doesn't exist, [default_value] returns.
        /// </summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, TColumn> Lag<T, TColumn>(T fieldSource, Expression<Func<T, TColumn>> columnSelector, int offset, TColumn defaultValue)
        {
            var arg1 = BigQueryTranslateVisitor.BuildQuery(0, 0, columnSelector);
            return new FirstWindowFunction<T, TColumn>("LAG", arg1 + ", " + offset + ", " + DataTypeFormatter.Format(defaultValue));
        }

        /// <summary>
        /// Returns the value of [expr] for the row located [offset] rows before the current row. If the row doesn't exist, [default_value] returns.
        /// </summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, TColumn> Lag<T, TColumn>(T fieldSource, string columnName, int offset, TColumn defaultValue)
        {
            var arg1 = columnName.EscapeBq();
            return new FirstWindowFunction<T, TColumn>("LAG", arg1 + ", " + offset + ", " + DataTypeFormatter.Format(defaultValue));
        }

        /// <summary>
        /// Returns the value of [expr] for the row located [offset] rows after the current row. If the row doesn't exist, [default_value] returns.
        /// </summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, TColumn> Lead<T, TColumn>(T fieldSource, Expression<Func<T, TColumn>> columnSelector, int offset)
        {
            var arg1 = BigQueryTranslateVisitor.BuildQuery(0, 0, columnSelector);
            return new FirstWindowFunction<T, TColumn>("LEAD", arg1 + ", " + offset);
        }

        /// <summary>
        /// Returns the value of [expr] for the row located [offset] rows after the current row. If the row doesn't exist, [default_value] returns.
        /// </summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, TColumn> Lead<T, TColumn>(T fieldSource, string columnName, int offset)
        {
            var arg1 = columnName.EscapeBq();
            return new FirstWindowFunction<T, TColumn>("LEAD", arg1 + ", " + offset);
        }

        /// <summary>
        /// Returns the value of [expr] for the row located [offset] rows after the current row. If the row doesn't exist, [default_value] returns.
        /// </summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, TColumn> Lead<T, TColumn>(T fieldSource, Expression<Func<T, TColumn>> columnSelector, int offset, TColumn defaultValue)
        {
            var arg1 = BigQueryTranslateVisitor.BuildQuery(0, 0, columnSelector);
            return new FirstWindowFunction<T, TColumn>("LEAD", arg1 + ", " + offset + ", " + DataTypeFormatter.Format(defaultValue));
        }

        /// <summary>
        /// Returns the value of [expr] for the row located [offset] rows after the current row. If the row doesn't exist, [default_value] returns.
        /// </summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, TColumn> Lead<T, TColumn>(T fieldSource, string columnName, int offset, TColumn defaultValue)
        {
            var arg1 = columnName.EscapeBq();
            return new FirstWindowFunction<T, TColumn>("LEAD", arg1 + ", " + offset + ", " + DataTypeFormatter.Format(defaultValue));
        }

        /// <summary>
        /// Returns the value of [expr] at position [n] of the window frame, where [n] is a one-based index.
        /// </summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, TColumn> NthValue<T, TColumn>(T fieldSource, Expression<Func<T, TColumn>> columnSelector, int n)
        {
            var arg1 = BigQueryTranslateVisitor.BuildQuery(0, 0, columnSelector);
            return new FirstWindowFunction<T, TColumn>("NTH_VALUE", arg1 + ", " + n);
        }

        /// <summary>
        /// Returns the value of [expr] at position [n] of the window frame, where [n] is a one-based index.
        /// </summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, TColumn> NthValue<T, TColumn>(T fieldSource, string columnName, int n)
        {
            var arg1 = columnName.EscapeBq();
            return new FirstWindowFunction<T, TColumn>("NTH_VALUE", arg1 + ", " + n);
        }

        /// <summary>
        /// Divides a sequence of rows into [num_buckets] buckets and assigns a corresponding bucket number, as an integer, with each row. The ntile() function assigns the bucket numbers as equally as possible and returns a value from 1 to [num_buckets] for each row.
        /// </summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, long> Ntile<T>(T fieldSource, int numBuckets)
        {
            return new FirstWindowFunction<T, long>("NTILE", numBuckets.ToString(CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Returns the rank of the current row, relative to the other rows in the partition. Returned values range between 0 and 1, inclusively. The first value returned is 0.0.
        /// <para>This window function requires ORDER BY in the OVER clause.</para>
        /// </summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, double> PercentRank<T>(T fieldSource)
        {
            return new FirstWindowFunction<T, double>("PERCENT_RANK", "");
        }

        /// <summary>
        /// Returns values that are based upon linear interpolation between the values of the group, after ordering them per the ORDER BY clause.
        /// <para>[percentile] must be between 0 and 1.</para>
        /// <para>This window function requires ORDER BY in the OVER clause.</para>
        /// </summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, long> PercentileCont<T>(T fieldSource, double percentile)
        {
            return new FirstWindowFunction<T, long>("PERCENTILE_CONT", percentile.ToString(CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Returns the value with the smallest cumulative distribution that's greater or equal to [percentile].
        /// <para>[percentile] must be between 0 and 1.</para>
        /// <para>This window function requires ORDER BY in the OVER clause.</para>
        /// </summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, long> PercentileDisc<T>(T fieldSource, double percentile)
        {
            return new FirstWindowFunction<T, long>("PERCENTILE_DISC", percentile.ToString(CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// <para>Returns the integer rank of a value in a group of values. The rank is calculated based on comparisons with other values in the group.</para>
        /// <para>Tied values display as the same rank. The rank of the next value is incremented according to how many tied values occurred before it. For example, if two values tie for rank 2, the next ranked value is 4, not 3. If you prefer no gaps in the ranking list, use dense_rank().</para>
        /// <para>This window function requires ORDER BY in the OVER clause.</para>
        /// </summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, long> Rank<T>(T fieldSource)
        {
            return new FirstWindowFunction<T, long>("RANK", "");
        }

        /// <summary>
        /// Returns the ratio of each value to the sum of the values, as a double between 0 and 1.
        /// </summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, double> RatioToReport<T, TColumn>(T fieldSource, Expression<Func<T, TColumn>> columnSelector)
        {
            var arg1 = BigQueryTranslateVisitor.BuildQuery(0, 0, columnSelector);
            return new FirstWindowFunction<T, double>("RATIO_TO_REPORT", arg1);
        }

        /// <summary>
        /// Returns the ratio of each value to the sum of the values, as a double between 0 and 1.
        /// </summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, double> RatioToReport<T>(T fieldSource, string columnName)
        {
            return new FirstWindowFunction<T, double>("RATIO_TO_REPORT", columnName.EscapeBq());
        }

        /// <summary>
        /// Returns the current row number of the query result, starting with 1.
        /// </summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, long> RowNumber<T>(T fieldSource)
        {
            return new FirstWindowFunction<T, long>("ROW_NUMBER", "");
        }

        // more window functions it's Undocumented(at 204-09-23)
        // but already added from September 18, 2013
        // http://googlecloudplatform.blogspot.jp/2013/09/google-bigquery-goes-real-time-with-streaming-inserts-time-based-queries-and-more.html

        /// <summary>Window and analytics function.</summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, TColumn> FirstValue<T, TColumn>(T fieldSource, Expression<Func<T, TColumn>> columnSelector)
        {
            var arg1 = BigQueryTranslateVisitor.BuildQuery(0, 0, columnSelector);
            return new FirstWindowFunction<T, TColumn>("FIRST_VALUE", arg1);
        }

        /// <summary>Window and analytics function.</summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, TColumn> FirstValue<T, TColumn>(T fieldSource, string columnName, TColumn typeInferenceDummy)
        {
            return new FirstWindowFunction<T, TColumn>("FIRST_VALUE", columnName.EscapeBq());
        }

        /// <summary>Window and analytics function.</summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, TColumn> LastValue<T, TColumn>(T fieldSource, Expression<Func<T, TColumn>> columnSelector)
        {
            var arg1 = BigQueryTranslateVisitor.BuildQuery(0, 0, columnSelector);
            return new FirstWindowFunction<T, TColumn>("LAST_VALUE", arg1);
        }

        /// <summary>Window and analytics function.</summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, TColumn> LastValue<T, TColumn>(T fieldSource, string columnName, TColumn typeInferenceDummy)
        {
            return new FirstWindowFunction<T, TColumn>("LAST_VALUE", columnName.EscapeBq());
        }

        /// <summary>Window and analytics function.</summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, long> Sum<T, TColumn>(T fieldSource, Expression<Func<T, long>> columnSelector)
        {
            var arg1 = BigQueryTranslateVisitor.BuildQuery(0, 0, columnSelector);
            return new FirstWindowFunction<T, long>("SUM", arg1);
        }

        /// <summary>Window and analytics function.</summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, double> Sum<T, TColumn>(T fieldSource, Expression<Func<T, double>> columnSelector)
        {
            var arg1 = BigQueryTranslateVisitor.BuildQuery(0, 0, columnSelector);
            return new FirstWindowFunction<T, double>("SUM", arg1);
        }

        /// <summary>Window and analytics function.</summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, double> Sum<T>(T fieldSource, string columnName)
        {
            return new FirstWindowFunction<T, double>("SUM", columnName.EscapeBq());
        }

        /// <summary>Window and analytics function.</summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, long> Count<T, TColumn>(T fieldSource, Expression<Func<T, TColumn>> columnSelector)
        {
            var arg1 = BigQueryTranslateVisitor.BuildQuery(0, 0, columnSelector);
            return new FirstWindowFunction<T, long>("COUNT", arg1);
        }

        /// <summary>Window and analytics function.</summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, long> Count<T, TColumn>(T fieldSource, string columnName)
        {
            return new FirstWindowFunction<T, long>("COUNT", columnName.EscapeBq());
        }

        /// <summary>Window and analytics function.</summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, double> Average<T, TColumn>(T fieldSource, Expression<Func<T, TColumn>> columnSelector)
        {
            var arg1 = BigQueryTranslateVisitor.BuildQuery(0, 0, columnSelector);
            return new FirstWindowFunction<T, double>("AVG", arg1);
        }

        /// <summary>Window and analytics function.</summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, double> Average<T, TColumn>(T fieldSource, string columnName)
        {
            return new FirstWindowFunction<T, double>("AVG", columnName.EscapeBq());
        }

        /// <summary>Window and analytics function.</summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, TColumn> Min<T, TColumn>(T fieldSource, Expression<Func<T, TColumn>> columnSelector)
        {
            var arg1 = BigQueryTranslateVisitor.BuildQuery(0, 0, columnSelector);
            return new FirstWindowFunction<T, TColumn>("MIN", arg1);
        }

        /// <summary>Window and analytics function.</summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, TColumn> Min<T, TColumn>(T fieldSource, string columnName, TColumn typeInferenceDummy)
        {
            return new FirstWindowFunction<T, TColumn>("MIN", columnName.EscapeBq());
        }

        /// <summary>Window and analytics function.</summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, TColumn> Max<T, TColumn>(T fieldSource, Expression<Func<T, TColumn>> columnSelector)
        {
            var arg1 = BigQueryTranslateVisitor.BuildQuery(0, 0, columnSelector);
            return new FirstWindowFunction<T, TColumn>("MAX", arg1);
        }

        /// <summary>Window and analytics function.</summary>
        [WindowFunctionAlert]
        public static FirstWindowFunction<T, TColumn> Max<T, TColumn>(T fieldSource, string columnName, TColumn typeInferenceDummy)
        {
            return new FirstWindowFunction<T, TColumn>("MAX", columnName.EscapeBq());
        }
    }

    public sealed class FirstWindowFunction<TSource, TResult>
    {
        readonly string methodName;
        readonly string argument;

        internal FirstWindowFunction(string methodName, string argument)
        {
            this.methodName = methodName;
            this.argument = argument;
        }

        [WindowFunction]
        public TResult Value { get { throw new InvalidOperationException("Property is marker for Query Analyze. Can't call directly."); } }

        [WindowFunctionAlert]
        public SecondWindowFunction<TSource, TResult, TPartitionKey> PartitionBy<TPartitionKey>(Expression<Func<TSource, TPartitionKey>> partitionKeySelector)
        {
            return new SecondWindowFunction<TSource, TResult, TPartitionKey>(methodName, argument, partitionKeySelector);
        }

        [WindowFunctionAlert]
        public FullWindowFunction<TSource, TResult, TSource> OrderBy<TOrderKey>(Expression<Func<TSource, TOrderKey>> keySelector)
        {
            return new FullWindowFunction<TSource, TResult, TSource>(methodName, argument, null, keySelector, false);
        }

        [WindowFunctionAlert]
        public FullWindowFunction<TSource, TResult, TSource> OrderByDescending<TOrderKey>(Expression<Func<TSource, TOrderKey>> keySelector)
        {
            return new FullWindowFunction<TSource, TResult, TSource>(methodName, argument, null, keySelector, true);
        }

        public override string ToString()
        {
            var translator = new BigQueryTranslateVisitor();
            var s = string.Format("{0}({1}) OVER ()", methodName, argument);

            return s;
        }
    }

    public sealed class SecondWindowFunction<TSource, TResult, TPartitionKey>
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
        public TResult Value { get { throw new InvalidOperationException("Property is marker for Query Analyze. Can't call directly."); } }

        [WindowFunctionAlert]
        public FullWindowFunction<TSource, TResult, TPartitionKey> OrderBy<TOrderKey>(Expression<Func<TSource, TOrderKey>> keySelector)
        {
            return new FullWindowFunction<TSource, TResult, TPartitionKey>(methodName, argument, partitionKeySelector, keySelector, false);
        }

        [WindowFunctionAlert]
        public FullWindowFunction<TSource, TResult, TPartitionKey> OrderByDescending<TOrderKey>(Expression<Func<TSource, TOrderKey>> keySelector)
        {
            return new FullWindowFunction<TSource, TResult, TPartitionKey>(methodName, argument, partitionKeySelector, keySelector, true);
        }

        public override string ToString()
        {
            var translator = new BigQueryTranslateVisitor();
            var expr1 = translator.VisitAndClearBuffer(partitionKeySelector);
            var s = string.Format("{0}({1}) OVER (PARTITION BY {2})", methodName, argument, expr1);

            return s;
        }
    }

    public sealed class FullWindowFunction<TSource, TResult, TPartitionKey>
    {
        readonly string methodName;
        readonly string argument;
        readonly Expression<Func<TSource, TPartitionKey>> partitionKeySelector;
        readonly Expression orderKeySelector;
        readonly bool isDescending;
        readonly FullWindowFunction<TSource, TResult, TPartitionKey> parent;

        internal FullWindowFunction(string methodName, string argument, Expression<Func<TSource, TPartitionKey>> partitionKeySelector, /* Expression<Func<TSource, TOrderKey>> */ Expression orderKeySelector, bool isDescending)
        {
            this.methodName = methodName;
            this.argument = argument;
            this.partitionKeySelector = partitionKeySelector;
            this.orderKeySelector = orderKeySelector;
            this.isDescending = isDescending;
            this.parent = null;
        }

        internal FullWindowFunction(FullWindowFunction<TSource, TResult, TPartitionKey> parent, Expression orderKeySelector, bool isDescending)
        {
            this.parent = parent;
            this.orderKeySelector = orderKeySelector;
            this.isDescending = isDescending;
        }

        [WindowFunction]
        public TResult Value { get { throw new InvalidOperationException("Property is marker for Query Analyze. Can't call directly."); } }

        [WindowFunctionAlert]
        public FullWindowFunction<TSource, TResult, TPartitionKey> ThenBy<TOrderKey>(Expression<Func<TSource, TOrderKey>> keySelector)
        {
            return new FullWindowFunction<TSource, TResult, TPartitionKey>(this, keySelector, false);
        }

        [WindowFunctionAlert]
        public FullWindowFunction<TSource, TResult, TPartitionKey> ThenByDescending<TOrderKey>(Expression<Func<TSource, TOrderKey>> keySelector)
        {
            return new FullWindowFunction<TSource, TResult, TPartitionKey>(this, keySelector, true);
        }

        public override string ToString()
        {
            var translator = new BigQueryTranslateVisitor();

            var orderByBuilder = new List<string>();
            FullWindowFunction<TSource, TResult, TPartitionKey> self = this;
            while (self.parent != null)
            {
                orderByBuilder.Add(translator.VisitAndClearBuffer(self.orderKeySelector) + (self.isDescending ? " DESC" : ""));
                self = self.parent;
            }
            orderByBuilder.Add(translator.VisitAndClearBuffer(self.orderKeySelector) + (self.isDescending ? " DESC" : ""));
            orderByBuilder.Reverse();

            if (self.partitionKeySelector != null)
            {
                var expr1 = translator.VisitAndClearBuffer(self.partitionKeySelector);
                var expr2 = string.Join(", ", orderByBuilder);
                var s = string.Format("{0}({1}) OVER (PARTITION BY {2} ORDER BY {3})", self.methodName, self.argument, expr1, expr2);
                return s;
            }
            else
            {
                var expr2 = string.Join(", ", orderByBuilder);
                var s = string.Format("{0}({1}) OVER (ORDER BY {2})", self.methodName, self.argument, expr2);
                return s;
            }
        }
    }
}