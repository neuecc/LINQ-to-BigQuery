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
        [WindowFunction]
        public static FirstWindowFunction<T> CumulativeDistribution<T>(T fieldSource)
        {
            return new FirstWindowFunction<T>("CUME_DIST", "");
        }

        [WindowFunction]
        public static FirstWindowFunction<T> Lag<T, TK>(T fieldSource, Expression<Func<T, TK>> expr, int offset)
        {
            var arg1 = BigQueryTranslateVisitor.BuildQuery(0, 0, expr);
            return new FirstWindowFunction<T>("LAG", arg1 + ", " + offset);
        }

        [WindowFunction]
        public static FirstWindowFunction<T> Lag<T, TK>(T fieldSource, Expression<Func<T, TK>> expr, int offset, TK defaultValue)
        {
            var arg1 = BigQueryTranslateVisitor.BuildQuery(0, 0, expr);
            return new FirstWindowFunction<T>("LAG", arg1 + ", " + offset + ", " + DataTypeFormatter.Format(defaultValue));
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