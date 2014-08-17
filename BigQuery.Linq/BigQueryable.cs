using BigQuery.Linq.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace BigQuery.Linq
{
    // BigQuery Query Reference : https://developers.google.com/bigquery/query-reference

    // Syntax:
    /*
    SELECT expr1 [[AS] alias1] [, expr2 [[AS] alias2], ...]
        [agg_function(expr3) WITHIN expr4]
        [FROM [(FLATTEN(table_name1|(subselect1)] [, table_name2|(subselect2), ...)]
        [[INNER|LEFT OUTER|CROSS] JOIN [EACH] table_2|(subselect2) [[AS] tablealias2]
          ON join_condition_1 [... AND join_condition_N ...]]+
        [WHERE condition]
        [GROUP [EACH] BY field1|alias1 [, field2|alias2, ...]]
        [HAVING condition]
        [ORDER BY field1|alias1 [DESC|ASC] [, field2|alias2 [DESC|ASC], ...]]
        [LIMIT n]
        ;
     */

    // like query provider
    public interface IBigQueryable
    {
        BigQueryContext QueryContext { get; }

        string ToString();

        string ToString(FormatOption option);

        string ToString(int indentSize, FormatOption option);
        string ToString(int depth, int indentSize, FormatOption option);
    }

    internal abstract class BigQueryable : IBigQueryable
    {
        internal IBigQueryable Parent { get; private set; }

        public BigQueryContext QueryContext { get; private set; }

        protected BigQueryable(IBigQueryable parent)
        {
            this.Parent = parent;
            this.QueryContext = parent.QueryContext;
        }

        protected BigQueryable(BigQueryContext context)
        {
            this.Parent = null;
            this.QueryContext = context;
        }

        public sealed override string ToString()
        {
            return ToString(1, QueryContext.IndentSize, QueryContext.FormatOption);
        }

        public string ToString(FormatOption option)
        {
            return ToString(1, QueryContext.IndentSize, option);
        }

        public string ToString(int indentSize, FormatOption option)
        {
            return ToString(1, indentSize, option);
        }

        public abstract string ToString(int depth, int indentSize, FormatOption option);
    }

    internal class RootBigQueryable<T> : BigQueryable
    {
        public RootBigQueryable(BigQueryContext context)
            : base(context)
        {

        }

        public override string ToString(int depth, int indentSize, FormatOption option)
        {
            throw new System.NotImplementedException();
        }
    }

    // method chain marker

    public interface ISelectBigQueryable<TSource, TResult> : IBigQueryable, IEnumerable<TResult>
    {
    }

    public interface ILimitBigQueryable<T> : IBigQueryable
    {
    }

    public interface IOrderByBigQueryable<TSource> : ILimitBigQueryable<TSource>
    {
        IOrderByBigQueryable<TSource> CreateThenBy<TKey>(Expression<Func<TSource, TKey>> keySelector, bool isDescending);
    }

    public interface IHavingBigQueryable<T> : ILimitBigQueryable<T>
    {
    }

    public interface IGroupByBigQueryable<T> : IHavingBigQueryable<T>
    {
    }

    public interface IWhereBigQueryable<T> : IHavingBigQueryable<T>
    {
    }

    public interface IJoinBigQueryable<T> : IWhereBigQueryable<T>
    {
    }

    public interface ITableDecoratorBigQueryable<T> : IJoinBigQueryable<T>
    {
    }

    public interface IFromBigQueryable<T> : ITableDecoratorBigQueryable<T>
    {
    }

    public static class BigQueryableQueryChainExtensions
    {
        public static ITableDecoratorBigQueryable<T> WithSnapshot<T>(this IFromBigQueryable<T> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return new TableDecoratorBigQueryable<T>(source, DecorateType.Snapshot, TableDecoratorBigQueryable<T>.Zero);
        }

        public static ITableDecoratorBigQueryable<T> WithSnapshot<T>(this IFromBigQueryable<T> source, DateTime time)
        {
            if (source == null) throw new ArgumentNullException("source");

            return new TableDecoratorBigQueryable<T>(source, DecorateType.Snapshot, time);
        }

        public static ITableDecoratorBigQueryable<T> WithRange<T>(this IFromBigQueryable<T> source, DateTime timeFrom)
        {
            if (source == null) throw new ArgumentNullException("source");

            return new TableDecoratorBigQueryable<T>(source, DecorateType.Range, timeFrom);
        }

        public static ITableDecoratorBigQueryable<T> WithRange<T>(this IFromBigQueryable<T> source, DateTime timeFrom, DateTime timeTo)
        {
            if (source == null) throw new ArgumentNullException("source");

            return new TableDecoratorBigQueryable<T>(source, DecorateType.Range, timeFrom, timeTo);
        }

        public static IJoinBigQueryable<T> Join<T>(this ITableDecoratorBigQueryable<T> source, JoinType joinType = JoinType.Inner, bool each = false)
        {
            return new JoinBigQueryable<T>(source, joinType, each);
        }

        /// <summary>
        /// The WHERE clause, sometimes called the predicate, states the qualifying conditions for a query. Multiple conditions can be joined by boolean AND and OR clauses, optionally surrounded by (parentheses) to group them. The fields listed in a WHERE clause do not need to be listed in any SELECT clause.
        /// </summary>
        /// <param name="condition">Aggregate functions cannot be used in the WHERE clause.</param>
        public static IWhereBigQueryable<T> Where<T>(this IFromBigQueryable<T> source, Expression<Func<T, bool>> condition)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (condition == null) throw new ArgumentNullException("condition");

            return new WhereBigQueryable<T>(source, condition);
        }

        /// <summary>
        /// The WHERE clause, sometimes called the predicate, states the qualifying conditions for a query. Multiple conditions can be joined by boolean AND and OR clauses, optionally surrounded by (parentheses) to group them. The fields listed in a WHERE clause do not need to be listed in any SELECT clause.
        /// </summary>
        /// <param name="condition">Aggregate functions cannot be used in the WHERE clause.</param>
        public static IWhereBigQueryable<T> Where<T>(this IWhereBigQueryable<T> source, Expression<Func<T, bool>> condition)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (condition == null) throw new ArgumentNullException("condition");

            var where = source as WhereBigQueryable<T>;
            if (where == null)
            {
                return new WhereBigQueryable<T>(source, condition);
            }
            else
            {
                return where.CombinePredicate(condition);
            }
        }

        public static IGroupByBigQueryable<TSource> GroupBy<TSource, TKey>(this IWhereBigQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector, bool each = false)
        {
            return new GroupByBigQueryable<TSource, TKey>(source, keySelector, each);
        }

        public static IHavingBigQueryable<TSource> Having<TSource, TKey>(this IGroupByBigQueryable<TSource> source, Expression<Func<TSource, bool>> condition)
        {
            return new HavingBigQueryable<TSource>(source, condition);
        }

        public static IOrderByBigQueryable<TSource> OrderBy<TSource, TKey>(this IHavingBigQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector)
        {
            return new OrderByBigQueryable<TSource, TKey>(source, keySelector, isDescending: false);
        }

        public static IOrderByBigQueryable<TSource> OrderByDescending<TSource, TKey>(this IHavingBigQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector)
        {
            return new OrderByBigQueryable<TSource, TKey>(source, keySelector, isDescending: true);
        }

        public static IOrderByBigQueryable<TSource> ThenBy<TSource, TKey>(this IOrderByBigQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (keySelector == null) throw new ArgumentNullException("keySelector");

            return source.CreateThenBy(keySelector, isDescending: false);
        }

        public static IOrderByBigQueryable<TSource> ThenByDescending<TSource, TKey>(this IOrderByBigQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (keySelector == null) throw new ArgumentNullException("keySelector");

            return source.CreateThenBy(keySelector, isDescending: true);
        }

        public static ILimitBigQueryable<T> Limit<T>(this IHavingBigQueryable<T> source, int numRows)
        {
            if (numRows < 0) throw new ArgumentOutOfRangeException("numRows:" + numRows);

            return new LimitBigQueryable<T>(source, numRows);
        }

        public static ILimitBigQueryable<TSource> Limit<TSource>(this IOrderByBigQueryable<TSource> source, int numRows)
        {
            if (numRows < 0) throw new ArgumentOutOfRangeException("numRows:" + numRows);

            return new LimitBigQueryable<TSource>(source, numRows);
        }

        public static ISelectBigQueryable<TSource, TSource> Select<TSource>(this ILimitBigQueryable<TSource> source)
        {
            return new SelectBigQueryable<TSource, TSource>(source, x => x);
        }

        public static ISelectBigQueryable<TSource, TResult> Select<TSource, TResult>(this ILimitBigQueryable<TSource> source, Expression<Func<TSource, TResult>> selector)
        {
            return new SelectBigQueryable<TSource, TResult>(source, selector);
        }

        public static IFromBigQueryable<TResult> AsNestedQuery<TSource, TResult>(this ISelectBigQueryable<TSource, TResult> source)
        {
            return new FromBigQueryable<TResult>(source);
        }
    }
}