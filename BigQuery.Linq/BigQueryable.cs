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
            return "";
        }
    }

    public interface IExecutableBigQueryable<T> : IBigQueryable
    {
        IEnumerable<T> AsEnumerable();

        T[] ToArray();

        // Run, RunAsync

        ISubqueryBigQueryable<T> AsSubquery();
    }

    internal abstract class ExecutableBigQueryableBase<T> : BigQueryable, BigQuery.Linq.IExecutableBigQueryable<T>
    {
        public ExecutableBigQueryableBase(IBigQueryable parent)
            : base(parent)
        {

        }

        public T[] ToArray()
        {
            return AsEnumerable().ToArray();
        }

        public IEnumerable<T> AsEnumerable()
        {
            var queryString = ToString();
            return QueryContext.Query<T>(queryString);
        }

        public ISubqueryBigQueryable<T> AsSubquery()
        {
            return QueryContext.From(this);
        }
    }

    // method chain marker
    /*
    From(+TableDecorate) -> Join -> Where -| -> OrderBy(ThenBy) -> Select ->                     | -> Limit
                                           | -> Select | -> GroupBy -> Having -> OrderBy(ThenBy) |
                                                       | -> OrderBy(ThenBy) ->                   |
    */
    public interface IFromBigQueryable<T> : ITableDecoratorBigQueryable<T> // TableDecorate, Join, Where, OrderBy, Select
    {
    }

    public interface ISubqueryBigQueryable<T> : ITableDecoratorBigQueryable<T>, IExecutableBigQueryable<T> // Join, Where, OrderBy, Select
    {
        IExecutableBigQueryable<T> Unwrap();
    }

    public interface ITableDecoratorBigQueryable<T> : IJoinBigQueryable<T> // Join, Where, OrderBy, Select
    {
    }

    public interface IJoinBigQueryable<T> : IWhereBigQueryable<T> // Join, Where, OrderBy, Select
    {
    }

    public interface IWhereBigQueryable<T> : IBigQueryable // Where, OrderBy, Select
    {

    }

    public interface IOrderByBigQueryable<TSource> : IBigQueryable // ThenBy, ThenByDescending, Select
    {
        IOrderByBigQueryable<TSource> ThenBy<TThenByKey>(Expression<Func<TSource, TThenByKey>> keySelector);
        IOrderByBigQueryable<TSource> ThenByDescending<TThenByKey>(Expression<Func<TSource, TThenByKey>> keySelector);
    }

    public interface IOrderByAfterSelectBigQueryable<TSource> : IExecutableBigQueryable<TSource> // ThenBy, ThenByDescending, Limit, Execute
    {
        IOrderByAfterSelectBigQueryable<TSource> ThenBy<TThenByKey>(Expression<Func<TSource, TThenByKey>> keySelector);
        IOrderByAfterSelectBigQueryable<TSource> ThenByDescending<TThenByKey>(Expression<Func<TSource, TThenByKey>> keySelector);
    }

    public interface ISelectAfterOrderByBigQueryable<T> : IExecutableBigQueryable<T> // Limit
    {
    }

    public interface ISelectBigQueryable<T> : IExecutableBigQueryable<T> // GroupBy, OrderBy, Limit, Execute
    {
    }

    public interface IGroupByBigQueryable<T> : IExecutableBigQueryable<T> // Having, OrderBy, Limit, Execute
    {
    }

    public interface IHavingBigQueryable<T> : IGroupByBigQueryable<T> // Having, OrderBy, Limit, Execute
    {
        // Having
    }

    public interface ILimitBigQueryable<T> : IExecutableBigQueryable<T> // Execute
    {
    }


    public static class BigQueryableQueryChainExtensions
    {
        public static ITableDecoratorBigQueryable<T> WithSnapshot<T>(this IFromBigQueryable<T> source)
        {
            if (source == null) throw new ArgumentNullException("source");

            return new TableDecoratorBigQueryable<T>(source, DecorateType.Snapshot, absoluteTime1: TableDecoratorBigQueryable<T>.Zero);
        }

        public static ITableDecoratorBigQueryable<T> WithSnapshot<T>(this IFromBigQueryable<T> source, DateTimeOffset absoluteTime)
        {
            if (source == null) throw new ArgumentNullException("source");

            return new TableDecoratorBigQueryable<T>(source, DecorateType.Snapshot, absoluteTime1: absoluteTime);
        }

        public static ITableDecoratorBigQueryable<T> WithSnapshot<T>(this IFromBigQueryable<T> source, TimeSpan relativeTime)
        {
            if (source == null) throw new ArgumentNullException("source");

            return new TableDecoratorBigQueryable<T>(source, DecorateType.Snapshot, relativeTime1: relativeTime);
        }

        public static ITableDecoratorBigQueryable<T> WithRange<T>(this IFromBigQueryable<T> source, DateTimeOffset absoluteTimeFrom)
        {
            if (source == null) throw new ArgumentNullException("source");

            return new TableDecoratorBigQueryable<T>(source, DecorateType.Range, absoluteTime1: absoluteTimeFrom);
        }

        public static ITableDecoratorBigQueryable<T> WithRange<T>(this IFromBigQueryable<T> source, TimeSpan relativeTimeFrom)
        {
            if (source == null) throw new ArgumentNullException("source");

            return new TableDecoratorBigQueryable<T>(source, DecorateType.Range, relativeTime1: relativeTimeFrom);
        }

        public static ITableDecoratorBigQueryable<T> WithRange<T>(this IFromBigQueryable<T> source, DateTimeOffset absoluteTimeFrom, DateTimeOffset absoluteTimeTo)
        {
            if (source == null) throw new ArgumentNullException("source");

            return new TableDecoratorBigQueryable<T>(source, DecorateType.Range, absoluteTime1: absoluteTimeFrom, absoluteTime2: absoluteTimeTo);
        }

        public static ITableDecoratorBigQueryable<T> WithRange<T>(this IFromBigQueryable<T> source, DateTimeOffset absoluteTimeFrom, TimeSpan relativeTimeTo)
        {
            if (source == null) throw new ArgumentNullException("source");

            return new TableDecoratorBigQueryable<T>(source, DecorateType.Range, absoluteTime1: absoluteTimeFrom, relativeTime2: relativeTimeTo);
        }

        public static ITableDecoratorBigQueryable<T> WithRange<T>(this IFromBigQueryable<T> source, TimeSpan relativeTimeFrom, DateTimeOffset absoluteTimeTo)
        {
            if (source == null) throw new ArgumentNullException("source");

            return new TableDecoratorBigQueryable<T>(source, DecorateType.Range, relativeTime1: relativeTimeFrom, absoluteTime2: absoluteTimeTo);
        }

        public static ITableDecoratorBigQueryable<T> WithRange<T>(this IFromBigQueryable<T> source, TimeSpan relativeTimeFrom, TimeSpan relativeTimeTo)
        {
            if (source == null) throw new ArgumentNullException("source");

            return new TableDecoratorBigQueryable<T>(source, DecorateType.Range, relativeTime1: relativeTimeFrom, relativeTime2: relativeTimeTo);
        }

        public static IJoinBigQueryable<TResult> Join<TOuter, TInner, TResult>(this IJoinBigQueryable<TOuter> source,
            IExecutableBigQueryable<TInner> joinTable,
            Expression<Func<TOuter, TInner, TResult>> aliasSelector,
            Expression<Func<TResult, bool>> joinCondition)
        {
            if (source == null) throw new ArgumentNullException("source");

            return new JoinBigQueryable<TOuter, TInner, TResult>(source, InternalJoinType.Inner, null, joinTable, aliasSelector, joinCondition);
        }

        public static IJoinBigQueryable<TResult> Join<TOuter, TInner, TResult>(this IJoinBigQueryable<TOuter> source,
            JoinType joinType,
            IExecutableBigQueryable<TInner> joinTable,
            Expression<Func<TOuter, TInner, TResult>> aliasSelector,
            Expression<Func<TResult, bool>> joinCondition)
        {
            if (source == null) throw new ArgumentNullException("source");

            return new JoinBigQueryable<TOuter, TInner, TResult>(source, (InternalJoinType)joinType, null, joinTable, aliasSelector, joinCondition);
        }

        public static IJoinBigQueryable<TResult> JoinCross<TOuter, TInner, TResult>(this IJoinBigQueryable<TOuter> source,
            JoinType joinType,
            IExecutableBigQueryable<TInner> joinTable,
            Expression<Func<TOuter, TInner, TResult>> aliasSelector)
        {
            if (source == null) throw new ArgumentNullException("source");

            return new JoinBigQueryable<TOuter, TInner, TResult>(source, InternalJoinType.Cross, null, joinTable, aliasSelector, null);
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
            if (where != null)
            {
                return where.CombineWhere(condition);
            }
            else
            {
                return new WhereBigQueryable<T>(source, condition);
            }
        }

        public static IOrderByBigQueryable<TSource> OrderBy<TSource, TKey>(this IWhereBigQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector)
        {
            return new OrderByBigQueryable<TSource, TKey>(source, keySelector, isDescending: false);
        }

        public static IOrderByBigQueryable<TSource> OrderByDescending<TSource, TKey>(this IWhereBigQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector)
        {
            return new OrderByBigQueryable<TSource, TKey>(source, keySelector, isDescending: true);
        }

        public static IOrderByAfterSelectBigQueryable<TSource> OrderBy<TSource, TKey>(this ISelectBigQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector)
        {
            return new OrderByBigQueryable<TSource, TKey>(source, keySelector, isDescending: false);
        }

        public static IOrderByAfterSelectBigQueryable<TSource> OrderByDescending<TSource, TKey>(this ISelectBigQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector)
        {
            return new OrderByBigQueryable<TSource, TKey>(source, keySelector, isDescending: true);
        }


        public static ISelectBigQueryable<TSource> Select<TSource>(this IWhereBigQueryable<TSource> source)
        {
            return new SelectBigQueryable<TSource, TSource>(source, x => x);
        }

        public static ISelectBigQueryable<TResult> Select<TSource, TResult>(this IWhereBigQueryable<TSource> source, Expression<Func<TSource, TResult>> selector)
        {
            return new SelectBigQueryable<TSource, TResult>(source, selector);
        }

        public static ISelectAfterOrderByBigQueryable<TSource> Select<TSource>(this IOrderByBigQueryable<TSource> source)
        {
            return new SelectBigQueryable<TSource, TSource>(source, x => x);
        }

        public static ISelectAfterOrderByBigQueryable<TResult> Select<TSource, TResult>(this IOrderByBigQueryable<TSource> source, Expression<Func<TSource, TResult>> selector)
        {
            return new SelectBigQueryable<TSource, TResult>(source, selector);
        }

        public static IGroupByBigQueryable<TSource> GroupBy<TSource, TKey>(this ISelectBigQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector, bool each = false)
        {
            return new GroupByBigQueryable<TSource, TKey>(source, keySelector, each);
        }

        public static IHavingBigQueryable<TSource> Having<TSource>(this IGroupByBigQueryable<TSource> source, Expression<Func<TSource, bool>> condition)
        {
            return new HavingBigQueryable<TSource>(source, condition);
        }

        public static ILimitBigQueryable<T> Limit<T>(this ISelectBigQueryable<T> source, int numRows)
        {
            if (numRows < 0) throw new ArgumentOutOfRangeException("numRows:" + numRows);

            return new LimitBigQueryable<T>(source, numRows);
        }

        public static ILimitBigQueryable<T> Limit<T>(this ISelectAfterOrderByBigQueryable<T> source, int numRows)
        {
            if (numRows < 0) throw new ArgumentOutOfRangeException("numRows:" + numRows);

            return new LimitBigQueryable<T>(source, numRows);
        }

        public static ILimitBigQueryable<T> Limit<T>(this IGroupByBigQueryable<T> source, int numRows)
        {
            if (numRows < 0) throw new ArgumentOutOfRangeException("numRows:" + numRows);

            return new LimitBigQueryable<T>(source, numRows);
        }

        public static ILimitBigQueryable<T> Limit<T>(this IOrderByAfterSelectBigQueryable<T> source, int numRows)
        {
            if (numRows < 0) throw new ArgumentOutOfRangeException("numRows:" + numRows);

            return new LimitBigQueryable<T>(source, numRows);
        }
    }
}