using BigQuery.Linq.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

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
    }

    internal abstract class BigQueryable : IBigQueryable
    {
        internal abstract int Order { get; }

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

        public override string ToString()
        {
            return BuildQueryString(1);
        }

        public abstract string BuildQueryString(int depth);

        protected string Indent(int depth)
        {
            return new string(' ', QueryContext.IndentSize * depth);
        }
    }

    internal class RootBigQueryable<T> : BigQueryable
    {
        internal override int Order
        {
            get { return -1; }
        }

        public RootBigQueryable(BigQueryContext context)
            : base(context)
        {

        }

        public override string BuildQueryString(int depth)
        {
            return "";
        }
    }

    public interface IExecutableBigQueryable<T> : IBigQueryable
    {
        IEnumerable<T> AsEnumerable();

        T[] ToArray();

        Task<T[]> ToArrayAsync(CancellationToken cancellationToken = default(CancellationToken));

        QueryResponse<T> Run();

        QueryResponse<T> RunDry();

        Task<QueryResponse<T>> RunAsync(CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>Change query as Subquery.</summary>
        ISubqueryBigQueryable<T> Into();
    }

    internal abstract class ExecutableBigQueryableBase : BigQueryable
    {
        public ExecutableBigQueryableBase(IBigQueryable parent)
            : base(parent)
        {

        }

        public override string ToString()
        {
            return ToQueryString(depth: 0);
        }

        internal string ToQueryString(int depth)
        {
            var list = new List<BigQueryable>();
            var parent = this as BigQueryable;
            while (parent != null)
            {
                list.Add(parent);
                parent = parent.Parent as BigQueryable;
            }

            var aliasName = default(string);
            var join = list.OfType<IJoinBigQueryable>().FirstOrDefault();
            if (join != null)
            {
                aliasName = join.GetAliasNames().First(); // from is first!
            }
            var joinLookup = list.ToLookup(x => x is IJoinBigQueryable)[true].Reverse();

            list = list.Where(x => x.Order != -1)
                .OrderBy(x => x.Order)
                .SelectMany(x =>
                {
                    if (x is IJoinBigQueryable)
                    {
                        var jl = joinLookup;
                        joinLookup = null;
                        if (jl != null) return jl;
                        else return Enumerable.Empty<BigQueryable>();
                    }
                    else return Enumerable.Repeat(x, 1);
                })
                .ToList();

            var queryString = string.Join(Environment.NewLine, list.Select(x =>
            {
                if (aliasName != null && x is IWithAlias)
                {
                    return ((IWithAlias)x).BuildQueryStringWithAlias(depth, aliasName);
                }
                return x.BuildQueryString(depth);
            }));

            return queryString;
        }
    }

    internal abstract class ExecutableBigQueryableBase<T> : ExecutableBigQueryableBase, BigQuery.Linq.IExecutableBigQueryable<T>
    {
        public ExecutableBigQueryableBase(IBigQueryable parent)
            : base(parent)
        {

        }

        public T[] ToArray()
        {
            var result = QueryContext.Run<T>(ToString());
            return result.ToArray();
        }

        public async Task<T[]> ToArrayAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            var result = await QueryContext.RunAsync<T>(ToString(), cancellationToken).ConfigureAwait(false);
            return await result.ToArrayAsync().ConfigureAwait(false);
        }

        public QueryResponse<T> Run()
        {
            return QueryContext.Run<T>(ToString());
        }

        public Task<QueryResponse<T>> RunAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            return QueryContext.RunAsync<T>(ToString(), cancellationToken);
        }

        public QueryResponse<T> RunDry()
        {
            return QueryContext.RunDry<T>(ToString());
        }

        public IEnumerable<T> AsEnumerable()
        {
            // lazy execution
            foreach (var item in ToArray())
            {
                yield return item;
            }
        }

        public ISubqueryBigQueryable<T> Into()
        {
            return QueryContext.From(this);
        }
    }

    // method chain marker
    /*
    From((+TableDecorate)+Flatten) -> Join -> Where -| -> OrderBy(ThenBy) -> Select ->                     | -> Limit -> IgnoreCase
                                                     | -> Select | -> GroupBy -> Having -> OrderBy(ThenBy) | -> IgnoreCase
                                                                 | -> OrderBy(ThenBy) ->                   |
    */
    public interface IFromBigQueryable<T> : ITableDecoratorBigQueryable<T> // TableDecorate, Flatten, Join, Where, OrderBy, Select
    {
    }

    public interface ISubqueryBigQueryable<T> : ITableDecoratorBigQueryable<T> // Flatten, Join, Where, OrderBy, Select
    {
    }

    public interface IFromTableWildcardBigQueryable<T> : ITableDecoratorBigQueryable<T> // Flatten, Join, Where, OrderBy, Select
    {
    }

    public interface ITableDecoratorBigQueryable<T> : IFlattenBigQueryable<T> // Flatten, Join, Where, OrderBy, Select
    {
    }

    public interface IFlattenBigQueryable<T> : IJoinBigQueryable<T> // Join, Where, OrderBy, Select
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

    public interface IOrderByAfterSelectBigQueryable<TSource> : ILimitBigQueryable<TSource> // ThenBy, ThenByDescending, Limit, IgnoreCase, Execute
    {
        IOrderByAfterSelectBigQueryable<TSource> ThenBy<TThenByKey>(Expression<Func<TSource, TThenByKey>> keySelector);
        IOrderByAfterSelectBigQueryable<TSource> ThenByDescending<TThenByKey>(Expression<Func<TSource, TThenByKey>> keySelector);
    }

    public interface ISelectAfterOrderByBigQueryable<T> : ILimitBigQueryable<T> // Limit, IgnoreCase, Execute
    {
    }

    public interface ISelectBigQueryable<T> : ILimitBigQueryable<T> // GroupBy, OrderBy, Limit, IgnoreCase, Execute
    {
    }

    public interface IGroupByBigQueryable<T> : ILimitBigQueryable<T> // Having, OrderBy, Limit, IgnoreCase, Execute
    {
    }

    public interface IHavingBigQueryable<T> : IGroupByBigQueryable<T> // Having, OrderBy, Limit, IgnoreCase, Execute
    {
        // Having
    }

    public interface ILimitBigQueryable<T> : IIgnoreCaseBigQueryable<T> // IgnoreCase, Execute
    {
    }

    public interface IIgnoreCaseBigQueryable<T> : IExecutableBigQueryable<T> // Execute
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

        public static ITableDecoratorBigQueryable<T> WithRange<T>(this IFromBigQueryable<T> source, TimeSpan relativeTimeFrom, TimeSpan relativeTimeTo)
        {
            if (source == null) throw new ArgumentNullException("source");

            return new TableDecoratorBigQueryable<T>(source, DecorateType.Range, relativeTime1: relativeTimeFrom, relativeTime2: relativeTimeTo);
        }

        /// <summary>
        /// Note:Flatten with Table decorators is works fine.
        /// But Flatten with Table wildcard currently BigQuery reports error.
        /// see: https://code.google.com/p/google-bigquery/issues/detail?id=113
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="fieldSelector"></param>
        /// <returns></returns>
        public static IFlattenBigQueryable<T> Flatten<T>(this ITableDecoratorBigQueryable<T> source, Expression<Func<T, object>> fieldSelector)
        {
            if (source == null) throw new ArgumentNullException("source");

            return new FlattenBigQueryable<T>(source, fieldSelector);
        }

        public static IJoinBigQueryable<TResult> Join<TOuter, TInner, TResult>(this IJoinBigQueryable<TOuter> source,
            IExecutableBigQueryable<TInner> joinTable,
            Expression<Func<TOuter, TInner, TResult>> aliasSelector,
            Expression<Func<TResult, bool>> joinCondition)
        {
            if (source == null) throw new ArgumentNullException("source");

            return new JoinBigQueryable<TOuter, TInner, TResult>(source, InternalJoinType.Inner, null, joinTable, null, aliasSelector, joinCondition);
        }
        public static IJoinBigQueryable<TResult> Join<TOuter, TInner, TResult>(this IJoinBigQueryable<TOuter> source,
            IFlattenBigQueryable<TInner> joinTable,
            Expression<Func<TOuter, TInner, TResult>> aliasSelector,
            Expression<Func<TResult, bool>> joinCondition)
        {
            if (source == null) throw new ArgumentNullException("source");

            var tb = joinTable as ITableName;
            if (tb == null)
            {
                var flatten = joinTable as IFlattenBigQueryable<TInner>;
                if (flatten == null)
                {
                    throw new ArgumentException("not supports joinTable Type:" + joinTable.GetType());
                }
                else
                {
                    return new JoinBigQueryable<TOuter, TInner, TResult>(source, InternalJoinType.Inner, null, null, flatten, aliasSelector, joinCondition);
                }
            }
            return new JoinBigQueryable<TOuter, TInner, TResult>(source, InternalJoinType.Inner, tb.GetTableName(), null, null, aliasSelector, joinCondition);
        }

        public static IJoinBigQueryable<TResult> Join<TOuter, TInner, TResult>(this IJoinBigQueryable<TOuter> source,
            JoinType joinType,
            IExecutableBigQueryable<TInner> joinTable,
            Expression<Func<TOuter, TInner, TResult>> aliasSelector,
            Expression<Func<TResult, bool>> joinCondition)
        {
            if (source == null) throw new ArgumentNullException("source");

            return new JoinBigQueryable<TOuter, TInner, TResult>(source, (InternalJoinType)joinType, null, joinTable, null, aliasSelector, joinCondition);
        }

        public static IJoinBigQueryable<TResult> Join<TOuter, TInner, TResult>(this IJoinBigQueryable<TOuter> source,
            JoinType joinType,
            IFlattenBigQueryable<TInner> joinTable,
            Expression<Func<TOuter, TInner, TResult>> aliasSelector,
            Expression<Func<TResult, bool>> joinCondition)
        {
            if (source == null) throw new ArgumentNullException("source");

            var tb = joinTable as ITableName;
            if (tb == null)
            {
                var flatten = joinTable as IFlattenBigQueryable<TInner>;
                if (flatten == null)
                {
                    throw new ArgumentException("not supports joinTable Type:" + joinTable.GetType());
                }
                else
                {
                    return new JoinBigQueryable<TOuter, TInner, TResult>(source, (InternalJoinType)joinType, null, null, flatten, aliasSelector, joinCondition);
                }
            }
            return new JoinBigQueryable<TOuter, TInner, TResult>(source, (InternalJoinType)joinType, tb.GetTableName(), null, null, aliasSelector, joinCondition);
        }

        public static IJoinBigQueryable<TResult> JoinCross<TOuter, TInner, TResult>(this IJoinBigQueryable<TOuter> source,
            IExecutableBigQueryable<TInner> joinTable,
            Expression<Func<TOuter, TInner, TResult>> aliasSelector,
            bool each = false)
        {
            if (source == null) throw new ArgumentNullException("source");

            return new JoinBigQueryable<TOuter, TInner, TResult>(source, (each ? InternalJoinType.CrossEach : InternalJoinType.Cross), null, joinTable, null, aliasSelector, null);
        }

        public static IJoinBigQueryable<TResult> JoinCross<TOuter, TInner, TResult>(this IJoinBigQueryable<TOuter> source,
            IFlattenBigQueryable<TInner> joinTable,
            Expression<Func<TOuter, TInner, TResult>> aliasSelector,
            bool each = false)
        {
            if (source == null) throw new ArgumentNullException("source");

            var tb = joinTable as ITableName;
            if (tb == null)
            {
                var flatten = joinTable as IFlattenBigQueryable<TInner>;
                if (flatten == null)
                {
                    throw new ArgumentException("not supports joinTable Type:" + joinTable.GetType());
                }
                else
                {
                    return new JoinBigQueryable<TOuter, TInner, TResult>(source, (each ? InternalJoinType.CrossEach : InternalJoinType.Cross), null, null, flatten, aliasSelector, null);
                }
            }
            return new JoinBigQueryable<TOuter, TInner, TResult>(source, (each ? InternalJoinType.CrossEach : InternalJoinType.Cross), tb.GetTableName(), null, null, aliasSelector, null);
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

        public static IOrderByAfterSelectBigQueryable<TSource> OrderBy<TSource, TKey>(this IGroupByBigQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector)
        {
            return new OrderByBigQueryable<TSource, TKey>(source, keySelector, isDescending: false);
        }

        public static IOrderByAfterSelectBigQueryable<TSource> OrderByDescending<TSource, TKey>(this IGroupByBigQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector)
        {
            return new OrderByBigQueryable<TSource, TKey>(source, keySelector, isDescending: true);
        }

        public static IOrderByAfterSelectBigQueryable<TSource> OrderBy<TSource, TKey>(this IHavingBigQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector)
        {
            return new OrderByBigQueryable<TSource, TKey>(source, keySelector, isDescending: false);
        }

        public static IOrderByAfterSelectBigQueryable<TSource> OrderByDescending<TSource, TKey>(this IHavingBigQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector)
        {
            return new OrderByBigQueryable<TSource, TKey>(source, keySelector, isDescending: true);
        }

        public static ISelectBigQueryable<TSource> Select<TSource>(this IWhereBigQueryable<TSource> source)
        {
            return new SelectBigQueryable<TSource, TSource>(source, null);
        }

        public static ISelectBigQueryable<TResult> Select<TSource, TResult>(this IWhereBigQueryable<TSource> source, Expression<Func<TSource, TResult>> selector)
        {
            return new SelectBigQueryable<TSource, TResult>(source, selector);
        }

        public static ISelectAfterOrderByBigQueryable<TSource> Select<TSource>(this IOrderByBigQueryable<TSource> source)
        {
            return new SelectBigQueryable<TSource, TSource>(source, null);
        }

        public static ISelectAfterOrderByBigQueryable<TResult> Select<TSource, TResult>(this IOrderByBigQueryable<TSource> source, Expression<Func<TSource, TResult>> selector)
        {
            return new SelectBigQueryable<TSource, TResult>(source, selector);
        }

        public static IGroupByBigQueryable<TSource> GroupBy<TSource, TKey>(this ISelectBigQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector, bool each = false, bool rollup = false)
        {
            return new GroupByBigQueryable<TSource, TKey>(source, keySelector, each, rollup);
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

        public static IIgnoreCaseBigQueryable<T> IgnoreCase<T>(this ILimitBigQueryable<T> source)
        {
            return new IgnoreCaseBigQueryable<T>(source);
        }
    }
}