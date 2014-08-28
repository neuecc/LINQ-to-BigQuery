using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Query
{
    internal class OrderByBigQueryable<TSource, TKey> : QueryExecutable<TSource>, IOrderByBigQueryable<TSource>, IOrderByAfterSelectBigQueryable<TSource>
    {
        readonly Tuple<Expression, bool>[] keySelectors;

        internal OrderByBigQueryable(IBigQueryable parent, Expression<Func<TSource, TKey>> keySelector, bool isDescending)
            : base(parent)
        {
            this.keySelectors = new[] { Tuple.Create<Expression, bool>(keySelector, isDescending) };
        }

        OrderByBigQueryable(IBigQueryable parent, Tuple<Expression, bool>[] keySelectors)
            : base(parent)
        {
            this.keySelectors = keySelectors;
        }

        OrderByBigQueryable<TSource, TThenByKey> CreateThenBy<TThenByKey>(Expression<Func<TSource, TThenByKey>> keySelector, bool isDescending)
        {
            var newContainer = new Tuple<Expression, bool>[this.keySelectors.Length + 1];
            Array.Copy(this.keySelectors, newContainer, 0);

            newContainer[newContainer.Length - 1] = Tuple.Create<Expression, bool>(keySelector, isDescending);
            return new OrderByBigQueryable<TSource, TThenByKey>(this.Parent, newContainer);
        }

        IOrderByBigQueryable<TSource> IOrderByBigQueryable<TSource>.ThenBy<TThenByKey>(Expression<Func<TSource, TThenByKey>> keySelector)
        {
            return CreateThenBy(keySelector, isDescending: false);
        }

        IOrderByBigQueryable<TSource> IOrderByBigQueryable<TSource>.ThenByDescending<TThenByKey>(Expression<Func<TSource, TThenByKey>> keySelector)
        {
            return CreateThenBy(keySelector, isDescending: true);
        }

        IOrderByAfterSelectBigQueryable<TSource> IOrderByAfterSelectBigQueryable<TSource>.ThenBy<TThenByKey>(Expression<Func<TSource, TThenByKey>> keySelector)
        {
            return CreateThenBy(keySelector, isDescending: false);
        }

        IOrderByAfterSelectBigQueryable<TSource> IOrderByAfterSelectBigQueryable<TSource>.ThenByDescending<TThenByKey>(Expression<Func<TSource, TThenByKey>> keySelector)
        {
            return CreateThenBy(keySelector, isDescending: true);
        }

        public override string ToString(int depth, int indentSize, FormatOption option)
        {
            throw new NotImplementedException();
        }
    }
}
