using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Query
{
    internal class OrderByBigQueryable<TSource, TKey> : BigQueryable, IOrderByBigQueryable<TSource>
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

        public IOrderByBigQueryable<TSource> CreateThenBy<TThenByKey>(Expression<Func<TSource, TThenByKey>> keySelector, bool isDescending)
        {
            var newContainer = new Tuple<Expression, bool>[this.keySelectors.Length + 1];
            Array.Copy(this.keySelectors, newContainer, 0);

            newContainer[newContainer.Length - 1] = Tuple.Create<Expression, bool>(keySelector, isDescending);
            return new OrderByBigQueryable<TSource, TThenByKey>(this.Parent, newContainer);
        }

        public override string ToString(int depth, int indentSize, FormatOption option)
        {
            throw new NotImplementedException();
        }
    }
}
