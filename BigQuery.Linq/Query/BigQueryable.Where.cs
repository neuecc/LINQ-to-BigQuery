using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Query
{
    internal class WhereBigQueryable<TSource> : QueryExecutable<TSource>, IWhereBigQueryable<TSource>
    {
        readonly Expression<Func<TSource, bool>> predicate;

        internal WhereBigQueryable(IBigQueryable parent, Expression<Func<TSource, bool>> predicate)
            : base(parent)
        {
            this.predicate = predicate;
        }

        internal IWhereBigQueryable<TSource> CombineWhere(Expression<Func<TSource, bool>> condition)
        {
            var newBody = Expression.AndAlso(this.predicate.Body, condition.Body);
            var newPredicate = Expression.Lambda<Func<TSource, bool>>(newBody, this.predicate.Parameters);

            return new WhereBigQueryable<TSource>(Parent, newPredicate);
        }

        public override string ToString(int depth, int indentSize, FormatOption option)
        {
            if (depth < 1) throw new ArgumentOutOfRangeException("depth:" + depth);

            var command = BigQueryTranslateVisitor.BuildQuery("WHERE", depth, indentSize, option, predicate, forceIndent: true);
            return Parent.ToString(depth, indentSize, option) + Environment.NewLine + command;
        }
    }
}
