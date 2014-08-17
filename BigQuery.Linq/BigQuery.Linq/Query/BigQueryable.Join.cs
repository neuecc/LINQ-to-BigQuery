using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Query
{
    // public AfterSelectBigQueryable<TResult> Select<TResult>(Expression<Func<TSource, TResult>> selector)

    public class JoinBigQueryable<T> : WhereBigQueryable<T>
    {
        internal JoinBigQueryable(BigQueryable parent) : base(parent) { }

        public WhereBigQueryable<T> Where(Expression<Func<T, bool>> predicate)
        {
            return new WhereBigQueryable<T>(this)
            {
                buildCommand = () => ConditionExpressionVisitor.Build(predicate)
            };
        }
    }
}
