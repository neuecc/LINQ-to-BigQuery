using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Query
{
    internal class HavingBigQueryable<TSource> : ExecutableBigQueryableBase<TSource>, IHavingBigQueryable<TSource>
    {
        readonly Expression<Func<TSource, bool>> predicate;

        internal HavingBigQueryable(IBigQueryable parent, Expression<Func<TSource, bool>> predicate)
            : base(parent)
        {
            this.predicate = predicate;
        }

        public override string ToString(int depth, int indentSize, FormatOption option)
        {
            throw new NotImplementedException();
        }
    }

}
