using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Query
{

    public class LimitBigQueryable<TSource> : BigQueryable<TSource>
    {
        internal LimitBigQueryable(BigQueryable parent)
            : base(parent)
        {

        }

        public SelectBigQueryable<TResult> Select<TResult>(Expression<Func<TSource, TResult>> selector)
        {
            return new SelectBigQueryable<TResult>(this, selector);
        }

        internal override string ToString(int depth, int indentSize, FormatOption option)
        {
            throw new NotImplementedException();
        }
    }

}
