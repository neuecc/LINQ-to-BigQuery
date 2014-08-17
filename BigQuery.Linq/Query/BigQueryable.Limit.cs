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
        readonly int numRows;

        internal LimitBigQueryable(BigQueryable parent)
            : base(parent)
        {
        }

        internal LimitBigQueryable(BigQueryable parent, int numRows)
            : base(parent)
        {
            this.numRows = numRows;
        }

        public SelectBigQueryable<TSource, TSource> Select()
        {
            return new SelectBigQueryable<TSource, TSource>(this, x => x);
        }

        public SelectBigQueryable<TSource, TResult> Select<TResult>(Expression<Func<TSource, TResult>> selector)
        {
            return new SelectBigQueryable<TSource, TResult>(this, selector);
        }

        internal override string ToString(int depth, int indentSize, FormatOption option)
        {
            // TODO:set depth, indentSize, option
            var command = "LIMIT " + numRows;
            return Parent.ToString(depth, indentSize, option) + Environment.NewLine + command;
        }
    }
}
