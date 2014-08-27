using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Query
{

    internal class GroupByBigQueryable<TSource, TKey> : QueryExecutable<TSource>, IGroupByBigQueryable<TSource>
    {
        readonly Expression<Func<TSource, TKey>> keySelector;
        readonly bool each;

        internal GroupByBigQueryable(IBigQueryable parent, Expression<Func<TSource, TKey>> keySelector, bool each)
            : base(parent)
        {
            this.keySelector = keySelector;
            this.each = each;
        }

        public override string ToString(int depth, int indentSize, FormatOption option)
        {
            return "GROUP " + ((each) ? "EACH BY " : "BY ")
                + Environment.NewLine;
        }
    }
}
