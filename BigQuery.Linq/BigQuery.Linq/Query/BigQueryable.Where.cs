using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Query
{
    public class WhereBigQueryable<T> : GroupByBigQueryable<T>
    {
        internal WhereBigQueryable(BigQueryable parent) : base(parent) { }

        public GroupByBigQueryable<T> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector, bool each = false)
        {
            return new GroupByBigQueryable<T>(this)
            {
                buildCommand = () => FieldSelectExpressionVisitor.BuildString(keySelector),
                each = each
            };
        }

        internal override string ToString(int depth, int indentSize, FormatOption option)
        {
            return "WHERE" + Environment.NewLine + buildCommand();
        }
    }
}
