using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace BigQuery.Linq.Query
{
    // SELECT expr1 [AS alias1], expr2 [AS alias2], ...
    public class SelectBigQueryable<T> : BigQueryable<T>, IEnumerable<T>
    {
        readonly Expression selector;

        internal SelectBigQueryable(BigQueryable parent, Expression selector)
            : base(parent)
        {
            this.selector = selector;
        }

        public FromBigQueryable<T> AsNestedQuery()
        {
            return new FromBigQueryable<T>(this);
        }

        public IEnumerator<T> GetEnumerator()
        {
            var queryString = ToString();
            return QueryContext.Query<T>(queryString).GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        internal override string ToString(int depth, int indentSize, FormatOption option)
        {
            if (depth < 1) throw new ArgumentOutOfRangeException("depth:" + depth);

            var sb = new StringBuilder();
            var command = BigQueryTranslateVisitor.BuildQuery("SELECT", depth, indentSize, option, selector);

            sb.Append(command);

            if (Parent != null)
            {
                if (option == FormatOption.Indent)
                {
                    sb.AppendLine();
                }
                else
                {
                    sb.Append(" ");
                }
                sb.Append(Parent.ToString(depth, indentSize, option));
            }

            return sb.ToString();
        }
    }
}