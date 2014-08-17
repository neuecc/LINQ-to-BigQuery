using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace BigQuery.Linq.Query
{
    // SELECT expr1 [AS alias1], expr2 [AS alias2], ...
    public class SelectBigQueryable<TSource, TResult> : BigQueryable<TResult>, IEnumerable<TResult>
    {
        readonly Expression<Func<TSource, TResult>> selector;

        internal SelectBigQueryable(BigQueryable parent, Expression<Func<TSource, TResult>> selector)
            : base(parent)
        {
            this.selector = selector;
        }

        public FromBigQueryable<TResult> AsNestedQuery()
        {
            return new FromBigQueryable<TResult>(this);
        }

        public IEnumerator<TResult> GetEnumerator()
        {
            var queryString = ToString();
            return QueryContext.Query<TResult>(queryString).GetEnumerator();
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