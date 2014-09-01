using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace BigQuery.Linq.Query
{
    // SELECT expr1 [AS alias1], expr2 [AS alias2], ...
    internal class SelectBigQueryable<TSource, TResult> : ExecutableBigQueryableBase<TResult>, ISelectBigQueryable<TResult>, ISelectAfterOrderByBigQueryable<TResult>
    {
        readonly Expression<Func<TSource, TResult>> selector;
        internal override int Order
        {
            get { return 0; }
        }
        internal SelectBigQueryable(IBigQueryable parent, Expression<Func<TSource, TResult>> selector)
            : base(parent)
        {
            this.selector = selector;
        }

        public IEnumerator<TResult> GetEnumerator()
        {
            var queryString = ToString();
            return QueryContext.Query<TResult>(queryString).GetEnumerator();
        }

        public override string BuildQueryString(int depth)
        {
            var command = (selector != null)
                ? BigQueryTranslateVisitor.BuildQuery(depth + 1, QueryContext.IndentSize, selector)
                : Indent(depth + 1) + "*";

            var sb = new StringBuilder();
            sb.Append(Indent(depth));
            sb.AppendLine("SELECT");
            sb.Append(command);

            return sb.ToString();
        }
    }
}