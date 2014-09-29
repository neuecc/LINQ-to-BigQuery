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

        public override string BuildQueryString(int depth)
        {
            var command = "";
            if (selector == null)
            {
                command = Indent(depth + 1) + "*";
            }
            else
            {
                if (selector.Body.NodeType != ExpressionType.New
                 && selector.Body.NodeType != ExpressionType.MemberInit)
                {
                    command = Indent(depth + 1);
                }
                command += BigQueryTranslateVisitor.BuildQuery(depth + 1, QueryContext.IndentSize, selector);
            }

            var sb = new StringBuilder();
            sb.Append(Indent(depth));
            sb.AppendLine("SELECT");
            sb.Append(command);

            return sb.ToString();
        }
    }
}