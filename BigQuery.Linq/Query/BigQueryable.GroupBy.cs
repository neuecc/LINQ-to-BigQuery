using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Query
{

    internal class GroupByBigQueryable<TSource, TKey> : ExecutableBigQueryableBase<TSource>, IGroupByBigQueryable<TSource>
    {
        readonly Expression<Func<TSource, TKey>> keySelector;
        readonly bool each;
        readonly bool rollup;

        internal override int Order
        {
            get { return 4; }
        }

        internal GroupByBigQueryable(IBigQueryable parent, Expression<Func<TSource, TKey>> keySelector, bool each, bool rollup)
            : base(parent)
        {
            this.keySelector = keySelector;
            this.each = each;
            this.rollup = rollup;
        }

        public override string BuildQueryString(int depth)
        {
            var command = "";
            if (keySelector == null)
            {
                command = Indent(depth + 1) + "*";
            }
            else
            {
                if (keySelector.Body.NodeType != ExpressionType.New)
                {
                    command = Indent(depth + 1);
                }
                command += BigQueryTranslateVisitor.BuildQuery(depth + 1, QueryContext.IndentSize, keySelector);
            }

            var sb = new StringBuilder();
            sb.Append(Indent(depth));
            sb.Append("GROUP " + ((each) ? "EACH BY" : "BY"));
            if (rollup)
            {
                sb.AppendLine(" ROLLUP");
                sb.Append(Indent(depth));
                sb.AppendLine("(");
            }
            else
            {
                sb.AppendLine();
            }
            sb.Append(command);
            if (rollup)
            {
                sb.AppendLine();
                sb.Append(Indent(depth));
                sb.Append(")");
            }

            return sb.ToString();
        }
    }
}