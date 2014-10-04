using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Query
{
    internal class FlattenBigQueryable<T> : BigQueryable, IFlattenBigQueryable<T>, IWithoutFrom, IWithAlias
    {
        readonly ITableDecoratorBigQueryable<T> typedParent;
        readonly Expression<Func<T, object>> fieldSelector;

        internal override int Order
        {
            get { return 1; }
        }

        internal FlattenBigQueryable(ITableDecoratorBigQueryable<T> parent, Expression<Func<T, object>> fieldSelector)
            : base(new RootBigQueryable<T>(parent.QueryContext))
        {
            this.typedParent = parent;
            this.fieldSelector = fieldSelector;
        }

        public override string BuildQueryString(int depth)
        {
            ITableName tableName = null;
            IBigQueryable parent = null;
            if (typedParent is ITableName)
            {
                parent = (IBigQueryable)typedParent;
                tableName = (ITableName)typedParent;
            }
            else if (typedParent is ITableName)
            {
                parent = (IBigQueryable)typedParent;
                tableName = (ITableName)typedParent;
            }

            if (tableName != null)
            {
                var sb = new StringBuilder();

                sb.Append(Indent(depth));
                sb.AppendLine("FROM");

                sb.Append(Indent(depth + 1));
                sb.Append("FLATTEN(");
                sb.Append(tableName.GetTableName());

                sb.Append(", ");
                sb.Append(BigQueryTranslateVisitor.BuildQuery(depth, parent.QueryContext.IndentSize, fieldSelector));

                sb.Append(")");

                return sb.ToString();
            }

            var subquery = typedParent as SubqueryBigQueryable<T>;
            if (subquery != null)
            {
                var sb = new StringBuilder();

                sb.Append(Indent(depth));
                sb.AppendLine("FROM FLATTEN(");

                sb.Append(subquery.BuildQueryStringWithoutFrom(depth));

                sb.Append(", ");
                sb.Append(BigQueryTranslateVisitor.BuildQuery(depth, subquery.QueryContext.IndentSize, fieldSelector));

                sb.Append(")");

                return sb.ToString();
            }

            throw new InvalidOperationException("Unexpected pattern, please report query to GitHub issues");
        }

        public string BuildQueryStringWithoutFrom(int depth)
        {
            ITableName tableName = null;
            IBigQueryable parent = null;
            if (typedParent is ITableName)
            {
                parent = (IBigQueryable)typedParent;
                tableName = (ITableName)typedParent;
            }
            else if (typedParent is ITableName)
            {
                parent = (IBigQueryable)typedParent;
                tableName = (ITableName)typedParent;
            }

            if (tableName != null)
            {
                var sb = new StringBuilder();

                sb.AppendLine();
                sb.Append(Indent(depth + 1));
                sb.Append("FLATTEN(");
                sb.Append(tableName.GetTableName());

                sb.Append(", ");
                sb.Append(BigQueryTranslateVisitor.BuildQuery(depth, parent.QueryContext.IndentSize, fieldSelector));

                sb.Append(")");

                return sb.ToString();
            }

            var subquery = typedParent as SubqueryBigQueryable<T>;
            if (subquery != null)
            {
                var sb = new StringBuilder();

                sb.AppendLine(" FLATTEN(");

                sb.Append(subquery.BuildQueryStringWithoutFrom(depth));

                sb.Append(", ");
                sb.Append(BigQueryTranslateVisitor.BuildQuery(depth, subquery.QueryContext.IndentSize, fieldSelector));

                sb.Append(")");

                return sb.ToString();
            }

            throw new InvalidOperationException("Unexpected pattern, please report query to GitHub issues");
        }

        public string BuildQueryStringWithAlias(int depth, string aliasName)
        {
            return BuildQueryString(depth) + " AS " + aliasName.EscapeBq();
        }
    }
}