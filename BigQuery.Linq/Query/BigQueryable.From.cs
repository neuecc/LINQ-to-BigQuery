using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BigQuery.Linq.Query
{
    internal interface IWithAlias
    {
        string BuildQueryStringWithAlias(int depth, string aliasName);
    }

    internal interface ITableName
    {
        string GetTableName();
    }

    internal interface IWithoutFrom
    {
        string BuildQueryStringWithoutFrom(int depth);
    }

    internal class FromBigQueryable<T> : BigQueryable, IFromBigQueryable<T>, IWithAlias, ITableName
    {
        internal readonly string[] tableNames; // for TableDecorator
        internal override int Order
        {
            get { return 1; }
        }

        internal FromBigQueryable(string[] tableNames, IBigQueryable parent)
            : base(parent)
        {
            this.tableNames = tableNames.Select(x => x.EscapeBq()).ToArray();
        }

        public override string BuildQueryString(int depth)
        {
            return Indent(depth) + "FROM" + Environment.NewLine + Indent(depth + 1) + string.Join(", ", tableNames);
        }

        public string BuildQueryStringWithAlias(int depth, string aliasName)
        {
            return Indent(depth) + "FROM" + Environment.NewLine + Indent(depth + 1) + string.Join(", ", tableNames) + " AS " + aliasName.EscapeBq();
        }

        public string GetTableName()
        {
            return string.Join(", ", tableNames);
        }
    }

    // Currently, DateRange is not supported with Table Decorator. : http://stackoverflow.com/questions/22740774/use-of-table-date-range-function-with-table-decorators
    internal class FromDateRangeBigQueryable<T> : BigQueryable, IFromTableWildcardBigQueryable<T>, IWithAlias, ITableName
    {
        readonly string prefix;
        readonly DateTimeOffset timestampFrom;
        readonly DateTimeOffset timestampTo;
        readonly Expression<Func<DateTimeOffset>> timestampFromExpr;
        readonly Expression<Func<DateTimeOffset>> timestampToExpr;

        internal override int Order
        {
            get { return 1; }
        }

        internal FromDateRangeBigQueryable(string prefix, DateTimeOffset timestampFrom, DateTimeOffset timestampTo, IBigQueryable parent)
            : base(parent)
        {
            this.prefix = prefix.EscapeBq();
            this.timestampFrom = timestampFrom;
            this.timestampTo = timestampTo;
        }

        internal FromDateRangeBigQueryable(string prefix, Expression<Func<DateTimeOffset>> timestampFrom, Expression<Func<DateTimeOffset>> timestampTo, IBigQueryable parent)
            : base(parent)
        {
            this.prefix = prefix.EscapeBq();
            this.timestampFromExpr = timestampFrom;
            this.timestampToExpr = timestampTo;
        }

        public override string BuildQueryString(int depth)
        {
            return Indent(depth) + "FROM" + Environment.NewLine + Indent(depth + 1) + RangeFormat();
        }

        public string BuildQueryStringWithAlias(int depth, string aliasName)
        {
            return Indent(depth) + "FROM" + Environment.NewLine + Indent(depth + 1) + RangeFormat() + " AS " + aliasName.EscapeBq();
        }

        string RangeFormat()
        {
            if (timestampFromExpr == null)
            {
                return string.Format("TABLE_DATE_RANGE({0}, TIMESTAMP('{1}'), TIMESTAMP('{2}'))", prefix, timestampFrom.ToUniversalTime().ToString("yyyy-MM-dd"), timestampTo.ToUniversalTime().ToString("yyyy-MM-dd"));
            }
            else
            {
                var translator = new BigQueryTranslateVisitor();
                var from = translator.VisitAndClearBuffer(timestampFromExpr);
                var to = translator.VisitAndClearBuffer(timestampToExpr);
                return string.Format("TABLE_DATE_RANGE({0}, {1}, {2})", prefix, from, to);
            }
        }

        public string GetTableName()
        {
            return RangeFormat();
        }
    }

    internal class FromDateRangeStrictBigQueryable<T> : BigQueryable, IFromTableWildcardBigQueryable<T>, IWithAlias, ITableName
    {
        readonly string prefix;
        readonly DateTimeOffset timestampFrom;
        readonly DateTimeOffset timestampTo;
        readonly Expression<Func<DateTimeOffset>> timestampFromExpr;
        readonly Expression<Func<DateTimeOffset>> timestampToExpr;

        internal override int Order
        {
            get { return 1; }
        }

        internal FromDateRangeStrictBigQueryable(string prefix, DateTimeOffset timestampFrom, DateTimeOffset timestampTo, IBigQueryable parent)
            : base(parent)
        {
            this.prefix = prefix.EscapeBq();
            this.timestampFrom = timestampFrom;
            this.timestampTo = timestampTo;
        }

        internal FromDateRangeStrictBigQueryable(string prefix, Expression<Func<DateTimeOffset>> timestampFrom, Expression<Func<DateTimeOffset>> timestampTo, IBigQueryable parent)
            : base(parent)
        {
            this.prefix = prefix.EscapeBq();
            this.timestampFromExpr = timestampFrom;
            this.timestampToExpr = timestampTo;
        }

        public override string BuildQueryString(int depth)
        {
            return Indent(depth) + "FROM" + Environment.NewLine + Indent(depth + 1) + RangeFormat();
        }

        public string BuildQueryStringWithAlias(int depth, string aliasName)
        {
            return Indent(depth) + "FROM" + Environment.NewLine + Indent(depth + 1) + RangeFormat() + " AS " + aliasName.EscapeBq();
        }

        string RangeFormat()
        {
            if (timestampFromExpr == null)
            {
                return string.Format("TABLE_DATE_RANGE_STRICT({0}, TIMESTAMP('{1}'), TIMESTAMP('{2}'))", prefix, timestampFrom.ToUniversalTime().ToString("yyyy-MM-dd"), timestampTo.ToUniversalTime().ToString("yyyy-MM-dd"));
            }
            else
            {
                var translator = new BigQueryTranslateVisitor();
                var from = translator.VisitAndClearBuffer(timestampFromExpr);
                var to = translator.VisitAndClearBuffer(timestampToExpr);
                return string.Format("TABLE_DATE_RANGE_STRICT({0}, {1}, {2})", prefix, from, to);
            }
        }

        public string GetTableName()
        {
            return RangeFormat();
        }
    }

    internal class FromTableQueryBigQueryable<T> : BigQueryable, IFromTableWildcardBigQueryable<T>, IWithAlias, ITableName
    {
        readonly string dataset;
        readonly Expression<Func<MetaTable, bool>> tableMatchCondition;
        internal override int Order
        {
            get { return 1; }
        }

        internal FromTableQueryBigQueryable(string dataset, Expression<Func<MetaTable, bool>> tableMatchCondition, IBigQueryable parent)
            : base(parent)
        {
            this.dataset = dataset.EscapeBq();
            this.tableMatchCondition = tableMatchCondition;
        }

        public override string BuildQueryString(int depth)
        {
            return Indent(depth) + "FROM" + Environment.NewLine + Indent(depth + 1) + QueryFormat();
        }

        public string BuildQueryStringWithAlias(int depth, string aliasName)
        {
            return Indent(depth) + "FROM" + Environment.NewLine + Indent(depth + 1) + QueryFormat() + " AS " + aliasName.EscapeBq();
        }

        string QueryFormat()
        {
            var expr = BigQueryTranslateVisitor.BuildQuery(0, 0, tableMatchCondition);
            return string.Format("TABLE_QUERY({0}, \"{1}\")", dataset, expr);
        }

        public string GetTableName()
        {
            return QueryFormat();
        }
    }

    internal class SubqueryBigQueryable<T> : BigQueryable, ISubqueryBigQueryable<T>, IWithAlias, IWithoutFrom
    {
        readonly ExecutableBigQueryableBase<T>[] typedInners;

        internal override int Order
        {
            get { return 1; }
        }

        internal SubqueryBigQueryable(IExecutableBigQueryable<T>[] subselects)
            : base(new RootBigQueryable<T>(subselects[0].QueryContext))
        {
            this.typedInners = subselects.OfType<ExecutableBigQueryableBase<T>>().ToArray();
        }

        public override string BuildQueryString(int depth)
        {
            var fromHeader = Indent(depth) + "FROM" + Environment.NewLine;

            return fromHeader + BuildQueryStringWithoutFrom(depth);
        }

        public string BuildQueryStringWithoutFrom(int depth)
        {
            var sb = new StringBuilder();
            var isFirst = true;
            foreach (var item in typedInners)
            {
                if (isFirst)
                {
                    isFirst = false;
                }
                else
                {
                    sb.Append(",");
                    sb.AppendLine();
                }

                sb.AppendLine(Indent(depth) + "(");
                sb.AppendLine(item.ToQueryString(depth + 1));
                sb.Append(Indent(depth) + ")");
            }

            return sb.ToString();
        }

        public string BuildQueryStringWithAlias(int depth, string aliasName)
        {
            if (typedInners.Length > 1)
            {
                throw new InvalidOperationException("JOIN cannot be applied directly to a table union or to a table wildcard function. Consider wrapping the table union or table wildcard function in a subquery (e.g., SELECT *).");
            }

            return BuildQueryString(depth) + " AS " + aliasName.EscapeBq();
        }
    }
}