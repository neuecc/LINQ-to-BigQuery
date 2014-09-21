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

    internal class FromBigQueryable<T> : BigQueryable, IFromBigQueryable<T>, IWithAlias
    {
        internal readonly string tableName; // for TableDecorator
        internal override int Order
        {
            get { return 1; }
        }

        internal FromBigQueryable(string tableName, IBigQueryable parent)
            : base(parent)
        {
            this.tableName = tableName.EscapeBq();
        }

        public override string BuildQueryString(int depth)
        {
            return Indent(depth) + "FROM" + Environment.NewLine + Indent(depth + 1) + tableName;
        }

        public string BuildQueryStringWithAlias(int depth, string aliasName)
        {
            return Indent(depth) + "FROM" + Environment.NewLine + Indent(depth + 1) + tableName + " AS " + aliasName;
        }
    }

    // Currently, DateRange is not supported with Table Decorator. : http://stackoverflow.com/questions/22740774/use-of-table-date-range-function-with-table-decorators
    internal class FromDateRangeBigQueryable<T> : BigQueryable, IFromTableWildcardBigQueryable<T>, IWithAlias
    {
        readonly string prefix;
        readonly DateTimeOffset timestampFrom;
        readonly DateTimeOffset timestampTo;
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

        public override string BuildQueryString(int depth)
        {
            return Indent(depth) + "FROM" + Environment.NewLine + Indent(depth + 1) + RangeFormat();
        }

        public string BuildQueryStringWithAlias(int depth, string aliasName)
        {
            return Indent(depth) + "FROM" + Environment.NewLine + Indent(depth + 1) + RangeFormat() + " AS " + aliasName;
        }

        string RangeFormat()
        {
            return string.Format("(TABLE_DATE_RANGE({0}, TIMESTAMP('{1}'), TIMESTAMP('{2}')))", prefix, timestampFrom.ToString("yyyy-MM-dd"), timestampTo.ToString("yyyy-MM-dd"));
        }
    }

    internal class FromDateRangeStrictBigQueryable<T> : BigQueryable, IFromTableWildcardBigQueryable<T>, IWithAlias
    {
        readonly string prefix;
        readonly DateTimeOffset timestampFrom;
        readonly DateTimeOffset timestampTo;
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

        public override string BuildQueryString(int depth)
        {
            return Indent(depth) + "FROM" + Environment.NewLine + Indent(depth + 1) + RangeFormat();
        }

        public string BuildQueryStringWithAlias(int depth, string aliasName)
        {
            return Indent(depth) + "FROM" + Environment.NewLine + Indent(depth + 1) + RangeFormat() + " AS " + aliasName;
        }

        string RangeFormat()
        {
            return string.Format("(TABLE_DATE_RANGE_STRICT({0}, TIMESTAMP('{1}'), TIMESTAMP('{2}')))", prefix, timestampFrom.ToString("yyyy-MM-dd"), timestampTo.ToString("yyyy-MM-dd"));
        }
    }

    internal class FromTableQueryBigQueryable<T> : BigQueryable, IFromTableWildcardBigQueryable<T>, IWithAlias
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
            return Indent(depth) + "FROM" + Environment.NewLine + Indent(depth + 1) + QueryFormat() + " AS " + aliasName;
        }

        string QueryFormat()
        {
            var expr = BigQueryTranslateVisitor.BuildQuery(0, 0, tableMatchCondition);
            return string.Format("(TABLE_QUERY({0}, \"{1}\"))", dataset, expr);
        }
    }

    internal class SubqueryBigQueryable<T> : BigQueryable, ISubqueryBigQueryable<T>
    {
        readonly ExecutableBigQueryableBase<T> typedInner;

        internal override int Order
        {
            get { return 1; }
        }

        internal SubqueryBigQueryable(IExecutableBigQueryable<T> subselect)
            : base(new RootBigQueryable<T>(subselect.QueryContext))
        {
            this.typedInner = subselect as ExecutableBigQueryableBase<T>;
        }

        public IExecutableBigQueryable<T> Unwrap()
        {
            return typedInner;
        }

        public T[] ToArray()
        {
            return typedInner.ToArray();
        }

        public override string BuildQueryString(int depth)
        {
            return Indent(depth) + "FROM" + Environment.NewLine
                + Indent(depth) + "(" + Environment.NewLine
                + typedInner.ToQueryString(depth + 1) + Environment.NewLine
                + Indent(depth) + ")";
        }
    }
}