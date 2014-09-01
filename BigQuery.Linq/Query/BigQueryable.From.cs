using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Query
{
    internal interface IFromBigQueryable
    {
        string BuildQueryStringWithAlias(int depth, string aliasName);
    }

    internal class FromBigQueryable<T> : BigQueryable, IFromBigQueryable<T>, IFromBigQueryable
    {
        internal readonly string tableName;
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

    internal class SubqueryBigQueryable<T> : BigQueryable, ISubqueryBigQueryable<T>
    {
        readonly ExecutableBigQueryableBase<T> typedInner;

        internal override int Order
        {
            get { return 1; }
        }

        internal SubqueryBigQueryable(IExecutableBigQueryable<T> subselect)
            : base(subselect)
        {
            this.typedInner = subselect as ExecutableBigQueryableBase<T>;
        }

        public IExecutableBigQueryable<T> Unwrap()
        {
            return typedInner;
        }

        public IEnumerable<T> AsEnumerable()
        {
            return typedInner.AsEnumerable();
        }

        public T[] ToArray()
        {
            return typedInner.ToArray();
        }

        public ISubqueryBigQueryable<T> AsSubquery()
        {
            return this;
        }

        public override string BuildQueryString(int depth)
        {
            return "(" + Environment.NewLine +
                typedInner.ToQueryString(depth + 1) + Environment.NewLine + ")";
        }
    }
}