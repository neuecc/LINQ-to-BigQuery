using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Query
{
    internal class FromBigQueryable<T> : BigQueryable, IFromBigQueryable<T>
    {
        internal readonly string tableName;

        internal FromBigQueryable(string tableName, IBigQueryable parent)
            : base(parent)
        {
            this.tableName = tableName;
        }

        public override string ToString(int depth, int indentSize, FormatOption option)
        {
            return "FROM" + Environment.NewLine + "  " + tableName;
        }
    }

    internal class SubqueryBigQueryable<T> : BigQueryable, ISubqueryBigQueryable<T>
    {
        readonly IExecutableBigQueryable<T> typedInner;

        internal SubqueryBigQueryable(IExecutableBigQueryable<T> subselect)
            : base(subselect)
        {
            this.typedInner = subselect;
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

        public override string ToString(int depth, int indentSize, FormatOption option)
        {
            return "(" + Parent.ToString() + ")";
        }
    }
}