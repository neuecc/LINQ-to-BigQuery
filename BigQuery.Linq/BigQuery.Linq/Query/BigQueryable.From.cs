using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Query
{

    public class FromBigQueryable<T> : TableDecorateBigQueryable<T>
    {
        readonly string tableName;

        internal FromBigQueryable(BigQueryable subselect)
            : base(subselect)
        {

        }

        internal FromBigQueryable(string tableName, BigQueryable parent)
            : base(parent)
        {
            this.tableName = tableName;
        }

        public TableDecorateBigQueryable<T> WithSnapshot()
        {
            throw new NotImplementedException();
        }

        public TableDecorateBigQueryable<T> WithRange()
        {
            throw new NotImplementedException();
        }

        internal override string ToString(int depth, int indentSize, FormatOption option)
        {
            return "FROM " + Environment.NewLine + "  " + tableName;
        }
    }
}
