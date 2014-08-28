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

        internal FromBigQueryable(IBigQueryable subselect)
            : base(subselect)
        {

        }

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
}
