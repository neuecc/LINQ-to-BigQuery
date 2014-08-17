using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Query
{

    internal class LimitBigQueryable<T> : BigQueryable, ILimitBigQueryable<T>
    {
        readonly int numRows;

        internal LimitBigQueryable(IBigQueryable parent, int numRows)
            : base(parent)
        {
            this.numRows = numRows;
        }

        public override string ToString(int depth, int indentSize, FormatOption option)
        {
            // TODO:set depth, indentSize, option
            var command = "LIMIT " + numRows;
            return Parent.ToString(depth, indentSize, option) + Environment.NewLine + command;
        }
    }
}
