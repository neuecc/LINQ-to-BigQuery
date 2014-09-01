using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Query
{

    internal class LimitBigQueryable<T> : ExecutableBigQueryableBase<T>, ILimitBigQueryable<T>
    {
        readonly int numRows;
        internal override int Order
        {
            get { return 7; }
        }

        internal LimitBigQueryable(IBigQueryable parent, int numRows)
            : base(parent)
        {
            this.numRows = numRows;
        }

        public override string BuildQueryString(int depth)
        {
            var command = Indent(depth) + "LIMIT " + numRows;
            return command;
        }
    }
}
