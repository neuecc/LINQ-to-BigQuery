using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Query
{

    internal class IgnoreCaseBigQueryable<T> : ExecutableBigQueryableBase<T>, IIgnoreCaseBigQueryable<T>
    {
        internal override int Order
        {
            get { return 8; }
        }

        internal IgnoreCaseBigQueryable(IBigQueryable parent)
            : base(parent)
        {
        }

        public override string BuildQueryString(int depth)
        {
            var command = Indent(depth) + "IGNORE CASE";
            return command;
        }
    }
}
