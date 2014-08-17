using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Query
{
    public class OrderByBigQueryable<T> : LimitBigQueryable<T>
    {
        internal OrderByBigQueryable(BigQueryable parent) : base(parent) { }

        public LimitBigQueryable<T> Limit(int numRows)
        {
            if (numRows < 0) throw new ArgumentOutOfRangeException("numRows:" + numRows);

            return new LimitBigQueryable<T>(this, numRows);
        }

        public OrderByBigQueryable<T> ThenBy()
        {
            throw new NotImplementedException();
        }

        public override string ToString()
        {
            throw new NotImplementedException();
        }
    }
}
