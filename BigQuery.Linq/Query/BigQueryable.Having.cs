using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Query
{
    public class HavingBigQueryable<T> : LimitBigQueryable<T>
    {
        internal HavingBigQueryable(BigQueryable parent) : base(parent) { }

        public LimitBigQueryable<T> Limit(int numRows)
        {
            if (numRows < 0) throw new ArgumentOutOfRangeException("numRows:" + numRows);

            return new LimitBigQueryable<T>(this, numRows);
        }

        public OrderByBigQueryable<T> OrderBy()
        {
            throw new NotImplementedException();
        }
    }

}
