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

        public LimitBigQueryable<T> Limit()
        {
            throw new NotImplementedException();
        }

        public OrderByBigQueryable<T> ThenBy()
        {
            throw new NotImplementedException();
        }
    }
}
