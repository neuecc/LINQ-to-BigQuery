using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Query
{

    public class TableDecoratorBigQueryable<T> : JoinBigQueryable<T>
    {
        internal TableDecoratorBigQueryable(BigQueryable parent)
            : base(parent)
        {

        }

        public JoinBigQueryable<T> Join()
        {
            throw new NotImplementedException();
        }

    }
}
