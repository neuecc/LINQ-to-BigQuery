using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Query
{
    internal enum DecorateType
    {
        Snapshot,
        Range
    }

    internal class TableDecoratorBigQueryable<T> : BigQueryable, ITableDecoratorBigQueryable<T>
    {
        public static DateTime Zero = DateTime.MinValue;

        readonly DecorateType Type;
        readonly DateTime Time1;
        readonly DateTime Time2;

        internal TableDecoratorBigQueryable(IBigQueryable parent, DecorateType type, DateTime time1)
            : base(parent)
        {
            this.Type = type;
            this.Time1 = time1;
        }

        internal TableDecoratorBigQueryable(IBigQueryable parent, DecorateType type, DateTime time1, DateTime time2)
            : base(parent)
        {
            this.Type = type;
            this.Time1 = time1;
            this.Time2 = time2;
        }

        public override string ToString(int depth, int indentSize, FormatOption option)
        {
            throw new NotImplementedException();
        }
    }
}