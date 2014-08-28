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
        readonly TimeSpan? RelativeTime1;
        readonly TimeSpan? RelativeTime2;
        readonly DateTime? AbsoluteTime1;
        readonly DateTime? AbsoluteTime2;

        internal TableDecoratorBigQueryable(IFromBigQueryable<T> parent, DecorateType type, DateTime? absoluteTime1 = null, DateTime? absoluteTime2 = null, TimeSpan? relativeTime1 = null, TimeSpan? relativeTime2 = null)
            : base(parent)
        {
            this.Type = type;
            this.AbsoluteTime1 = absoluteTime1;
            this.AbsoluteTime2 = absoluteTime2;
            this.RelativeTime1 = relativeTime1;
            this.RelativeTime2 = relativeTime2;
        }

        public override string ToString(int depth, int indentSize, FormatOption option)
        {
            var parent = (FromBigQueryable<T>)Parent;

            var sb = new StringBuilder();

            var tableName = parent.tableName.Trim('[', ']');

            sb.AppendLine("FROM").Append("  [").Append(tableName).Append("@");


            if (Type == DecorateType.Snapshot)
            {
                if (RelativeTime1 != null)
                {
                    sb.Append("-").Append(Math.Floor(RelativeTime1.Value.TotalMilliseconds));
                }
                else if (AbsoluteTime1 == Zero)
                {
                    sb.Append("0");
                }
                else
                {
                    sb.Append(AbsoluteTime1.Value.ToBigQueryTimestamp());
                }
            }
            else
            {
                if (RelativeTime1 != null)
                {
                    sb.Append("-").Append(Math.Floor(RelativeTime1.Value.TotalMilliseconds));
                }
                else if (AbsoluteTime1.HasValue)
                {
                    sb.Append(AbsoluteTime1.Value.ToBigQueryTimestamp());
                }
                sb.Append("-");
                if (RelativeTime2 != null)
                {
                    sb.Append("-").Append(Math.Floor(RelativeTime2.Value.TotalMilliseconds));
                }
                else if (AbsoluteTime2.HasValue)
                {
                    sb.Append(AbsoluteTime2.Value.ToBigQueryTimestamp());
                }
            }

            sb.Append("]");

            return sb.ToString();
        }
    }
}