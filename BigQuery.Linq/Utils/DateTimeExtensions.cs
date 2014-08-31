using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    public static class DateTimeExtensions
    {
        static readonly DateTime unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        static readonly DateTimeOffset unixEpochOffset = new DateTimeOffset(unixEpoch);

        public static long ToBigQueryTimestamp(this DateTime target)
        {
            return (long)(target - unixEpoch).TotalMilliseconds * 1000;
        }

        public static long ToBigQueryTimestamp(this DateTimeOffset target)
        {
            return (long)(target - unixEpochOffset).TotalMilliseconds * 1000;
        }
    }
}