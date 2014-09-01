using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    public enum IntervalUnit
    {
        YEAR, MONTH, DAY, HOUR, MINUTE, SECOND
    }

    public static partial class BqFunc
    {
        /// <summary>Returns a human-readable string of the current date in the format %Y-%m-%d.</summary>
        [FunctionName("CURRENT_DATE")]
        public static string CurrentDate()
        {
            throw Invalid();
        }

        /// <summary>Returns a human-readable string of the server's current time in the format %H:%M:%S.</summary>
        [FunctionName("CURRENT_TIME")]
        public static string CurrentTime()
        {
            throw Invalid();
        }

        /// <summary>Returns a TIMESTAMP data type of the server's current time in the format %Y-%m-%d %H:%M:%S.</summary>
        [FunctionName("CURRENT_TIMESTAMP")]
        public static DateTimeOffset CurrentTimestamp()
        {
            throw Invalid();
        }

        /// <summary>Returns a human-readable string of a TIMESTAMP data type in the format %Y-%m-%d.</summary>
        [FunctionName("DATE")]
        public static string Date(DateTimeOffset timestamp)
        {
            throw Invalid();
        }

        /// <summary>Adds the specified interval to a TIMESTAMP data type. Possible interval_units values include YEAR, MONTH, DAY, HOUR, MINUTE, and SECOND. If interval is a negative number, the interval is subtracted from the TIMESTAMP data type.</summary>
        [FunctionName("DATE_ADD")]
        public static DateTimeOffset DateAdd(DateTimeOffset timestamp, int interval, IntervalUnit intervalUnit)
        {
            throw Invalid();
        }

        /// <summary>Returns the number of days between two TIMESTAMP data types.</summary>
        [FunctionName("DATEDIFF")]
        public static long DateDiff(DateTimeOffset timestamp1, DateTimeOffset timestamp2)
        {
            throw Invalid();
        }

        // ---------------

        /// <summary>Convert a date string to a TIMESTAMP data type.</summary>
        [FunctionName("TIMESTAMP")]
        public static DateTimeOffset Timestamp(string dateString)
        {
            throw Invalid();
        }
    }
}
