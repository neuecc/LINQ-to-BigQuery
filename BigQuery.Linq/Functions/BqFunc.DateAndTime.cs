using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    public enum IntervalUnit
    {
        Year,
        Month,
        Day,
        Hour,
        Minute,
        Second
    }

    // note:DateTimeOffset? is so dangerous!
    // I found following code cause C# compiler crash!
    // Expression<Func<DateTimeOffset?>> expr = () => new DateTime(2014, 10, 13);
    // therefore I can't change method signature to nullable.

    public static partial class BqFunc
    {
        // https://developers.google.com/bigquery/query-reference#datetimefunctions

        // The following functions enable date and time manipulation for UNIX timestamps, date strings and TIMESTAMP data types.
        // For more information about working with the TIMESTAMP data type, see Using TIMESTAMP.
        // Date and time functions that work with UNIX timestamps operate on UNIX time.
        // Date and time functions return values based upon the UTC time zone.

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
        public static string Date(DateTimeOffset timestamp) // DateTimeOffset? cause compiler error.
        {
            throw Invalid();
        }

        /// <summary>Adds the specified interval to a TIMESTAMP data type. Possible interval_units values include YEAR, MONTH, DAY, HOUR, MINUTE, and SECOND. If interval is a negative number, the interval is subtracted from the TIMESTAMP data type.</summary>
        [FunctionName("DATE_ADD", SpecifiedFormatterType = typeof(DateAddFormatter))]
        public static DateTimeOffset DateAdd(DateTimeOffset timestamp, int interval, IntervalUnit intervalUnit)
        {
            throw Invalid();
        }

        /// <summary>Returns the number of days between two TIMESTAMP data types.</summary>
        [FunctionName("DATEDIFF")]
        public static long DateDiff(DateTimeOffset timestamp1, DateTimeOffset? timestamp2)
        {
            throw Invalid();
        }

        /// <summary>Returns the day of the month of a TIMESTAMP data type as an integer between 1 and 31, inclusively.</summary>
        [FunctionName("DAY")]
        public static long Day(DateTimeOffset timestamp)
        {
            throw Invalid();
        }

        /// <summary>Returns the day of the week of a TIMESTAMP data type as an integer between 1 (Sunday) and 7 (Saturday), inclusively.</summary>
        [FunctionName("DAYOFWEEK")]
        public static long DayOfWeek(DateTimeOffset timestamp)
        {
            throw Invalid();
        }

        /// <summary>Returns the day of the year of a TIMESTAMP data type as an integer between 1 and 366, inclusively. The integer 1 refers to January 1.</summary>
        [FunctionName("DAYOFYEAR")]
        public static long DayOfYear(DateTimeOffset timestamp)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns a human-readable string representation of a UNIX timestamp in the format YYYY-MM-DD HH:MM:SS.uuuuuu.
        /// </summary>
        [FunctionName("FORMAT_UTC_USEC")]
        public static string FormatUtcUsec(long? unixTimestamp)
        {
            throw Invalid();
        }

        /// <summary>Returns the hour of a TIMESTAMP data type as an integer between 0 and 23, inclusively.</summary>
        [FunctionName("HOUR")]
        public static long Hour(DateTimeOffset timestamp)
        {
            throw Invalid();
        }

        /// <summary>Returns the minutes of a TIMESTAMP data type as an integer between 0 and 59, inclusively.</summary>
        [FunctionName("MINUTE")]
        public static long Minute(DateTimeOffset timestamp)
        {
            throw Invalid();
        }

        /// <summary>Returns the month of a TIMESTAMP data type as an integer between 1 and 12, inclusively.</summary>
        [FunctionName("MONTH")]
        public static long Month(DateTimeOffset timestamp)
        {
            throw Invalid();
        }

        /// <summary>
        /// <para>Returns the seconds of a TIMESTAMP data type as an integer between 0 and 59, inclusively.</para>
        /// <para>During a leap second, the integer range is between 0 and 60, inclusively.</para>
        /// </summary>
        [FunctionName("SECOND")]
        public static long Second(DateTimeOffset timestamp)
        {
            throw Invalid();
        }

        /// <summary>Returns the year of a TIMESTAMP data type.</summary>
        [FunctionName("YEAR")]
        public static long Year(DateTimeOffset timestamp)
        {
            throw Invalid();
        }

        /// <summary>Converts a UNIX timestamp in milliseconds to a TIMESTAMP data type.</summary>
        [FunctionName("MSEC_TO_TIMESTAMP")]
        public static DateTimeOffset MsecToTimestamp(long? milliseconds)
        {
            throw Invalid();
        }

        /// <summary>Returns a UNIX timestamp in microseconds.</summary>
        [FunctionName("NOW")]
        public static long Now()
        {
            throw Invalid();
        }

        /// <summary>
        /// <para>Converts a date string to a UNIX timestamp in microseconds.</para>
        /// <para>date_string must have the format YYYY-MM-DD HH:MM:SS[.uuuuuu].</para>
        /// <para>The fractional part of the second can be up to 6 digits long or can be omitted.</para>
        /// <para>TIMESTAMP_TO_USEC is an equivalent function that converts a TIMESTAMP data type argument instead of a date string.</para>
        /// </summary>
        [FunctionName("PARSE_UTC_USEC")]
        public static long ParseUtcUsec(string dateString)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns the quarter of the year of a TIMESTAMP data type as an integer between 1 and 4, inclusively.
        /// </summary>
        [FunctionName("QUARTER")]
        public static long Quarter(DateTimeOffset timestamp)
        {
            throw Invalid();
        }

        /// <summary>
        /// Converts a UNIX timestamp in seconds to a TIMESTAMP data type.
        /// </summary>
        [FunctionName("SEC_TO_TIMESTAMP")]
        public static DateTimeOffset SecToTimestamp(long? unixTimestampSecond)
        {
            throw Invalid();
        }


        /// <summary>
        /// <para>Returns a human-readable date string in the format date_format_str. date_format_str can include date-related punctuation characters (such as / and -) and special characters accepted by the strftime function in C++ (such as %d for day of month).</para>
        /// <para>Use the UTC_USEC_TO_&lt;function_name&gt; functions if you plan to group query data by time intervals, such as getting all data for a certain month, because the functions are more efficient.</para>
        /// </summary>
        [FunctionName("STRFTIME_UTC_USEC")]
        public static string StrftimeUtcUsec(long? unixTimestamp, string dateFormat)
        {
            throw Invalid();
        }

        /// <summary>Returns a human-readable string of a TIMESTAMP data type, in the format %H:%M:%S.</summary>
        [FunctionName("TIME")]
        public static string Time(DateTimeOffset timestamp)
        {
            throw Invalid();
        }

        /// <summary>Convert a date string to a TIMESTAMP data type.</summary>
        [FunctionName("TIMESTAMP")]
        public static DateTimeOffset Timestamp(string dateString)
        {
            throw Invalid();
        }

        /// <summary>Converts a TIMESTAMP data type to a UNIX timestamp in milliseconds.</summary>
        [FunctionName("TIMESTAMP_TO_MSEC")]
        public static long TimestampToMsec(DateTimeOffset timestamp)
        {
            throw Invalid();
        }

        /// <summary>Converts a TIMESTAMP data type to a UNIX timestamp in seconds.</summary>
        [FunctionName("TIMESTAMP_TO_SEC")]
        public static long TimestampToSec(DateTimeOffset timestamp)
        {
            throw Invalid();
        }

        /// <summary>
        /// <para>Converts a TIMESTAMP data type to a UNIX timestamp in microseconds.</para>
        /// <para>PARSE_UTC_USEC is an equivalent function that converts a data string argument instead of a TIMESTAMP data type.</para>
        /// </summary>
        [FunctionName("TIMESTAMP_TO_USEC")]
        public static long TimestampToUsec(DateTimeOffset timestamp)
        {
            throw Invalid();
        }

        /// <summary>
        /// <para>Converts a UNIX timestamp in microseconds to a TIMESTAMP data type.</para>
        /// </summary>
        [FunctionName("USEC_TO_TIMESTAMP")]
        public static DateTimeOffset UsecToTimestamp(long? unixTimestamp)
        {
            throw Invalid();
        }

        /// <summary>
        /// <para>Shifts a UNIX timestamp in microseconds to the beginning of the day it occurs in.</para>
        /// <para>For example, if unix_timestamp occurs on May 19th at 08:58, this function returns a UNIX timestamp for May 19th at 00:00 (midnight).</para>
        /// </summary>
        [FunctionName("UTC_USEC_TO_DAY")]
        public static long UtcUsecToDay(long? unixTimestamp)
        {
            throw Invalid();
        }

        /// <summary>
        /// <para>Shifts a UNIX timestamp in microseconds to the beginning of the hour it occurs in.</para>
        /// <para>For example, if unix_timestamp occurs at 08:58, this function returns a UNIX timestamp for 08:00 on the same day.</para>
        /// </summary>
        [FunctionName("UTC_USEC_TO_HOUR")]
        public static long UtcUsecToHour(long? unixTimestamp)
        {
            throw Invalid();
        }

        /// <summary>
        /// <para>Shifts a UNIX timestamp in microseconds to the beginning of the month it occurs in.</para>
        /// <para>For example, if unix_timestamp occurs on March 19th, this function returns a UNIX timestamp for March 1st of the same year.</para>
        /// </summary>
        [FunctionName("UTC_USEC_TO_MONTH")]
        public static long UtcUsecToMonth(long? unixTimestamp)
        {
            throw Invalid();
        }

        /// <summary>
        /// <para>Returns a UNIX timestamp in microseconds that represents a day in the week of the unix_timestamp argument. This function takes two arguments: a UNIX timestamp in microseconds, and a day of the week from 0 (Sunday) to 6 (Saturday).</para>
        /// <para>For example, if unix_timestamp occurs on Friday, 2008-04-11, and you set day_of_week to 2 (Tuesday), the function returns a UNIX timestamp for Tuesday, 2008-04-08.</para>
        /// </summary>
        [FunctionName("UTC_USEC_TO_WEEK")]
        public static long UtcUsecToWeek(long? unixTimestamp, long dayOfWeek)
        {
            throw Invalid();
        }

        /// <summary>
        /// <para>Returns a UNIX timestamp in microseconds that represents the year of the unix_timestamp argument.</para>
        /// <para>For example, if unix_timestamp occurs in 2010, the function returns 1274259481071200, the microsecond representation of 2010-01-01 00:00.</para>
        /// </summary>
        [FunctionName("UTC_USEC_TO_YEAR")]
        public static long UtcUsecToYear(long? unixTimestamp)
        {
            throw Invalid();
        }

        class DateAddFormatter : ISpeficiedFormatter
        {
            public string Format(int depth, int indentSize, string fuctionName, MethodCallExpression node)
            {
                var innerTranslator = new BigQueryTranslateVisitor();

                var timestamp = innerTranslator.VisitAndClearBuffer(node.Arguments[0]);
                var interval = innerTranslator.VisitAndClearBuffer(node.Arguments[1]);
                var intervalUnit = ((IntervalUnit)(node.Arguments[2] as ConstantExpression).Value).ToString().ToUpper();

                return string.Format("DATE_ADD({0}, {1}, '{2}')", timestamp, interval, intervalUnit);
            }
        }
    }
}
