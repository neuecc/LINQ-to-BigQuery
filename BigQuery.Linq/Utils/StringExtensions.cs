using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    internal static class StringExtensions
    {
        public static string UnescapeBq(this string value)
        {
            return value.TrimStart('[').TrimEnd(']');
        }

        public static string EscapeBq(this string value)
        {
            return '[' + value.UnescapeBq() + ']';
        }
    }
}