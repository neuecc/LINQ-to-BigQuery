using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    internal static class StringExtensions
    {
        public static string TrimSmart(this string s)
        {
            return s.Trim('\r', '\n', ' ');
        }

        public static string TrimFlat(this string s)
        {
            return s.Replace("\r\n", " ").Trim('\r', '\n', ' ');
        }

        public static string ToSql(this IBigQueryable q)
        {
            return q.ToString().TrimFlat();
        }
    }
}
