using Google.Apis.Bigquery.v2.Data;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    public enum DataType
    {
        String,
        Integer, // long
        Float, // double
        Boolean,
        Timestamp, // DateTimeOffset
        Record // object
    }

    internal static class DataTypeFormatter
    {
        internal static string Format(object value)
        {
            if (value == null) return "NULL";

            var tc = Type.GetTypeCode(value.GetType());
            switch (tc)
            {
                case TypeCode.Boolean:
                    var b = (bool)value;
                    return (b == true) ? "true" : "false";
                case TypeCode.Char:
                case TypeCode.String:
                    return "\'" + value + "\'";
                case TypeCode.DateTime:
                    return "\'" + string.Format("{0:yyyy-MM-dd hh:mm:ss.ffffff}", value) + "\'";
                case TypeCode.DBNull:
                case TypeCode.Empty:
                    return "NULL";
                case TypeCode.Decimal:
                case TypeCode.Single:
                case TypeCode.Double:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.SByte:
                case TypeCode.Byte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                    if (value.GetType().IsEnum)
                    {
                        return "\'" + value + "\'";
                    }
                    else
                    {
                        return value.ToString();
                    }
                case TypeCode.Object:
                    if (value.GetType() == typeof(DateTimeOffset))
                    {
                        return "\'" + string.Format("{0:yyyy-MM-dd hh:mm:ss.ffffff}", value) + "\'";
                    }
                    throw new NotImplementedException("Currently not supported object(as record?)");
                default:
                    throw new InvalidOperationException();
            }
        }
    }
}