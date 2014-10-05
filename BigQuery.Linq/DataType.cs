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

    public static class DataTypeUtility
    {
        public static DataType Parse(string dataTypeFormat)
        {
            DataType dataType;
            if (TryParse(dataTypeFormat, out dataType))
            {
                return dataType;
            }
            else
            {
                throw new FormatException("invalid type format:" + dataTypeFormat);
            }
        }

        public static bool TryParse(string dataTypeFormat, out DataType dataType)
        {
            switch (dataTypeFormat)
            {
                case "STRING":
                    dataType = DataType.String;
                    break;
                case "INTEGER":
                    dataType = DataType.Integer;
                    break;
                case "FLOAT":
                    dataType = DataType.Float;
                    break;
                case "BOOLEAN":
                    dataType = DataType.Boolean;
                    break;
                case "TIMESTAMP":
                    dataType = DataType.Timestamp;
                    break;
                case "RECORD":
                    dataType = DataType.Record;
                    break;
                default:
                    dataType = (DataType)0;
                    return false;
            }
            return true;
        }

        public static string ToIdentifier(this DataType type)
        {
            switch (type)
            {
                case DataType.String:
                    return "STRING";
                case DataType.Integer:
                    return "INTEGER";
                case DataType.Float:
                    return "FLOAT";
                case DataType.Boolean:
                    return "BOOLEAN";
                case DataType.Timestamp:
                    return "TIMESTAMP";
                case DataType.Record:
                    return "RECORD";
                default:
                    throw new ArgumentException("invalid type:" + type);
            }
        }

        public static DataType ToDataType<T>()
        {
            return ToDataType(typeof(T));
        }

        public static DataType ToDataType(Type type)
        {
            var isNulable = type.IsNullable();
            if (isNulable)
            {
                type = type.GetGenericArguments()[0];
            }
            if (type.IsArray)
            {
                type = type.GetElementType();
            }

            var tc = Type.GetTypeCode(type);
            switch (tc)
            {
                case TypeCode.Boolean:
                    return DataType.Boolean;
                case TypeCode.Char:
                case TypeCode.String:
                    return DataType.String;
                case TypeCode.DateTime:
                    return DataType.Timestamp;
                case TypeCode.DBNull:
                case TypeCode.Empty:
                    throw new ArgumentException("not supported type");
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
                    return DataType.Integer;
                case TypeCode.Object:
                    if (type == typeof(DateTimeOffset))
                    {
                        return DataType.Timestamp;
                    }
                    return DataType.Record;
                default:
                    return DataType.Record;
            }
        }

        public static TableFieldSchema[] ToTableFieldSchema<T>()
        {
            return ToTableFieldSchema(typeof(T));
        }

        public static TableFieldSchema[] ToTableFieldSchema<T>(T dynamicSchema)
        {
            return ToTableFieldSchema(typeof(T));
        }

        public static TableFieldSchema[] ToTableFieldSchema(Type type)
        {
            return type.GetProperties().Select(x =>
            {
                var isNulable = x.PropertyType.IsNullable();
                var isArray = x.PropertyType.IsArray;
                var dataType = ToDataType(x.PropertyType);

                var schema = new TableFieldSchema
                {
                    Name = x.Name,
                    Type = dataType.ToIdentifier(),
                    Mode = isArray ? "REPEATED"
                         : isNulable ? "NULLABLE"
                         : "REQUIRED"
                };

                if (dataType == DataType.Record)
                {
                    schema.Fields = ToTableFieldSchema(x.PropertyType);
                }

                return schema;
            }).ToArray();
        }
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
                    // escape
                    var str = value.ToString()
                        .Replace("\"", "\\\"") // "
                        .Replace("\'", "\\\'");// '

                    return "\'" + str + "\'";
                case TypeCode.DateTime:
                    return "\'" + string.Format("{0:yyyy-MM-dd HH:mm:ss.ffffff}", value) + "\'";
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
                        var underlyingType = Enum.GetUnderlyingType(value.GetType());
                        return Convert.ChangeType(value, underlyingType).ToString();
                    }
                    else
                    {
                        return value.ToString();
                    }
                case TypeCode.Object:
                    if (value.GetType() == typeof(DateTimeOffset))
                    {
                        return "\'" + string.Format("{0:yyyy-MM-dd HH:mm:ss.ffffff}", value) + "\'";
                    }
                    throw new InvalidOperationException(value.GetType() + " can't format BigQuery SQL string.");
                default:
                    throw new InvalidOperationException();
            }
        }
    }
}