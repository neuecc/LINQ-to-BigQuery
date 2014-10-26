using Google.Apis.Bigquery.v2.Data;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
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
                case TypeCode.Single:
                case TypeCode.Double:
                    return DataType.Float;
                case TypeCode.Decimal:
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
            return ToTableFieldSchema(type, _ => null);
        }

        /// <param name="customFieldSchemaSelector">Use custom fallback. If return null, use default fieldschema.</param>
        public static TableFieldSchema[] ToTableFieldSchema(Type type, Func<PropertyInfo, TableFieldSchema> customFieldSchemaSelector)
        {
            return type.GetProperties()
                .Select(x =>
                {
                    if (x.GetCustomAttribute<JsonIgnoreAttribute>() != null)
                    {
                        return null;
                    }

                    var customSchema = customFieldSchemaSelector(x);
                    if (customSchema != null)
                    {
                        return customSchema;
                    }

                    var isNulable = x.PropertyType.IsNullable();
                    var isArray = x.PropertyType.IsArray;

                    var dataType = ToDataType(x.PropertyType);
                    var isRecord = dataType == DataType.Record;

                    var jsonName = x.GetCustomAttribute<JsonPropertyAttribute>();

                    var schema = new TableFieldSchema
                    {
                        Name = (jsonName != null) ? jsonName.PropertyName : x.Name,
                        Type = dataType.ToIdentifier(),
                        Mode = (isArray) ? "REPEATED"
                             : (isNulable || isRecord || dataType == DataType.String) ? "NULLABLE"
                             : "REQUIRED"
                    };

                    if (isRecord)
                    {
                        if (isArray)
                        {
                            schema.Fields = ToTableFieldSchema(x.PropertyType.GetElementType(), customFieldSchemaSelector);
                        }
                        else
                        {
                            schema.Fields = ToTableFieldSchema(x.PropertyType, customFieldSchemaSelector);
                        }
                    }

                    return schema;
                })
                .Where(x => x != null)
                .ToArray();
        }

        public static string ToJsonSchema(this TableFieldSchema schema, Formatting formatting = Formatting.Indented)
        {
            using (var sw = new StringWriter())
            using (var jw = new JsonTextWriter(sw))
            {
                jw.Formatting = formatting;
                WriteJsonSchema(jw, schema);
                return sw.ToString();
            }
        }

        public static string ToJsonSchema(this IEnumerable<TableFieldSchema> schemas, Formatting formatting = Formatting.Indented)
        {
            using (var sw = new StringWriter())
            using (var jw = new JsonTextWriter(sw))
            {
                jw.Formatting = formatting;
                jw.WriteStartArray();
                foreach (var item in schemas)
                {
                    WriteJsonSchema(jw, item);
                }
                jw.WriteEndArray();
                return sw.ToString();
            }
        }

        static void WriteJsonSchema(JsonTextWriter jw, TableFieldSchema schema)
        {
            jw.WriteStartObject();
            {
                jw.WritePropertyName("name");
                jw.WriteValue(schema.Name);
                if (schema.Description != null)
                {
                    jw.WritePropertyName("description");
                    jw.WriteValue(schema.Description);
                }
                if (schema.Mode != null)
                {
                    jw.WritePropertyName("mode");
                    jw.WriteValue(schema.Mode);
                }
                jw.WritePropertyName("type");
                jw.WriteValue(schema.Type);
                if (schema.Fields != null)
                {
                    jw.WritePropertyName("fields");
                    jw.WriteStartArray();
                    foreach (var item in schema.Fields)
                    {
                        WriteJsonSchema(jw, item);
                    }
                    jw.WriteEndArray();
                }
            }
            jw.WriteEndObject();
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
                        return Convert.ChangeType(value, underlyingType, CultureInfo.InvariantCulture).ToString();
                    }
                    else
                    {
                        if (tc == TypeCode.Double)
                        {
                            return ((double) value).ToString(CultureInfo.InvariantCulture);
                        }
                        else
                        {
                            return value.ToString();                            
                        }


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