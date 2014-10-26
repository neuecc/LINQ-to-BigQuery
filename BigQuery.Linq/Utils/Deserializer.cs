using Google.Apis.Bigquery.v2.Data;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    public delegate bool CustomDeserializeFallback(PropertyInfo targetProperty, object value, out object deserializedValue);

    internal abstract class Deserializer
    {
        protected static readonly HashSet<Type> ParsableType = new HashSet<Type>
        {
            typeof(Int16),
            typeof(Int32),
            typeof(Int64),
            typeof(UInt16),
            typeof(UInt32),
            typeof(UInt64),
            typeof(Single),
            typeof(Double),
            typeof(SByte),
            typeof(Byte),
            typeof(Boolean),
            typeof(DateTime),
            typeof(DateTimeOffset),
            typeof(Nullable<Int16>),
            typeof(Nullable<Int32>),
            typeof(Nullable<Int64>),
            typeof(Nullable<UInt16>),
            typeof(Nullable<UInt32>),
            typeof(Nullable<UInt64>),
            typeof(Nullable<Single>),
            typeof(Nullable<Double>),
            typeof(Nullable<SByte>),
            typeof(Nullable<Byte>),
            typeof(Nullable<Boolean>),
            typeof(Nullable<DateTime>),
            typeof(Nullable<DateTimeOffset>),
            typeof(String),
        };

        protected static readonly Regex ExtractAnonymousFieldNameRegex = new Regex("<(.+)>", RegexOptions.Compiled);

        static protected object Parse(string type, string value)
        {
            if (value == null) return null;

            switch (type)
            {
                case "STRING":
                    return value;
                case "INTEGER":
                    return long.Parse(value, CultureInfo.InvariantCulture);
                case "FLOAT":
                    return double.Parse(value, CultureInfo.InvariantCulture);
                case "BOOLEAN":
                    return bool.Parse(value);
                case "TIMESTAMP":
                    var v = double.Parse(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture);
                    var xs = v.Split('.');
                    var intergralPart = xs[0];
                    var decimalPart = (xs.Length == 1)
                        ? "000000"
                        : xs[1].Substring(0, Math.Min(xs[1].Length, 6)).PadRight(6, '0'); // Truncate Len:6
                    var l = long.Parse(intergralPart + decimalPart, CultureInfo.InvariantCulture);
                    return DateTimeExtensions.FromBigQueryTimestamp(l);
                case "RECORD":
                    throw new InvalidOperationException("Deserializer can't need support record type. If you encount this error, please write to GitHub issues.");
                default:
                    throw new InvalidOperationException("UNKNOWN TYPE:" + type);
            }
        }

        internal static dynamic DeserializeDynamic(TableSchema schema, TableRow row)
        {
            if (row.F.Count == 1 && DataTypeUtility.Parse(schema.Fields[0].Type) != DataType.Record)
            {
                var field = schema.Fields[0];
                var value = row.F[0].V;
                var parsedValue = Parse(field.Type, (string)value);

                return parsedValue;
            }

            IDictionary<string, object> container = new ExpandoObject();
            for (int i = 0; i < row.F.Count; i++)
            {
                var field = schema.Fields[i];
                var value = row.F[i].V;
                var parsedValue = Parse(field.Type, (string)value);

                container.Add(field.Name, parsedValue);
            }
            return container;
        }
    }

    internal class Deserializer<T> : Deserializer
    {
        readonly TableSchema schema;
        readonly Dictionary<string, PropertyInfo> typeInfo;
        readonly Dictionary<string, FieldInfo> fallbackFieldInfo;
        readonly CustomDeserializeFallback customDeserializeFallback;

        public Deserializer(TableSchema schema, CustomDeserializeFallback customDeserializeFallback)
        {
            this.schema = schema;
            this.typeInfo = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetProperty | BindingFlags.SetProperty | BindingFlags.Instance)
                .ToDictionary(x => x.Name, StringComparer.InvariantCultureIgnoreCase);
            this.fallbackFieldInfo = typeof(T).GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.GetField | BindingFlags.SetField | BindingFlags.Instance)
                .Where(x => x.Name.StartsWith("<"))
                .ToDictionary(x => ExtractAnonymousFieldNameRegex.Match(x.Name).Groups[1].Value, StringComparer.InvariantCultureIgnoreCase);
            this.customDeserializeFallback = customDeserializeFallback;
        }

        public T Deserialize(TableRow row)
        {
            if (row.F.Count == 1 && ParsableType.Contains(typeof(T)))
            {
                var field = schema.Fields[0];
                var value = row.F[0].V;
                var parsedValue = Parse(field.Type, (string)value);

                object v = (parsedValue == null) ? null
                    : ((typeof(T) == typeof(DateTime)) || (typeof(T) == typeof(DateTime?))) ? ((DateTimeOffset)parsedValue).UtcDateTime
                    : typeof(T).IsNullable() ? Convert.ChangeType(parsedValue, typeof(T).GetGenericArguments()[0], CultureInfo.InvariantCulture)
                    : Convert.ChangeType(parsedValue, typeof(T), CultureInfo.InvariantCulture);
                return (T)v;
            }

            var result = (T)System.Runtime.Serialization.FormatterServices.GetUninitializedObject(typeof(T));

            for (int i = 0; i < row.F.Count; i++)
            {
                var field = schema.Fields[i];
                var value = row.F[i].V;
                var parsedValue = Parse(field.Type, (string)value);

                PropertyInfo propertyInfo;
                if (typeInfo.TryGetValue(field.Name, out propertyInfo))
                {
                    object v;
                    object customResult;
                    if (customDeserializeFallback != null && customDeserializeFallback(propertyInfo, parsedValue, out customResult))
                    {
                        v = customResult;
                    }
                    else
                    {
                        v = (parsedValue == null) ? null
                            : ((typeof(T) == typeof(DateTime)) || (typeof(T) == typeof(DateTime?))) ? ((DateTimeOffset)parsedValue).UtcDateTime
                            : propertyInfo.PropertyType.IsNullable() ? Convert.ChangeType(parsedValue, propertyInfo.PropertyType.GetGenericArguments()[0], CultureInfo.InvariantCulture)
                            : Convert.ChangeType(parsedValue, propertyInfo.PropertyType,CultureInfo.InvariantCulture);
                    }

                    if (propertyInfo.GetSetMethod(true) != null)
                    {
                        propertyInfo.SetValue(result, v);
                    }
                    else
                    {
                        // for anonymous type
                        var fieldInfo = fallbackFieldInfo[field.Name];
                        fieldInfo.SetValue(result, v);
                    }
                }
            }
            return result;
        }
    }
}