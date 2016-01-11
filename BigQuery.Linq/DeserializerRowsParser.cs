using System;
using System.Collections.Generic;
using System.Linq;
using Google.Apis.Bigquery.v2.Data;

namespace BigQuery.Linq
{
    public class DeserializerRowsParser : IRowsParser
    {
        public IEnumerable<T> Parse<T>(Dictionary<Type, CustomDeserializeFallback> fallbacks, bool isConvertResultUtcToLocalTime, TableSchema schema, IEnumerable<TableRow> rows, bool isDynamic)
        {
            if (!isDynamic)
            {
                CustomDeserializeFallback fallback;
                fallbacks.TryGetValue(typeof(T), out fallback);

                var deserializer = new Deserializer<T>(schema, fallback);
                return rows.Select(row => deserializer.Deserialize(row, isConvertResultUtcToLocalTime));
            }

            return rows.Select(row => Deserializer.DeserializeDynamic(schema, row)).Cast<T>(); // T as dynamic...
        } 
    }
}