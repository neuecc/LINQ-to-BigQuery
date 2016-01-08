using System.Collections.Generic;
using System.Linq;
using Google.Apis.Bigquery.v2.Data;

namespace BigQuery.Linq
{
    public class DeserializerRowsParser : IRowsParser
    {
        private readonly BigQueryContext _context;

        public IEnumerable<T> Parse<T>(TableSchema schema, IEnumerable<TableRow> rows, bool isDynamic)
        {
            if (!isDynamic)
            {
                CustomDeserializeFallback fallback;
                _context.fallbacks.TryGetValue(typeof(T), out fallback);

                var deserializer = new Deserializer<T>(schema, fallback);
                return rows.Select(row => deserializer.Deserialize(row, _context.IsConvertResultUtcToLocalTime));
            }

            return rows.Select(row => Deserializer.DeserializeDynamic(schema, row)).Cast<T>(); // T as dynamic...
        } 
    }
}