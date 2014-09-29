using Google.Apis.Bigquery.v2.Data;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    public class QueryResponse<T>
    {
        public string Query { get; private set; }
        public bool? CacheHit { get; private set; }
        public string ETag { get; private set; }
        public string Kind { get; private set; }
        public string PageToken { get; private set; }
        public T[] Rows { get; private set; }
        public long? TotalBytesProcessed { get; private set; }
        public string TotalBytesProcessedFormatted { get; private set; }
        public ulong? TotalRows { get; private set; }
        public TimeSpan ExecutionTime { get; private set; }
        public IList<TableFieldSchema> TableFieldSchemas { get; private set; }

        internal QueryResponse(string query, TimeSpan executionTime, QueryResponse queryResponse, bool isDynamic)
        {
            T[] rows;
            if (queryResponse.Rows == null)
            {
                rows = new T[0];
            }
            else if (!isDynamic)
            {
                var deserializer = new Deserializer<T>(queryResponse.Schema);
                rows = queryResponse.Rows.Select(row => deserializer.Deserialize(row)).ToArray();
            }
            else
            {
                rows = queryResponse.Rows.Select(row => Deserializer.DeserializeDynamic(queryResponse.Schema, row))
                    .Cast<T>() // T as dynamic...
                    .ToArray();
            }
            var schemas = (queryResponse.Schema == null)
                ? new TableFieldSchema[0]
                : queryResponse.Schema.Fields;

            this.Query = query;
            this.CacheHit = queryResponse.CacheHit;
            this.ETag = queryResponse.ETag;
            this.Kind = queryResponse.Kind;
            this.PageToken = queryResponse.PageToken;
            this.Rows = rows;
            this.TotalBytesProcessed = queryResponse.TotalBytesProcessed;
            this.TotalRows = queryResponse.TotalRows;
            this.ExecutionTime = executionTime;
            this.TableFieldSchemas = schemas;

            this.TotalBytesProcessedFormatted = queryResponse.TotalBytesProcessed.ToHumanReadableSize();
        }
    }
}