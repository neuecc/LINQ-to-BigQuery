using Google.Apis.Bigquery.v2.Data;
using System;
using System.Collections.Generic;
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

        internal QueryResponse(string query, TimeSpan executionTime, QueryResponse queryResponse)
        {
            var deserializer = new Deserializer<T>(queryResponse.Schema);
            var rows = (queryResponse.Rows == null)
                ? new T[0]
                : queryResponse.Rows.Select(row => deserializer.Deserialize(row)).ToArray();
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