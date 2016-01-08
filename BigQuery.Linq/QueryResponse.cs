using Google.Apis.Bigquery.v2.Data;
using System;
using System.Collections.Generic;
using System.Linq;

namespace BigQuery.Linq
{
    public class QueryResponse<T>
    {
        public string Query { get; }
        public bool? CacheHit { get; }
        public string ETag { get; }
        public string Kind { get; }
        public string PageToken { get; }
        [Obsolete("Access via FirstPage property instead")]
        public T[] Rows { get; }
        public long? TotalBytesProcessed { get; }
        public string TotalBytesProcessedFormatted { get; }
        [Obsolete("Access via FirstPage property instead")]
        public ulong? TotalRows { get; }
        [Obsolete("Access via FirstPage property instead")]
        public TimeSpan ExecutionTime { get; }
        public IList<TableFieldSchema> TableFieldSchemas { get; }
        public IQueryResponsePage<T> FirstPage { get; }

        internal QueryResponse(BigQueryContext context, string query, TimeSpan executionTime, QueryResponse queryResponse, bool isDynamic, IRowsParser rowsParser)
        {
            var rows = queryResponse.Rows == null
                ? new T[0]
                : rowsParser.Parse<T>(queryResponse.Schema, queryResponse.Rows, isDynamic).ToArray();

            var schemas = (queryResponse.Schema == null)
                ? new TableFieldSchema[0]
                : queryResponse.Schema.Fields;

            Query = query;
            CacheHit = queryResponse.CacheHit;
            ETag = queryResponse.ETag;
            Kind = queryResponse.Kind;
            PageToken = queryResponse.PageToken;
            Rows = rows;
            TotalBytesProcessed = queryResponse.TotalBytesProcessed;
            TotalRows = queryResponse.TotalRows;
            ExecutionTime = executionTime;
            TableFieldSchemas = schemas;

            TotalBytesProcessedFormatted = queryResponse.TotalBytesProcessed.ToHumanReadableSize();

            FirstPage = new QueryResponsePage<T>(
                context,
                rowsParser, queryResponse.JobReference, queryResponse.TotalRows, queryResponse.JobComplete, queryResponse.PageToken, queryResponse.Schema, queryResponse.Rows, executionTime, isDynamic);
        }

    }
}