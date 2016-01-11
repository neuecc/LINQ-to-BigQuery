using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Apis.Bigquery.v2.Data;

namespace BigQuery.Linq
{
    public class QueryResponsePage<T> : IQueryResponsePage<T>
    {
        private readonly BigQueryContext _context;
        private readonly JobReference _jobReference;
        private readonly string _pageToken;
        private readonly bool _isDynamic;
        private readonly IRowsParser _rowsParser;
        private readonly bool? _jobComplete;

        internal QueryResponsePage(BigQueryContext context, IRowsParser rowsParser, JobReference jobReference, ulong? totalRows, bool? jobComplete,
            string pageToken, TableSchema schema, IEnumerable<TableRow> rows, TimeSpan executionTime, bool isDynamic)
        {
            _context = context;
            _isDynamic = isDynamic;
            _rowsParser = rowsParser;
            _jobReference = jobReference;
            _pageToken = pageToken;
            _jobComplete = jobComplete;
            ExecutionTime = executionTime;
            TotalRows = totalRows;

            Rows = rows == null
                ? new T[0]
                : rowsParser.Parse<T>(context.fallbacks, context.IsConvertResultUtcToLocalTime, schema, rows, isDynamic).ToArray();
        }

        public TimeSpan ExecutionTime { get; }
        public T[] Rows { get; }
        public ulong? TotalRows { get; }

        public QueryResponsePage<T> NextPage()
        {
            if (!_jobComplete.GetValueOrDefault())
            {
                throw new InvalidOperationException("Can't get next page for a job that has not completed");
            }

            // there's only another page if the previous response contained a PageToken 
            // (see https://cloud.google.com/bigquery/docs/data#paging)
            if (_pageToken != null)
            {
                var furtherRequest = _context.BigQueryService.Jobs.GetQueryResults(_jobReference.ProjectId, _jobReference.JobId);
                furtherRequest.PageToken = _pageToken;

                var sw = Stopwatch.StartNew();
                var furtherQueryResponse = furtherRequest.Execute();
                sw.Stop();

                return new QueryResponsePage<T>(
                    _context,
                    _rowsParser, furtherQueryResponse.JobReference, furtherQueryResponse.TotalRows, furtherQueryResponse.JobComplete, furtherQueryResponse.PageToken, furtherQueryResponse.Schema, furtherQueryResponse.Rows, sw.Elapsed, _isDynamic);
            }
            return null;
        }

        public async Task<QueryResponsePage<T>> NextPageAsync(CancellationToken token = default(CancellationToken))
        {
            if (!_jobComplete.GetValueOrDefault())
            {
                throw new InvalidOperationException("Can't get next page for a job that has not completed");
            }

            // there's only another page if the previous response contained a PageToken 
            // (see https://cloud.google.com/bigquery/docs/data#paging)
            if (_pageToken != null)
            {
                var furtherRequest = _context.BigQueryService.Jobs.GetQueryResults(_jobReference.ProjectId, _jobReference.JobId);
                furtherRequest.PageToken = _pageToken;

                var sw = Stopwatch.StartNew();
                var furtherQueryResponse = await furtherRequest.ExecuteAsync(token);
                sw.Stop();

                return new QueryResponsePage<T>(
                    _context,
                    _rowsParser, furtherQueryResponse.JobReference, furtherQueryResponse.TotalRows, furtherQueryResponse.JobComplete, furtherQueryResponse.PageToken, furtherQueryResponse.Schema, furtherQueryResponse.Rows, sw.Elapsed, _isDynamic);
            }
            return null;
        }
    }
}