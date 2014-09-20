using System;
using System.Reflection;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using BigQuery.Linq.Query;
using Google.Apis.Bigquery.v2.Data;
using Google.Apis.Bigquery.v2;
using System.Security.Cryptography.X509Certificates;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Services;
using System.Threading;

namespace BigQuery.Linq
{
    public class BigQueryContext
    {
        public int IndentSize { get; set; }
        public BigqueryService BigQueryService { get; set; }
        public string ProjectId { get; set; }

        // QueryOptions
        public bool? DryRun { get; set; }
        public string ETag { get; set; }
        public string Kind { get; set; }
        public long? MaxResults { get; set; }
        public bool? PreserveNulls { get; set; }
        public long? TimeoutMs { get; set; }
        public bool? UseQueryCache { get; set; }

        public BigQueryContext()
        {
            this.IndentSize = 2;
        }

        public BigQueryContext(BigqueryService service, string projectId)
        {
            this.BigQueryService = service;
            this.ProjectId = projectId;
        }

        public ISelectAfterOrderByBigQueryable<T> Select<T>(Expression<Func<T>> selector)
        {
            var unusedParameter = Expression.Parameter(typeof(T), "_");
            var wrapped = Expression.Lambda<Func<T, T>>(selector.Body, unusedParameter);

            return new SelectBigQueryable<T, T>(new RootBigQueryable<T>(this), wrapped);
        }

        public IFromBigQueryable<T> From<T>()
        {
            var attr = typeof(T).GetCustomAttribute<TableNameAttribute>();
            if (attr == null) throw new ArgumentException("T should use TableNameAttribute");

            return new FromBigQueryable<T>(attr.TableName, new RootBigQueryable<T>(this));
        }

        public IFromBigQueryable<T> From<T>(string tableName)
        {
            return new FromBigQueryable<T>(tableName, new RootBigQueryable<T>(this));
        }

        public IFromBigQueryable<T> From<T>(string tableName, T dynamicSchema)
        {
            return new FromBigQueryable<T>(tableName, new RootBigQueryable<T>(this));
        }

        public ISubqueryBigQueryable<T> From<T>(IExecutableBigQueryable<T> nestedSource)
        {
            return new SubqueryBigQueryable<T>(nestedSource);
        }

        // Table wildcard functions

        /// <summary>
        /// Queries daily tables that overlap with the time range between timestamp1 and timestamp2.
        /// </summary>
        public IFromTableWildcardBigQueryable<T> FromDateRange<T>(DateTimeOffset timestampFrom, DateTimeOffset timestampTo)
        {
            var attr = typeof(T).GetCustomAttribute<TablePrefixAttribute>();
            if (attr == null) throw new ArgumentException("T should use TablePrefixAttribute");

            return FromDateRange<T>(attr.TablePrefix, timestampFrom, timestampTo);
        }

        /// <summary>
        /// Queries daily tables that overlap with the time range between timestamp1 and timestamp2.
        /// </summary>
        public IFromTableWildcardBigQueryable<T> FromDateRange<T>(string prefix, DateTimeOffset timestampFrom, DateTimeOffset timestampTo)
        {
            return new FromDateRangeBigQueryable<T>(prefix, timestampFrom, timestampTo, new RootBigQueryable<T>(this));
        }

        /// <summary>
        /// Queries daily tables that overlap with the time range between timestamp1 and timestamp2.
        /// </summary>
        public IFromTableWildcardBigQueryable<T> FromDateRange<T>(string prefix, T dynamicSchema, DateTimeOffset timestampFrom, DateTimeOffset timestampTo)
        {
            return new FromDateRangeBigQueryable<T>(prefix, timestampFrom, timestampTo, new RootBigQueryable<T>(this));
        }

        /// <summary>
        /// This function is equivalent to TABLE_DATE_RANGE. The only difference is that if any daily table is missing in the sequence, TABLE_DATE_RANGE_STRICT fails and returns a Not Found: Table [table_name] error.
        /// </summary>
        public IFromTableWildcardBigQueryable<T> FromDateRangeStrict<T>(DateTimeOffset timestampFrom, DateTimeOffset timestampTo)
        {
            var attr = typeof(T).GetCustomAttribute<TablePrefixAttribute>();
            if (attr == null) throw new ArgumentException("T should use TablePrefixAttribute");

            return FromDateRangeStrict<T>(attr.TablePrefix, timestampFrom, timestampTo);
        }

        /// <summary>
        /// This function is equivalent to TABLE_DATE_RANGE. The only difference is that if any daily table is missing in the sequence, TABLE_DATE_RANGE_STRICT fails and returns a Not Found: Table [table_name] error.
        /// </summary>
        public IFromTableWildcardBigQueryable<T> FromDateRangeStrict<T>(string prefix, DateTimeOffset timestampFrom, DateTimeOffset timestampTo)
        {
            return new FromDateRangeStrictBigQueryable<T>(prefix, timestampFrom, timestampTo, new RootBigQueryable<T>(this));
        }

        /// <summary>
        /// This function is equivalent to TABLE_DATE_RANGE. The only difference is that if any daily table is missing in the sequence, TABLE_DATE_RANGE_STRICT fails and returns a Not Found: Table [table_name] error.
        /// </summary>
        public IFromTableWildcardBigQueryable<T> FromDateRangeStrict<T>(string prefix, T dynamicSchema, DateTimeOffset timestampFrom, DateTimeOffset timestampTo)
        {
            return new FromDateRangeStrictBigQueryable<T>(prefix, timestampFrom, timestampTo, new RootBigQueryable<T>(this));
        }

        /// <summary>
        /// Queries tables whose names match the supplied expr. The expr parameter must be represented as a string and must contain an expression to evaluate.
        /// </summary>
        public IFromTableWildcardBigQueryable<T> FromTableQuery<T>(string dataset, Expression<Func<MetaTable, bool>> tableMatchCondition)
        {
            return new FromTableQueryBigQueryable<T>(dataset, tableMatchCondition, new RootBigQueryable<T>(this));
        }

        /// <summary>
        /// Queries tables whose names match the supplied expr. The expr parameter must be represented as a string and must contain an expression to evaluate.
        /// </summary>
        public IFromTableWildcardBigQueryable<T> FromTableQuery<T>(string dataset, T dynamicSchema, Expression<Func<MetaTable, bool>> tableMatchCondition)
        {
            return new FromTableQueryBigQueryable<T>(dataset, tableMatchCondition, new RootBigQueryable<T>(this));
        }

        // Util Methods

        public MetaTable[] GetAllTableInfo(string dataset)
        {
            var query = "SELECT * from " + (dataset.UnescapeBq() + ".__TABLES__").EscapeBq();
            return Query<MetaTable>(query);
        }

        // Execute

        JobsResource.QueryRequest BuildRequest(string query, bool isForceDry)
        {
            var body = new QueryRequest
            {
                Query = query,
                DryRun = (isForceDry) ? true : DryRun,
                ETag = ETag,
                Kind = Kind,
                MaxResults = MaxResults,
                PreserveNulls = PreserveNulls,
                TimeoutMs = TimeoutMs,
                UseQueryCache = UseQueryCache
            };
            var request = BigQueryService.Jobs.Query(body, ProjectId);
            request.PrettyPrint = false;
            return request;
        }

        public QueryResponse<T> Run<T>(string query)
        {
            var queryResponse = BuildRequest(query, isForceDry: false).Execute();
            var response = new QueryResponse<T>(query, queryResponse);
            return response;
        }

        public QueryResponse<T> RunDry<T>(string query)
        {
            var queryResponse = BuildRequest(query, isForceDry: true).Execute();
            var response = new QueryResponse<T>(query, queryResponse);
            return response;
        }

        public async Task<QueryResponse<T>> RunAsync<T>(string query, CancellationToken cancellationToken = default(CancellationToken))
        {
            var queryResponse = await BuildRequest(query, isForceDry: false).ExecuteAsync(cancellationToken).ConfigureAwait(false);
            var response = new QueryResponse<T>(query, queryResponse);
            return response;
        }

        public T[] Query<T>(string query)
        {
            return Run<T>(query).Rows;
        }

        public async Task<T[]> QueryAsync<T>(string query, CancellationToken cancellationToken = default(CancellationToken))
        {
            var response = await RunAsync<T>(query, cancellationToken).ConfigureAwait(false);
            return response.Rows;
        }
    }
}
