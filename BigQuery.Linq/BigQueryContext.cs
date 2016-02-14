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
using System.Text.RegularExpressions;
using System.IO;
using Google.Apis.Util.Store;

namespace BigQuery.Linq
{
    public class BigQueryContext
    {
        public int IndentSize { get; set; }
        public bool IsConvertResultUtcToLocalTime { get; set; }
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

        internal readonly Dictionary<Type, CustomDeserializeFallback> fallbacks = new Dictionary<Type, CustomDeserializeFallback>();
        private readonly IRowsParser _rowsParser;

        public BigQueryContext() : this(new DeserializerRowsParser())
        {
        }

        public BigQueryContext(IRowsParser rowsParser)
        {
            this.IndentSize = 2;
            this.IsConvertResultUtcToLocalTime = true;
            this._rowsParser = rowsParser;
        }

        public BigQueryContext(IRowsParser rowsParser, BigqueryService service, string projectId)
            : this(rowsParser)
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

            return new FromBigQueryable<T>(new[] { attr.TableName }, new RootBigQueryable<T>(this));
        }

        public IFromBigQueryable<T> From<T>(params string[] tableNames)
        {
            return new FromBigQueryable<T>(tableNames, new RootBigQueryable<T>(this));
        }

        public IFromBigQueryable<T> From<T>(string tableName, T dynamicSchema)
        {
            return new FromBigQueryable<T>(new[] { tableName }, new RootBigQueryable<T>(this));
        }

        public IFromBigQueryable<T> From<T>(string[] tableNames, T dynamicSchema)
        {
            return new FromBigQueryable<T>(tableNames, new RootBigQueryable<T>(this));
        }

        public ISubqueryBigQueryable<T> From<T>(IEnumerable<IExecutableBigQueryable<T>> nestedSources)
        {
            return new SubqueryBigQueryable<T>(nestedSources.ToArray());
        }

        public ISubqueryBigQueryable<T> From<T>(IExecutableBigQueryable<T> nestedSource, params IExecutableBigQueryable<T>[] unionSources)
        {
            return new SubqueryBigQueryable<T>(new[] { nestedSource }.Concat(unionSources).ToArray());
        }

        // Table wildcard functions

        /// <summary>
        /// Queries todays table
        /// </summary>
        public IFromTableWildcardBigQueryable<T> FromDateRange<T>()
        {
            var attr = typeof(T).GetCustomAttribute<TablePrefixAttribute>();
            if (attr == null) throw new ArgumentException("T should use TablePrefixAttribute");

            return FromDateRange<T>(attr.TablePrefix, DateTimeOffset.Now, DateTimeOffset.Now);
        }

        /// <summary>
        /// Queries timestamp date table
        /// </summary>
        public IFromTableWildcardBigQueryable<T> FromDateRange<T>(DateTimeOffset timestamp)
        {
            var attr = typeof(T).GetCustomAttribute<TablePrefixAttribute>();
            if (attr == null) throw new ArgumentException("T should use TablePrefixAttribute");

            return FromDateRange<T>(attr.TablePrefix, timestamp, timestamp);
        }

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
        public IFromTableWildcardBigQueryable<T> FromDateRange<T>(Expression<Func<DateTimeOffset>> timestampFrom, Expression<Func<DateTimeOffset>> timestampTo)
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
        public IFromTableWildcardBigQueryable<T> FromDateRange<T>(string prefix, Expression<Func<DateTimeOffset>> timestampFrom, Expression<Func<DateTimeOffset>> timestampTo)
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
        /// Queries daily tables that overlap with the time range between timestamp1 and timestamp2.
        /// </summary>
        public IFromTableWildcardBigQueryable<T> FromDateRange<T>(string prefix, T dynamicSchema, Expression<Func<DateTimeOffset>> timestampFrom, Expression<Func<DateTimeOffset>> timestampTo)
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
        public IFromTableWildcardBigQueryable<T> FromDateRangeStrict<T>(Expression<Func<DateTimeOffset>> timestampFrom, Expression<Func<DateTimeOffset>> timestampTo)
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
        public IFromTableWildcardBigQueryable<T> FromDateRangeStrict<T>(string prefix, Expression<Func<DateTimeOffset>> timestampFrom, Expression<Func<DateTimeOffset>> timestampTo)
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
        /// This function is equivalent to TABLE_DATE_RANGE. The only difference is that if any daily table is missing in the sequence, TABLE_DATE_RANGE_STRICT fails and returns a Not Found: Table [table_name] error.
        /// </summary>
        public IFromTableWildcardBigQueryable<T> FromDateRangeStrict<T>(string prefix, T dynamicSchema, Expression<Func<DateTimeOffset>> timestampFrom, Expression<Func<DateTimeOffset>> timestampTo)
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

        public async Task<string[]> GetAllDatasetsAsync()
        {
            // TODO:needs to paging?
            var datasets = await BigQueryService.Datasets.List(ProjectId).ExecuteAsync().ConfigureAwait(false);
            return datasets.Datasets.Select(x => x.Id.Split(':')[1]).ToArray();
        }

        public MetaTable[] GetAllTableInfo(string dataset)
        {
            var query = "SELECT * from " + (dataset.UnescapeBq() + ".__TABLES__").EscapeBq();
            return Run<MetaTable>(query).ToArray();
        }

        public string[] BuildCSharpClass(string dataset)
        {
            return GetFastTableSchemasAsync(dataset)
                .Result
                .Select(x => x.MetaTableSchemas.First().BuildCSharpClasses(outTablePrefixClassIfMatched: true))
                .SelectMany(xs => xs, (x, y) => y.Code)
                .Distinct()
                .ToArray();
        }

        /// <summary>
        /// GetTableSchemas but if can grouping by day suffix, use same schema(for reduce query).
        /// </summary>
        public Task<GroupedMetaTableSchema[]> GetFastTableSchemasAsync(string dataset)
        {
            var tables = GetAllTableInfo(dataset)
                .Select(x =>
                {
                    var match = Regex.Match(x.ToFullTableName(), @"(.*)(\d{8})]$");
                    if (match.Success && x.type != 2)
                    {
                        return new
                        {
                            info = x,
                            prefix = match.Groups[1].Value + "]",
                            day = int.Parse(match.Groups[2].Value)
                        };
                    }
                    else
                    {
                        return new { info = x, prefix = default(string), day = default(int) };
                    }
                })
                .GroupBy(x => x.prefix ?? x.info.ToFullTableName())
                .Select(async xs =>
                {
                    var orderedTable = xs.OrderByDescending(x => x.day).ToArray();
                    var targetTable = orderedTable.First();

                    var schema = await targetTable.info.GetTableSchemaAsync(BigQueryService).ConfigureAwait(false);

                    if (orderedTable.Length == 1)
                    {
                        return new GroupedMetaTableSchema
                        {
                            MetaTableSchemas = new[] { schema },
                            IsGrouped = false,
                        };
                    }
                    else
                    {
                        return new GroupedMetaTableSchema
                        {
                            MetaTableSchemas = orderedTable.Select(x => new MetaTableSchema(x.info, schema.Fields)).ToArray(),
                            IsGrouped = true,
                            TablePrefix = targetTable.prefix,
                            ShortTablePrefix = Regex.Match(targetTable.info.table_id, @"(.*)(\d{8})$").Groups[1].Value,
                        };
                    }
                });

            return Task.WhenAll(tables);
        }

        public void RegisterCustomDeserializeFallback(Type targetType, CustomDeserializeFallback fallback)
        {
            fallbacks[targetType] = fallback;
        }

        // Execute

        JobsResource.QueryRequest BuildRequest(string query, bool isForceDry)
        {
            if (BigQueryService == null)
            {
                throw new InvalidOperationException("BigQueryService is null. Please set BigQueryService and ProjectId on constructor.");
            }

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
            var sw = Stopwatch.StartNew();
            var queryResponse = BuildRequest(query, isForceDry: false).Execute();
            if (queryResponse.JobComplete == false)
            {
                sw.Stop();
                throw new TimeoutException("Job is uncompleted maybe timeout, you can change QueryContext.TimeoutMs. ExecutionTime:" + sw.Elapsed);
            }

            var response = new QueryResponse<T>(this, query, sw.Elapsed, queryResponse, false, _rowsParser);
            return response;
        }

        public async Task<QueryResponse<T>> RunAsync<T>(string query, CancellationToken cancellationToken = default(CancellationToken))
        {
            var sw = Stopwatch.StartNew();
            var queryResponse = await BuildRequest(query, isForceDry: false).ExecuteAsync(cancellationToken).ConfigureAwait(false);
            sw.Stop();
            if (queryResponse.JobComplete == false)
            {
                throw new TimeoutException("Job is uncompleted maybe timeout, you can change QueryContext.TimeoutMs. ExecutionTime:" + sw.Elapsed);
            }

            var response = new QueryResponse<T>(this, query, sw.Elapsed, queryResponse, false, _rowsParser);
            return response;
        }

        public QueryResponse<T> RunDry<T>(string query)
        {
            var sw = Stopwatch.StartNew();
            var queryResponse = BuildRequest(query, isForceDry: true).Execute();
            sw.Stop();
            if (queryResponse.JobComplete == false)
            {
                throw new TimeoutException("Job is uncompleted maybe timeout, you can change QueryContext.TimeoutMs. ExecutionTime:" + sw.Elapsed);
            }

            var response = new QueryResponse<T>(this, query, sw.Elapsed, queryResponse, false, _rowsParser);
            return response;
        }

        // dynamic

        /// <summary>
        /// Run query and return Response with dynamic(ExpandoObject/Primitive) rows.
        /// </summary>
        public QueryResponse<dynamic> Run(string query)
        {
            var sw = Stopwatch.StartNew();
            var queryResponse = BuildRequest(query, isForceDry: false).Execute();
            sw.Stop();
            if (queryResponse.JobComplete == false)
            {
                throw new TimeoutException("Job is uncompleted maybe timeout, you can change QueryContext.TimeoutMs. ExecutionTime:" + sw.Elapsed);
            }

            var response = new QueryResponse<dynamic>(this, query, sw.Elapsed, queryResponse, true, _rowsParser);
            return response;
        }

        /// <summary>
        /// Run query and return Response with dynamic(ExpandoObject/Primitive) rows.
        /// </summary>
        public async Task<QueryResponse<dynamic>> RunAsync(string query, CancellationToken cancellationToken = default(CancellationToken))
        {
            var sw = Stopwatch.StartNew();
            var queryResponse = await BuildRequest(query, isForceDry: false).ExecuteAsync(cancellationToken).ConfigureAwait(false);
            sw.Stop();
            if (queryResponse.JobComplete == false)
            {
                throw new TimeoutException("Job is uncompleted maybe timeout, you can change QueryContext.TimeoutMs. ExecutionTime:" + sw.Elapsed);
            }

            var response = new QueryResponse<dynamic>(this, query, sw.Elapsed, queryResponse, true, _rowsParser);
            return response;
        }

        /// <summary>
        /// Dry run and return Response with dynamic(ExpandoObject/Primitive) rows.
        /// </summary>
        public QueryResponse<dynamic> RunDry(string query)
        {
            var sw = Stopwatch.StartNew();
            var queryResponse = BuildRequest(query, isForceDry: true).Execute();
            sw.Stop();
            if (queryResponse.JobComplete == false)
            {
                throw new TimeoutException("Job is uncompleted maybe timeout, you can change QueryContext.TimeoutMs. ExecutionTime:" + sw.Elapsed);
            }

            var response = new QueryResponse<dynamic>(this, query, sw.Elapsed, queryResponse, true, _rowsParser);
            return response;
        }

        // query

        /// <summary>
        /// Run query and return rows.
        /// </summary>
        public T[] Query<T>(string query)
        {
            return Run<T>(query).ToArray();
        }

        /// <summary>
        /// Run query and return rows.
        /// </summary>
        public async Task<T[]> QueryAsync<T>(string query, CancellationToken cancellationToken = default(CancellationToken))
        {
            var response = await RunAsync<T>(query, cancellationToken).ConfigureAwait(false);
            return await response.ToArrayAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Run query and return dynamic(ExpandoObject/Primitive) rows.
        /// </summary>
        public dynamic[] Query(string query)
        {
            return Run(query).ToArray();
        }

        /// <summary>
        /// Run query and return dynamic(ExpandoObject/Primitive) rows.
        /// </summary>
        public async Task<dynamic[]> QueryAsync(string query, CancellationToken cancellationToken = default(CancellationToken))
        {
            var response = await RunAsync(query, cancellationToken).ConfigureAwait(false);
            return await response.ToArrayAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}