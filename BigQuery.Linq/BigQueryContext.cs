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

namespace BigQuery.Linq
{
    public class BigQueryContext
    {
        public int IndentSize { get; set; }
        public BigqueryService BigQueryService { get; set; }
        public string ProjectId { get; set; }

        public BigQueryContext()
        {
            this.IndentSize = 2;
        }

        public BigQueryContext(BigqueryService service, string projectId)
        {
            this.BigQueryService = service;
            this.ProjectId = projectId;
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

        public IFromBigQueryable<T> FromDateRange<T>(DateTimeOffset timestampFrom, DateTimeOffset timestampTo)
        {
            var attr = typeof(T).GetCustomAttribute<TablePrefixAttribute>();
            if (attr == null) throw new ArgumentException("T should use TablePrefixAttribute");

            return FromDateRange<T>(attr.TablePrefix, timestampFrom, timestampTo);
        }

        public IFromBigQueryable<T> FromDateRange<T>(string prefix, DateTimeOffset timestampFrom, DateTimeOffset timestampTo)
        {
            throw new NotImplementedException();
        }

        public IFromBigQueryable<T> FromDateRangeStrict<T>(DateTimeOffset timestampFrom, DateTimeOffset timestampTo)
        {
            var attr = typeof(T).GetCustomAttribute<TablePrefixAttribute>();
            if (attr == null) throw new ArgumentException("T should use TablePrefixAttribute");

            throw new NotImplementedException();
        }

        public IFromBigQueryable<T> FromDateRangeStrict<T>(string prefix, DateTimeOffset timestampFrom, DateTimeOffset timestampTo)
        {
            throw new NotImplementedException();
        }

        public IFromBigQueryable<T> FromTableQuery<T>(string dataset, Expression<Func<MetaTable, bool>> tableMatchCondition)
        {
            FromTableQuery<int>("aaa", x => x.table_id.Contains("hogehoge"));

            throw new NotImplementedException();
        }

        public IExecutableBigQueryable<T> Select<T>(Expression<Func<T>> selector)
        {
            var unusedParameter = Expression.Parameter(typeof(T), "_");
            var wrapped = Expression.Lambda<Func<T, T>>(selector.Body, unusedParameter);

            return new SelectBigQueryable<T, T>(new RootBigQueryable<T>(this), wrapped);
        }

        public MetaTable[] GetAllTableInfo(string dataset)
        {
            var query = "SELECT " + (dataset.UnescapeBq() + ".__TABLES__").EscapeBq();
            return Query<MetaTable>(query).ToArray();
        }

        // Run

        public IEnumerable<T> Query<T>(string query)
        {
            // TODO:Option
            var body = new QueryRequest { Query = query };
            var request = BigQueryService.Jobs.Query(body, ProjectId);

            var queryResponse = request.Execute();

            queryResponse.Rows.Select(row => row.F.Select(cell =>
            {
                // TODO:Deserialize
                return cell;
            }));

            throw new NotImplementedException();
        }
    }
}
