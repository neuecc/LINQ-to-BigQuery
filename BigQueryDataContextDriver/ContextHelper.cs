using System;
using System.IO;
using System.Text;
using System.Threading;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2;
using Google.Apis.Services;
using Google.Apis.Util.Store;

namespace BigQuery.Linq
{
    public static class ContextHelper
    {
        public static BigQueryContext GetContext(string json, string user, string projectId)
        {
            BigQueryContext context;

            using (var ms = new MemoryStream(Encoding.UTF8.GetBytes(json)))
            {
                // Open Browser, Accept Auth
                var userCredential = GoogleWebAuthorizationBroker.AuthorizeAsync(ms,
                    new[] { BigqueryService.Scope.Bigquery },
                    user,
                    CancellationToken.None, new FileDataStore(@"LINQ-to-BigQuery-for-" + projectId)) // localcache
                    .GetAwaiter().GetResult();

                var bigquery = new BigqueryService(new BaseClientService.Initializer
                {
                    ApplicationName = "LINQ to BigQuery",
                    HttpClientInitializer = userCredential
                });

                var rowsParser = new DeserializerRowsParser();

                context = new BigQueryContext(rowsParser, bigquery, projectId);
            }
            // Timeout or other options
            context.TimeoutMs = (long)TimeSpan.FromMinutes(3).TotalMilliseconds;
            return context;
        }
    }
}