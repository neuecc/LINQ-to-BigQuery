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
        private static BigQueryContext GetContext(string json, string user, string projectId)
        {
            BigQueryContext context;

            using (var ms = new MemoryStream(Encoding.UTF8.GetBytes(json)))
            {
                // Open Browser, Accept Auth
                var userCredential = GoogleWebAuthorizationBroker.AuthorizeAsync(ms,
                    new[] { BigqueryService.Scope.Bigquery },
                    user,
                    CancellationToken.None, new FileDataStore(@"LINQ-to-BigQuery-for-" + projectId)) // localcache
                    .Result;

                var bigquery = new BigqueryService(new BaseClientService.Initializer
                {
                    ApplicationName = "LINQ to BigQuery",
                    HttpClientInitializer = userCredential
                });

                context = new BigQueryContext(bigquery, projectId);
            }
            // Timeout or other options
            context.TimeoutMs = (long)TimeSpan.FromMinutes(1).TotalMilliseconds;
            return context;
        }
    }
}