using Google.Apis.Auth.OAuth2;
using Google.Apis.Auth.OAuth2.Flows;
using Google.Apis.Bigquery.v2;
using Google.Apis.Bigquery.v2.Data;
using Google.Apis.Services;
using Google.Apis.Util;
using Google.Apis.Util.Store;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Text.RegularExpressions;

namespace BigQuery.Linq.ConsoleApp
{
    public static class Query
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


    class Program
    {


        static void Main(string[] args)
        {


          
            

        }
    }

    [TableName("publicdata:samples.trigrams")]
    public class trigrams
    {
        public cell cell { get; set; }
    }

    public class cell
    {
        public string value { get; set; }
    }
}