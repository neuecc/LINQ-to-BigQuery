using BigQuery.Linq;
using CoreTweet;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2;
using Google.Apis.Services;
using System;
using System.Configuration;
using System.IO;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;

namespace StoreTwitterTimeline
{
    public static class Account
    {
        // BigQuery

        public static BigqueryService GetService()
        {
            var certificateFileName = ConfigurationManager.AppSettings["certificateFileName"];
            var certificatePassword = ConfigurationManager.AppSettings["certificatePassword"];
            var serviceAccountEmail = ConfigurationManager.AppSettings["serviceAccountEmail"];
            var applicationName = ConfigurationManager.AppSettings["applicationName"];

            var path = Path.Combine(new FileInfo(Assembly.GetExecutingAssembly().Location).Directory.FullName, certificateFileName);

            var credential = new ServiceAccountCredential(new ServiceAccountCredential.Initializer(serviceAccountEmail)
            {
                Scopes = new[] { BigqueryService.Scope.Bigquery, BigqueryService.Scope.BigqueryInsertdata },
            }.FromCertificate(new X509Certificate2(path, certificatePassword, X509KeyStorageFlags.Exportable)));

            var bigquery = new BigqueryService(new BaseClientService.Initializer
            {
                ApplicationName = applicationName,
                HttpClientInitializer = credential
            });

            return bigquery;
        }

        public static BigQueryContext GetContext()
        {
            var projectId = ConfigurationManager.AppSettings["projectId"];

            var bigquery = GetService();
            var context = new BigQueryContext(bigquery, projectId);
            context.TimeoutMs = (long)TimeSpan.FromMinutes(5).TotalMilliseconds;
            return context;
        }

        public static string GetTableSchema(string datasetId, string tableId)
        {
            var context = Account.GetContext();
            var meta = new MetaTable(context.ProjectId, datasetId, tableId);
            return meta.GetTableSchema(context.BigQueryService).BuildCSharpClass();
        }

        // Twitter
        public static Tokens GetTokens()
        {
            var consumerKey = ConfigurationManager.AppSettings["consumerKey"];
            var consumerSecret = ConfigurationManager.AppSettings["consumerSecret"];
            var accessToken = ConfigurationManager.AppSettings["accessToken"];
            var accessSecret = ConfigurationManager.AppSettings["accessSecret"];

            return Tokens.Create(consumerKey, consumerSecret, accessToken, accessSecret);
        }
    }
}