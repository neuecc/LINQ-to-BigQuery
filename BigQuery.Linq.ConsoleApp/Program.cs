using Google.Apis.Auth.OAuth2;
using Google.Apis.Auth.OAuth2.Flows;
using Google.Apis.Bigquery.v2;
using Google.Apis.Bigquery.v2.Data;
using Google.Apis.Services;
using Google.Apis.Util.Store;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BigQuery.Linq.ConsoleApp
{
    class Program
    {

        static void Main(string[] args)
        {


            var json = @"{""installed"":{""auth_uri"":""https://accounts.google.com/o/oauth2/auth"",""client_secret"":""GvCjNq5VG2jIjjlcuNuKwkWk"",""token_uri"":""https://accounts.google.com/o/oauth2/token"",""client_email"":"""",""redirect_uris"":[""urn:ietf:wg:oauth:2.0:oob"",""oob""],""client_x509_cert_url"":"""",""client_id"":""948208029220-rgjcvmo3ecfdml9c90j8dtltq89h9tg7.apps.googleusercontent.com"",""auth_provider_x509_cert_url"":""https://www.googleapis.com/oauth2/v1/certs""}}";

            var ms = new MemoryStream(Encoding.UTF8.GetBytes(json));


            var hugahuga = GoogleWebAuthorizationBroker.AuthorizeAsync(ms,
                new[] { BigqueryService.Scope.Bigquery },
                "user",
                CancellationToken.None, new FileDataStore(@"LINQtoBigQuery"))
                .Result;



            //new ServiceAccountCredential

            var bigquery = new BigqueryService(new BaseClientService.Initializer
            {
                ApplicationName = "LINQ to BigQuery",
                HttpClientInitializer = hugahuga
            });



            var query = "SELECT title, id, is_redirect FROM [publicdata:samples.wikipedia] LIMIT 1000";
            var queryRequest = new QueryRequest { Query = query };


            var result = bigquery.Jobs.Query(queryRequest, "exalted-country-672").ExecuteAsync().Result;


            var ctx = new BigQueryContext(bigquery, "exalted-country-672").Query<int>(@"SELECT 100");





            //var flow = new GoogleAuthorizationCodeFlow(new GoogleAuthorizationCodeFlow.Initializer
            //{
            //    ClientSecretsStream = ms,
            //    Scopes = new[] { BigqueryService.Scope.Bigquery },
            //    DataStore = new FileDataStore(@"C:\hoge")
            //});
            // Console.WriteLine(flow.);

            // new UserCredential(flow, "ils@neue.cc", 

            //var hoge = flow.LoadTokenAsync("ils@neue.cc", CancellationToken.None).Result;





            // schema?


        }
    }

    public class Wikipe
    {
        public string title { get; set; }
        public int id { get; set; }
        public bool? is_redirect { get; set; }
    }
}
