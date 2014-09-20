using BigQuery.Linq.Tests.Builder;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2;
using Google.Apis.Services;
using Google.Apis.Util.Store;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BigQuery.Linq.Tests
{
    [Ignore]
    [TestClass]
    public class QueryTest
    {
        BigQueryContext context;

        public QueryTest()
        {
            // auth json!
            var json = "";
            var projectId = "";

            using (var ms = new MemoryStream(Encoding.UTF8.GetBytes(json)))
            {
                var authToken = GoogleWebAuthorizationBroker.AuthorizeAsync(ms,
                    new[] { BigqueryService.Scope.Bigquery },
                    "user",
                    CancellationToken.None, new FileDataStore(@"LINQtoBigQuery"))
                    .Result;

                var service = new BigqueryService(new BaseClientService.Initializer
                {
                    ApplicationName = "LINQ to BigQuery",
                    HttpClientInitializer = authToken
                });

                context = new BigQueryContext(service, projectId);
            }
        }

        [TestMethod]
        public void ConnectTest()
        {
            var result = context.From<Wikipedia>()
                .Select(x => new { x.title, x.wp_namespace })
                .Limit(1000)
                .RunDry();
        }

        [TestMethod]
        public void ConnectTest2()
        {
            var meta = context.GetAllTableInfo("publicdata:samples");
        }

        [TestMethod]
        public void NANMapping()
        {
            // SELECT ATANH(10)
        }
    }
}
