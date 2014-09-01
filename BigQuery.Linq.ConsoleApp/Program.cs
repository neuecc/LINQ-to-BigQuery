using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2;
using Google.Apis.Bigquery.v2.Data;
using Google.Apis.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.ConsoleApp
{
    public class Deserializer<T>
    {
        readonly TableSchema schema;
        readonly Dictionary<string, PropertyInfo> typeInfo;

        public Deserializer(TableSchema schema)
        {
            this.schema = schema;
            this.typeInfo = typeof(T).GetProperties().ToDictionary(x => x.Name);
        }

        // TODO:Parse
        object Parse(string type, string value)
        {
            if (value == null) return null;

            switch (type)
            {
                case "STRING":
                    return value;
                case "INTEGER":
                    return long.Parse(value);
                case "FLOAT":
                    return double.Parse(value);
                case "BOOLEAN":
                    return bool.Parse(value);
                case "TIMESTAMP":
                    // return DateTime.Parse(value); // NO!
                    return DateTimeOffset.MinValue;
                default:
                    throw new InvalidOperationException();
            }
        }

        public T Deserialize(TableRow row)
        {
            var result = Activator.CreateInstance<T>();
            for (int i = 0; i < row.F.Count; i++)
            {
                var field = schema.Fields[i];
                var value = row.F[i].V;
                typeInfo[field.Name].SetValue(result, Parse(field.Type, (string)value));
            }
            return result;
        }
    }

    class wikipedia
    {
        public long? id { get; set; }
        public string title { get; set; }
        public bool? is_redirect { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var query = "SELECT title, id, is_redirect FROM [publicdata:samples.wikipedia] LIMIT 1000";
            var queryRequest = new QueryRequest { Query = query };

            // TODO:Authenticate
            var projectId = "";
            var service = new BigqueryService();

            var certificate = new X509Certificate2(@"", "notasecret", X509KeyStorageFlags.Exportable);

            var credential = new ServiceAccountCredential(new ServiceAccountCredential.Initializer(@"")
            {
                Scopes = new[]
		{
			BigqueryService.Scope.Bigquery,
			BigqueryService.Scope.BigqueryInsertdata,
		}
            }.FromCertificate(certificate));

            var bigquery = new BigqueryService(new BaseClientService.Initializer
            {
                ApplicationName = "TESTES",
                HttpClientInitializer = credential
            });

            var queryResponse = bigquery.Jobs.Query(queryRequest, projectId).Execute();

            // schema?

            var schema = queryResponse.Schema;
            var deserializer = new Deserializer<wikipedia>(schema);
            var hogehoge = queryResponse.Rows.Select(row => deserializer.Deserialize(row)).ToArray();

        }
    }
}
