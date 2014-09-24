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
