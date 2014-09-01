using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Tests.Functions
{
    [TestClass]
    public class UrlTest
    {
        [TestMethod]
        public void HostDomainTld()
        {
            var context = new BigQueryContext();

            context.Select<string>(() => BqFunc.Host("http://www.google.com/")).ToSql()
                .Is("SELECT HOST('http://www.google.com/')");

            context.Select<string>(() => BqFunc.Domain("http://www.google.com/")).ToSql()
                .Is("SELECT DOMAIN('http://www.google.com/')");

            context.Select<string>(() => BqFunc.Tld("http://www.google.com/")).ToSql()
                .Is("SELECT TLD('http://www.google.com/')");
        }
    }
}
