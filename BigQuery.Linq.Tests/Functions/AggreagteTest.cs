using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Tests.Functions
{
    [TestClass]
    public class AggreagteTest
    {
        class MyClass
        {
            public int field1 { get; set; }
            public string field2 { get; set; }
        }

        [TestMethod]
        public void Aggregate()
        {
            var context = new BigQueryContext();

            context.Select<int>(() => BqFunc.Average(100)).ToString().TrimFlat()
                .Is("SELECT AVG(100)");

            context.Select<int>(() => BqFunc.Correlation(100, 100)).ToString().TrimFlat()
                .Is("SELECT CORR(100, 100)");

            context.Select<int>(() => BqFunc.Count()).ToString().TrimFlat()
                .Is("SELECT COUNT(*)");

            var field1 = 100;
            context.Select<int>(() => BqFunc.Count(field1)).ToString().TrimFlat()
                .Is("SELECT COUNT([field1])");
        }
    }
}
