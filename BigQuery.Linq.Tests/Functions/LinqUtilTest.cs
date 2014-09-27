using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Tests.Functions
{
    [TestClass]
    public class LinqUtilTest
    {
        [TestMethod]
        public void AsConstant()
        {
            var a = 100;
            var b = "aaa";
            var c = new DateTime(2010, 1, 1, 5, 3, 4, DateTimeKind.Utc);
            var d = true;


            new BigQueryContext().Select(() => new
            {
                a = BqFunc.AsConstant(a),
                b = BqFunc.AsConstant(b),
                c = BqFunc.AsConstant(c.AddDays(10)),
                d = BqFunc.AsConstant(d),
            })
            .ToString()
            .Is(@"
SELECT
  100 AS [a],
  'aaa' AS [b],
  '2010-01-11 05:03:04.000000' AS [c],
  true AS [d]
".TrimSmart());
        }
    }

}