using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Tests.Functions
{
    [TableName("mydata.nest")]
    class Person
    {
        public string kind { get; set; }
        public string fullName { get; set; }
        public long? age { get; set; }
        public string gender { get; set; }
        public phoneNumber phoneNumber { get; set; }
        public children[] children { get; set; }
        public citiesLived[] citiesLived { get; set; }
    }

    class phoneNumber
    {
        public long? areaCode { get; set; }
        public long? number { get; set; }
    }

    class children
    {
        public string name { get; set; }
        public string gender { get; set; }
        public long? age { get; set; }
    }

    class citiesLived
    {
        public string place { get; set; }
        public long[] yearsLived { get; set; }
    }

    [Microsoft.VisualStudio.TestTools.UnitTesting.TestClass]
    public class WithInTest
    {
        [TestMethod]
        public void WithIn()
        {
            var r = new BigQueryContext()
              .From<Person>()
              .Select(x => new
              {
                  x.fullName,
                  numberOfPlacesLived = BqFunc.WithIn(BqFunc.Count(x.citiesLived[0].place)),
                  citiesLivedPlace = x.citiesLived[0].place,
                  numberOfTimesInEachCity = BqFunc.WithIn(BqFunc.Count(x.citiesLived[0].yearsLived), x.citiesLived)
              })
              .ToString();

            r.Is(@"
SELECT
  [fullName],
  COUNT([citiesLived.place]) WITHIN RECORD AS [numberOfPlacesLived],
  [citiesLived.place] AS [citiesLivedPlace],
  COUNT([citiesLived.yearsLived]) WITHIN [citiesLived] AS [numberOfTimesInEachCity]
FROM
  [mydata.nest]
".TrimSmart());
        }
    }
}
