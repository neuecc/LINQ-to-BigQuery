using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Tests.Functions
{
    [TestClass]
    public class OtherTest
    {
        [TableName("MyClass")]
        class MyClass
        {
            public int field1 { get; set; }
            public string field2 { get; set; }
        }

        public BigQueryContext Ctx { get { return new BigQueryContext(); } }

        [TestMethod]
        public void Hash()
        {
            Ctx.From<MyClass>()
                .Select(x => new { F1 = x.field1, Hash = BqFunc.Hash(x.field1) })
                .IgnoreCase()
                .ToString()
                .Is(@"
SELECT
  [field1] AS [F1],
  HASH([field1]) AS [Hash]
FROM
  [MyClass]
IGNORE CASE
".TrimSmart());
        }

        [TestMethod]
        public void Position()
        {
            new BigQueryContext()
                .From("mydata.nest", new
                {
                    fullName = "",
                    children = new[]
                    {
                        new { name = "", age = 0L},
                    }
                })
                .Select(x => new { x.fullName, x.children[0].name, position = BqFunc.Position(x.children[0].age)})
                .ToString()
                .Is(@"
SELECT
  [fullName],
  [children.name] AS [name],
  POSITION([children.age]) AS [position]
FROM
  [mydata.nest]
".TrimSmart());
        }

        [TestMethod]
        public void If()
        {
            Ctx.From<MyClass>()
                .Select(x => new { F1 = (x.field1 >= 100) ? "A" : "B" })
                .ToString()
                .Is(@"
SELECT
  IF(([field1] >= 100), 'A', 'B') AS [F1]
FROM
  [MyClass]".TrimSmart());
        }

        [TestMethod]
        public void Case()
        {
            Ctx.From<MyClass>()
                .Select(x => new
                {
                    F1 = (x.field1 == 100) ? "A" :
                         (x.field1 >= 200) ? "B" :
                         (x.field1 == 1000 && x.field2 == "hoge") ? "C"
                         : "D",
                    Len = BqFunc.Length(x.field2)
                })
                .ToString()
                .Is(@"
SELECT
  CASE
    WHEN ([field1] = 100) THEN 'A'
    WHEN ([field1] >= 200) THEN 'B'
    WHEN (([field1] = 1000) AND ([field2] = 'hoge')) THEN 'C'
    ELSE 'D'
  END AS [F1],
  LENGTH([field2]) AS [Len]
FROM
  [MyClass]".TrimSmart());
        }
    }
}
