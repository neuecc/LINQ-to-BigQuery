using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Google.Apis.Bigquery.v2.Data;
using System.Collections.Generic;
using System.Reflection;
using Newtonsoft.Json;

namespace BigQuery.Linq.Tests
{
    [TestClass]
    public class DataTypeTest
    {
        class MyClass
        {
            public int Hoge { get; set; }
            public DateTimeOffset? Nano { get; set; }
            public Dictionary<string, object> MyProperty { get; set; }
            public MyClass2 Next { get; set; }
            public int[] Xs { get; set; }
        }

        class MyClass2
        {
            public string BBB { get; set; }
            public double? ZZZ { get; set; }
        }

        enum MyEnum
        {
            Apple = 1,
            Orange = 2
        }

        [TestMethod]
        public void Parse()
        {
            DataTypeUtility.Parse("TIMESTAMP").Is(DataType.Timestamp);
            DataTypeUtility.Parse("BOOLEAN").Is(DataType.Boolean);
            DataTypeUtility.Parse("FLOAT").Is(DataType.Float);
            DataTypeUtility.Parse("INTEGER").Is(DataType.Integer);
            DataTypeUtility.Parse("RECORD").Is(DataType.Record);
            DataTypeUtility.Parse("STRING").Is(DataType.String);
        }

        [TestMethod]
        public void ToDataType()
        {
            DataTypeUtility.ToDataType(typeof(int)).Is(DataType.Integer);
            DataTypeUtility.ToDataType(typeof(int?)).Is(DataType.Integer);
            DataTypeUtility.ToDataType(typeof(double)).Is(DataType.Float);
            DataTypeUtility.ToDataType(typeof(double?)).Is(DataType.Float);
            DataTypeUtility.ToDataType(typeof(decimal)).Is(DataType.Float);
            DataTypeUtility.ToDataType(typeof(decimal?)).Is(DataType.Float);
            DataTypeUtility.ToDataType<bool>().Is(DataType.Boolean);
            DataTypeUtility.ToDataType<bool?>().Is(DataType.Boolean);
            DataTypeUtility.ToDataType<DateTime>().Is(DataType.Timestamp);
            DataTypeUtility.ToDataType<DateTime?>().Is(DataType.Timestamp);
            DataTypeUtility.ToDataType<DateTimeOffset>().Is(DataType.Timestamp);
            DataTypeUtility.ToDataType<DateTimeOffset?>().Is(DataType.Timestamp);
            DataTypeUtility.ToDataType<MyEnum>().Is(DataType.Integer);
            DataTypeUtility.ToDataType<MyEnum?>().Is(DataType.Integer);
            DataTypeUtility.ToDataType<wikipedia>().Is(DataType.Record);
        }

        [TestMethod]
        public void ToTableFieldSchema()
        {
            DataTypeUtility.ToTableFieldSchema<wikipedia>().IsStructuralEqual(new[] 
            {
                new TableFieldSchema(){ Name = "title", Type = "STRING", Mode = "NULLABLE"},
                new TableFieldSchema(){ Name = "id", Type = "INTEGER", Mode = "NULLABLE"},
                new TableFieldSchema(){ Name = "language", Type = "STRING", Mode = "NULLABLE"},
                new TableFieldSchema(){ Name = "wp_namespace", Type = "INTEGER", Mode = "REQUIRED"},
                new TableFieldSchema(){ Name = "is_redirect", Type = "BOOLEAN", Mode = "NULLABLE"},
                new TableFieldSchema(){ Name = "revision_id", Type = "INTEGER", Mode = "NULLABLE"},
                new TableFieldSchema(){ Name = "contributor_ip", Type = "STRING", Mode = "NULLABLE"},
                new TableFieldSchema(){ Name = "contributor_id", Type = "INTEGER", Mode = "NULLABLE"},
                new TableFieldSchema(){ Name = "contributor_username", Type = "STRING", Mode = "NULLABLE"},
                new TableFieldSchema(){ Name = "timestamp", Type = "INTEGER", Mode = "REQUIRED"},
                new TableFieldSchema(){ Name = "is_minor", Type = "BOOLEAN", Mode = "NULLABLE"},
                new TableFieldSchema(){ Name = "is_bot", Type = "BOOLEAN", Mode = "NULLABLE"},
                new TableFieldSchema(){ Name = "reversion_id", Type = "INTEGER", Mode = "NULLABLE"},
                new TableFieldSchema(){ Name = "comment", Type = "STRING", Mode = "NULLABLE"},
                new TableFieldSchema(){ Name = "num_characters", Type = "INTEGER", Mode = "REQUIRED"},
            });


            DataTypeUtility.ToTableFieldSchema(new
            {
                Hoge = default(DateTime),
                Huga = default(DateTimeOffset?),
                Tako = default(bool),
                Nano = 1.0,
                K = default(int?),
                C = MyEnum.Apple
            }).IsStructuralEqual(new[] 
            {
                new TableFieldSchema(){ Name = "Hoge", Type = "TIMESTAMP", Mode = "REQUIRED"},
                new TableFieldSchema(){ Name = "Huga", Type = "TIMESTAMP", Mode = "NULLABLE"},
                new TableFieldSchema(){ Name = "Tako", Type = "BOOLEAN", Mode = "REQUIRED"},
                new TableFieldSchema(){ Name = "Nano", Type = "FLOAT", Mode = "REQUIRED"},
                new TableFieldSchema(){ Name = "K", Type = "INTEGER", Mode = "NULLABLE"},
                new TableFieldSchema(){ Name = "C", Type = "INTEGER", Mode = "REQUIRED"},
            });
        }

        [TestMethod]
        public void CustomSerialization()
        {
            var schema = DataTypeUtility.ToTableFieldSchema(typeof(MyClass), pi =>
            {
                if (pi == typeof(MyClass).GetProperty("MyProperty"))
                {
                    return new TableFieldSchema { Name = "MyProperty", Type = DataType.String.ToIdentifier() };
                }
                return null;
            });

            schema[0].Is(x => x.Name == "Hoge" && x.Type == "INTEGER");
            schema[1].Is(x => x.Name == "Nano" && x.Type == "TIMESTAMP");
            schema[2].Is(x => x.Name == "MyProperty" && x.Type == "STRING");
            schema[3].Is(x => x.Name == "Next" && x.Type == "RECORD");
            schema[4].Is(x => x.Name == "Xs" && x.Type == "INTEGER" && x.Mode == "REPEATED");

            // deserialize

            var value = new Deserializer<MyClass>(new TableSchema { Fields = schema }, (PropertyInfo pi, object v, out object r) =>
            {
                if (pi == typeof(MyClass).GetProperty("MyProperty"))
                {
                    r = JsonConvert.DeserializeObject<Dictionary<string, object>>((string)v);
                    return true;
                }
                r = null;
                return false;
            })
            .Deserialize(new TableRow()
            {
                F = new[] 
                {
                    new TableCell { V = "100" } ,
                    new TableCell { V = null } ,
                    new TableCell { V = "{\"a\": 1}"},
                }
            });

            value.Hoge.Is(100);
            value.MyProperty["a"].Is(1L);
        }

        [TestMethod]
        public void ToJsonSchema()
        {
            var schema = DataTypeUtility.ToTableFieldSchema(typeof(MyClass), pi =>
            {
                if (pi == typeof(MyClass).GetProperty("MyProperty"))
                {
                    return new TableFieldSchema { Name = "MyProperty", Type = DataType.String.ToIdentifier() };
                }
                return null;
            });

            schema.ToJsonSchema().Is(@"[
  {
    ""name"": ""Hoge"",
    ""mode"": ""REQUIRED"",
    ""type"": ""INTEGER""
  },
  {
    ""name"": ""Nano"",
    ""mode"": ""NULLABLE"",
    ""type"": ""TIMESTAMP""
  },
  {
    ""name"": ""MyProperty"",
    ""type"": ""STRING""
  },
  {
    ""name"": ""Next"",
    ""mode"": ""NULLABLE"",
    ""type"": ""RECORD"",
    ""fields"": [
      {
        ""name"": ""BBB"",
        ""mode"": ""NULLABLE"",
        ""type"": ""STRING""
      },
      {
        ""name"": ""ZZZ"",
        ""mode"": ""NULLABLE"",
        ""type"": ""FLOAT""
      }
    ]
  },
  {
    ""name"": ""Xs"",
    ""mode"": ""REPEATED"",
    ""type"": ""INTEGER""
  }
]");
        }
    }
}
