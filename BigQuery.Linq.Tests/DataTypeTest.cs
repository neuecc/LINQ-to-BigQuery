using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Google.Apis.Bigquery.v2.Data;

namespace BigQuery.Linq.Tests
{
    [TestClass]
    public class DataTypeTest
    {
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
                new TableFieldSchema(){ Name = "title", Type = "STRING", Mode = "REQUIRED"},
                new TableFieldSchema(){ Name = "id", Type = "INTEGER", Mode = "NULLABLE"},
                new TableFieldSchema(){ Name = "language", Type = "STRING", Mode = "REQUIRED"},
                new TableFieldSchema(){ Name = "wp_namespace", Type = "INTEGER", Mode = "REQUIRED"},
                new TableFieldSchema(){ Name = "is_redirect", Type = "BOOLEAN", Mode = "NULLABLE"},
                new TableFieldSchema(){ Name = "revision_id", Type = "INTEGER", Mode = "NULLABLE"},
                new TableFieldSchema(){ Name = "contributor_ip", Type = "STRING", Mode = "REQUIRED"},
                new TableFieldSchema(){ Name = "contributor_id", Type = "INTEGER", Mode = "NULLABLE"},
                new TableFieldSchema(){ Name = "contributor_username", Type = "STRING", Mode = "REQUIRED"},
                new TableFieldSchema(){ Name = "timestamp", Type = "INTEGER", Mode = "REQUIRED"},
                new TableFieldSchema(){ Name = "is_minor", Type = "BOOLEAN", Mode = "NULLABLE"},
                new TableFieldSchema(){ Name = "is_bot", Type = "BOOLEAN", Mode = "NULLABLE"},
                new TableFieldSchema(){ Name = "reversion_id", Type = "INTEGER", Mode = "NULLABLE"},
                new TableFieldSchema(){ Name = "comment", Type = "STRING", Mode = "REQUIRED"},
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
                new TableFieldSchema(){ Name = "Nano", Type = "INTEGER", Mode = "REQUIRED"},
                new TableFieldSchema(){ Name = "K", Type = "INTEGER", Mode = "NULLABLE"},
                new TableFieldSchema(){ Name = "C", Type = "INTEGER", Mode = "REQUIRED"},
            });
        }
    }
}
