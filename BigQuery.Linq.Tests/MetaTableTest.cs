using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Google.Apis.Bigquery.v2.Data;
using System.Collections.Generic;

namespace BigQuery.Linq.Tests
{
    [TestClass]
    public class MetaTableTest
    {
        [TestMethod]
        public void Construction()
        {
            {
                var mt = new MetaTable("publicdata:samples.github_timeline");
                mt.project_id.Is("publicdata");
                mt.dataset_id.Is("samples");
                mt.table_id.Is("github_timeline");
            }

            {
                var mt = new MetaTable("[publicdata:samples.github_timeline]");
                mt.project_id.Is("publicdata");
                mt.dataset_id.Is("samples");
                mt.table_id.Is("github_timeline");
            }
        }

        [TestMethod]
        public void BuildCSharpClass()
        {
            var schema = new MetaTableSchema(new MetaTable("bigquery-samples:wikipedia_pageviews_s2.201001"), new List<TableFieldSchema>()
            {
                new TableFieldSchema(){ Name = "a", Type = DataType.Integer.ToIdentifier()}
            });

            schema.BuildCSharpClass().Is(@"[TableName(""[bigquery-samples:wikipedia_pageviews_s2.201001]"")]
public class _201001
{
    [ColumnName(""a"")]public long a { get; set; }
}");
            
        }
    }
}
