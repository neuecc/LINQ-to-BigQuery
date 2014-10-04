using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

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
    }
}
