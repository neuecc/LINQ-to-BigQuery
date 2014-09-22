
using Google.Apis.Bigquery.v2;
using Google.Apis.Bigquery.v2.Data;
using System.Collections.Generic;
using System.Threading.Tasks;
namespace BigQuery.Linq
{
    /// <summary>
    /// Table meta data shown by __TABLES__.
    /// </summary>
    public class MetaTable
    {
        /// <summary>name of the project.</summary>
        public string project_id { get; set; }
        /// <summary>name of the dataset.</summary>
        public string dataset_id { get; set; }
        /// <summary>name of the table.</summary>
        public string table_id { get; set; }
        /// <summary>time, in milliseconds since 1/1/1970 UTC, that the table was created. This is the same as the creation_time field on the table.</summary>
        public long creation_time { get; set; }
        /// <summary>time, in milliseconds since 1/1/1970 UTC, that the table was updated (either metadata or table contents).</summary>
        public long last_modified_time { get; set; }
        /// <summary>number of rows in the table.</summary>
        public long row_count { get; set; }
        /// <summary>total size in bytes of the table.</summary>
        public long size_bytes { get; set; }
        /// <summary>whether it is a view (2) or regular table (1).</summary>
        public long type { get; set; }

        public MetaTable()
        {

        }

        public MetaTable(string project_id, string dataset_id, string table_id)
        {
            this.project_id = project_id;
            this.dataset_id = dataset_id;
            this.table_id = table_id;
        }

        public MetaTableSchema GetTableSchema(BigqueryService service)
        {
            var response = service.Tables.Get(project_id, dataset_id, table_id).Execute();
            return new MetaTableSchema(this, response.Schema.Fields);
        }

        public async Task<MetaTableSchema> GetTableSchemaAsync(BigqueryService service)
        {
            var response = await service.Tables.Get(project_id, dataset_id, table_id).ExecuteAsync().ConfigureAwait(false);
            return new MetaTableSchema(this, response.Schema.Fields);
        }

        public string ToFullTableName()
        {
            var tableInfo = string.Format("[{0}:{1}.{2}]", project_id, dataset_id, table_id);
            return tableInfo;
        }

        public override string ToString()
        {
            var tableInfo = string.Format("[{0}:{1}.{2}]", project_id, dataset_id, table_id);
            var sizeInfo = string.Format("Size:{0}, RowCount:{1}", size_bytes.ToHumanReadableSize(), row_count);
            var time = string.Format("Created:{0}, LastModified:{1}", creation_time.FromTimestampMilliSeconds(), last_modified_time.FromTimestampMilliSeconds());
            return tableInfo + sizeInfo + ", " + time;
        }
    }
}
