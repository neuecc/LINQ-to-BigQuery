
namespace BigQuery.Linq
{
    /// <summary>
    /// Table meta data shown by __TABLES__. This definition under
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
    }
}
