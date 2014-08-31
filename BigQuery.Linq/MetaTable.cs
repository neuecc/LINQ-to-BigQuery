
namespace BigQuery.Linq
{
    /// <summary>
    /// Table meta data shown by __TABLES__. This definition under
    /// </summary>
    public class MetaTable
    {
        public string project_id { get; set; }
        public string dataset_id { get; set; }
        public string table_id { get; set; }
        public long creation_time { get; set; }
        public long last_modified_time { get; set; }
        public long row_count { get; set; }
        public long size_bytes { get; set; }
        public long type { get; set; }
    }
}
