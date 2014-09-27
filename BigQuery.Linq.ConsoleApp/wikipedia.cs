using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    [TableName("[publicdata:samples.wikipedia]")]
    public class wikipedia
    {
        public string title { get; set; }
        public long? id { get; set; }
        public string language { get; set; }
        public long wp_namespace { get; set; }
        public bool? is_redirect { get; set; }
        public long? revision_id { get; set; }
        public string contributor_ip { get; set; }
        public long? contributor_id { get; set; }
        public string contributor_username { get; set; }
        public long timestamp { get; set; }
        public bool? is_minor { get; set; }
        public bool? is_bot { get; set; }
        public long? reversion_id { get; set; }
        public string comment { get; set; }
        public long num_characters { get; set; }
    }
}
