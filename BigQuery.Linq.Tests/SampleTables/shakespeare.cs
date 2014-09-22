using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Tests
{
    [TableName("[publicdata:samples.shakespeare]")]
    public class shakespeare
    {
        public string word { get; set; }
        public long word_count { get; set; }
        public string corpus { get; set; }
        public long corpus_date { get; set; }
    }
}
