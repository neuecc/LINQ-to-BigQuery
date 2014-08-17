using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Query
{

    public class GroupByBigQueryable<T> : HavingBigQueryable<T>
    {
        public Func<string> buildCommand { get; set; }
        public bool each { get; set; }

        internal GroupByBigQueryable(BigQueryable parent) : base(parent) { }

        public HavingBigQueryable<T> Having()
        {
            throw new NotImplementedException();
        }

        internal override string ToString(int depth, int indentSize, FormatOption option)
        {
            return "GROUP " + ((each) ? "EACH BY " : "BY ")
                + Environment.NewLine + buildCommand();
        }
    }
}
