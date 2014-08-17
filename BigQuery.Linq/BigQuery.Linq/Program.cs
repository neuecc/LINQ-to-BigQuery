using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    class Program
    {
        static void Main(string[] args)
        {
            var ctx = new BigQueryContext();
            ctx.From("tablename", new { Hoge = default(int), Huga = default(int) });
            
        }
    }
}
