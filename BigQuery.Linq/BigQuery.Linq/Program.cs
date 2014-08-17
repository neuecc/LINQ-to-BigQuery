using BigQuery.Linq.Functions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    class Hoge
    {
        public double MyProperty { get; set; }
    }


    class Huga
    {
        public double MyProperty2 { get; set; }
    }

    public class HogeMoge
    {
        public string Hoge { get; set; }
        public string Huga { get; set; }
        public int Tako { get; set; }
    }

    class Wikipedia
    {
        public string title { get; set; }

        public int wp_namespace { get; set; }

        public Repository repository { get; set; }
    }

    public class Repository
    {
        public bool has_downloads { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
         

            //new MySource().Execute(
            // var context = new BigQueryContext().From<int>().Select();
            var context = new BigQueryContext();

            var query = new BigQueryContext()
                .From<HogeMoge>()
                .Where(x => x.Tako == 1000 && x.Huga == null)
                .GroupBy(x => new { x.Hoge, x.Huga })
                .Select(x => new { x.Hoge, x.Huga })
                .ToString();


            /*
             * SELECT
  title,
  HASH(title) AS hash_value,
  IF(ABS(HASH(title)) % 2 == 1, 'True', 'False') 
    AS included_in_sample
FROM
  [publicdata:samples.wikipedia]
WHERE
  wp_namespace = 0
LIMIT 5;*/


            // as ↓

            //context.From<Wikipedia>("[publicdata:samples.wikipedia]")
            //    .Where(x => x.wp_namespace == 0)
            //    .Limit(5)
            //    .Select(x => new
            //    {
            //        x.title,
            //        includedInSample = (Mathematical.Abs(Other.Hash(x.title)) % 2 == 1) ? "True" : "False"
            //    });



            Console.WriteLine(query);
        }
    }
}
