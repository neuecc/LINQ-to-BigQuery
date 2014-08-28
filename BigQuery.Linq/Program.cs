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


            //var query = new BigQueryContext()
            //    .From<HogeMoge>()
            //    .Where(x => x.Tako == 1000 && x.Huga == null)
            //    .GroupBy(x => new { x.Hoge, x.Huga })
            //    .Limit(10)
            //    .Select(x => new { x.Hoge, x.Huga })
            //    .ToString();



            /*
SELECT
  title,
  HASH(title) AS hash_value,
  IF(ABS(HASH(title)) % 2 = 1, 'True', 'False') AS included_in_sample
FROM
  [publicdata:samples.wikipedia]
WHERE
  wp_namespace = 0
LIMIT 5;
             
             */


            // context.From<Wikipedia>("[publicdata:samples.wikipedia]").Where(x => x)

            var context = new BigQueryContext();

            var query = context.From<Wikipedia>("[publicdata:samples.wikipedia]")
                .Where(x => x.wp_namespace == 0)
                .Select(x => new
                {
                    x.title,
                    hashValue = Other.Hash(x.title),
                    count1 = Aggregate.Count(),
                    count2 = Aggregate.Count(x.title),
                    count3 = Aggregate.CountDistinct(x.title),
                    count4 = Aggregate.CountDistinct(x.title, 100),
                    logical = !(x.title == "hoge"),
                    includedInSample = (Mathematical.Abs(Other.Hash(x.title)) % 2 == 1) ? "True" : "False",
                    casewhentes = (x.title == "aaa") ? "b" : (x.title == "bbb") ? "c" : (x.title == "ddd") ? "e" : "f"
                })
                .Limit(5);





            Console.WriteLine(query.ToString());
        }
    }
}
