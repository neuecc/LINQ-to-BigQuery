using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    public static partial class BqFunc
    {
        // BigQuery's JSON functions give you the ability to find values within your stored JSON data, by using JSONPath-like expressions.

        // Storing JSON data can be more flexible than declaring all of your individual fields in your table schema, but can lead to higher costs. When you select data from a JSON string, you are charged for scanning the entire string, which is more expensive than if each field is in a separate column. The query is also slower since the entire string needs to be parsed at query time. But for ad-hoc or rapidly-changing schemas, the flexibility of JSON can be worth the extra cost.

        // Use JSON functions instead of BigQuery's regular expression functions if working with structured data, as JSON functions are easier to use.

        /// <summary>
        /// Selects a value in json according to the JSONPath expression json_path. json_path must be a string constant. Returns the value in JSON string format.
        /// </summary>
        [FunctionName("JSON_EXTRACT")]
        public static string JsonExtract(string json, string jsonPath)
        {
            throw Invalid();
        }

        /// <summary>
        /// Selects a value in json according to the JSONPath expression json_path. json_path must be a string constant. Returns a scalar JSON value.
        /// </summary>
        [FunctionName("JSON_EXTRACT_SCALAR")]
        public static string JsonExtractScalar(string json, string jsonPath)
        {
            throw Invalid();
        }
    }
}
