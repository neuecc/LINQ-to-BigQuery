using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    public static partial class BqFunc
    {
        // Case => () ? :
        // If => () ? :
        // see:BigQueryTranslateVisitor.VisitConditional

        /// <summary>
        /// Computes and returns a 64-bit signed hash value of the bytes of expr as defined by the CityHash library. Any string or integer expression is supported and the function respects IGNORE CASE for strings, returning case invariant values.
        /// </summary>
        [FunctionName("HASH")]
        public static long Hash(object expr) { throw Invalid(); }

        /// <summary>
        /// Returns the one-based, sequential position of field within a set of repeated fields.
        /// </summary>
        [FunctionName("POSITION")]
        public static long Position(object field)
        {
            throw Invalid();
        }
    }
}
