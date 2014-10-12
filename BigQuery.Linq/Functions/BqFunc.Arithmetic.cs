using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    public static partial class BqFunc
    {
        /// <summary>Returns the number of bits that are set in numeric_expr.</summary>
        [FunctionName("BIT_COUNT")]
        public static int BitCount(long? numericExpression)
        {
            throw Invalid();
        }
    }
}