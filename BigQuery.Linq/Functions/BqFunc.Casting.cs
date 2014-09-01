using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    public static partial class BqFunc
    {
        /// <summary>
        /// <para>Returns true if numeric_expr is not 0 and not NULL.</para>
        /// <para>Returns false if numeric_expr is 0.</para>
        /// <para>Returns NULL if numeric_expr is NULL.</para>
        /// </summary>
        [FunctionName("BOOLEAN")]
        public static bool? Boolean(object numericExpr) { throw Invalid(); }

        /// <summary>
        /// Returns expr as a double. The expr can be a string like '45.78', but the function returns NULL for non-numeric values.
        /// </summary>
        [FunctionName("FLOAT")]
        public static double? Float(object expr) { throw Invalid(); }

        /// <summary>
        /// Returns numeric_expr as a hexadecimal string.
        /// </summary>
        [FunctionName("HEX_STRING")]
        public static string HexString(object expr) { throw Invalid(); }

        /// <summary>
        /// Returns expr as a 64-bit integer. This function expects an integer string, such as '45', and returns NULL for non-integer values.
        /// </summary>
        [FunctionName("INTEGER")]
        public static long? Integer(object expr) { throw Invalid(); }

        /// <summary>
        /// Returns numeric_expr as a string.
        /// </summary>
        [FunctionName("STRING")]
        public static string String(object numericExpr) { throw Invalid(); }
    }
}
