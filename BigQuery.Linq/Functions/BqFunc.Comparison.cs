using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    public static partial class BqFunc
    {
        /// <summary>Returns true if numeric_expr is positive or negative infinity.</summary>
        [FunctionName("IS_INF")]
        public static bool IsInfinity(long numericExpr) { throw Invalid(); }
        /// <summary>Returns true if numeric_expr is positive or negative infinity.</summary>
        [FunctionName("IS_INF")]
        public static bool IsInfinity(double numericExpr) { throw Invalid(); }
        [FunctionName("IS_NAN")]
        /// <summary>Returns true if numeric_expr is the special NaN numeric value.</summary>
        public static bool IsNAN(long numericExpr) { throw Invalid(); }
        /// <summary>Returns true if numeric_expr is the special NaN numeric value.</summary>
        [FunctionName("IS_NAN")]
        public static bool IsNAN(double numericExpr) { throw Invalid(); }
    }
}
