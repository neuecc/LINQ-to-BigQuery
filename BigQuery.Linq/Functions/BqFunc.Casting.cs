using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    public static partial class BqFunc
    {
        // https://developers.google.com/bigquery/query-reference#castingfunctions
        // Casting functions change the data type of a numeric expression.
        // Casting functions are particularly useful for ensuring that arguments in a comparison function have the same data type.

        /// <summary>
        /// <para>Returns true if numeric_expr is not 0 and not NULL.</para>
        /// <para>Returns false if numeric_expr is 0.</para>
        /// <para>Returns NULL if numeric_expr is NULL.</para>
        /// </summary>
        [FunctionName("BOOLEAN")]
        public static bool? Boolean(long? numericExpr) { throw Invalid(); }
        /// <summary>
        /// <para>Returns true if numeric_expr is not 0 and not NULL.</para>
        /// <para>Returns false if numeric_expr is 0.</para>
        /// <para>Returns NULL if numeric_expr is NULL.</para>
        /// </summary>
        [FunctionName("BOOLEAN")]
        public static bool? Boolean(double? numericExpr) { throw Invalid(); }

        /// <summary>
        /// Converts expr into a variable of type type.
        /// </summary>
        [FunctionName("CAST", SpecifiedFormatterType = typeof(CastFormatter))]
        public static T Cast<T>(object expr) { throw Invalid(); }

        /// <summary>
        /// Returns expr as a double. The expr can be a string like '45.78', but the function returns NULL for non-numeric values.
        /// </summary>
        [FunctionName("FLOAT")]
        public static double? Float(object expr) { throw Invalid(); }

        /// <summary>
        /// Returns numeric_expr as a hexadecimal string.
        /// </summary>
        [FunctionName("HEX_STRING")]
        public static string HexString(long? numericExpr) { throw Invalid(); }

        /// <summary>
        /// Returns numeric_expr as a hexadecimal string.
        /// </summary>
        [FunctionName("HEX_STRING")]
        public static string HexString(double? numericExpr) { throw Invalid(); }

        /// <summary>
        /// Returns expr as a 64-bit integer. This function expects an integer string, such as '45', and returns NULL for non-integer values.
        /// </summary>
        [FunctionName("INTEGER")]
        public static long? Integer(object expr) { throw Invalid(); }

        /// <summary>
        /// Returns numeric_expr as a string.
        /// </summary>
        [FunctionName("STRING")]
        public static string String(long? numericExpr) { throw Invalid(); }

        /// <summary>
        /// Returns numeric_expr as a string.
        /// </summary>
        [FunctionName("STRING")]
        public static string String(double? numericExpr) { throw Invalid(); }

        class CastFormatter : ISpecifiedFormatter
        {
            public string Format(int depth, int indentSize, string functionName, MethodCallExpression node)
            {
                var type = node.Method.GetGenericArguments()[0];
                var dataType = DataTypeUtility.ToDataType(type);
                var identifier = dataType.ToIdentifier();

                var innerTranslator = new BigQueryTranslateVisitor();
                var expr = innerTranslator.VisitAndClearBuffer(node.Arguments[0]);

                return $"{functionName}({expr} AS {identifier})";
            }
        }
    }
}
