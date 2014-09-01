using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    public static partial class BqFunc
    {
        // =, !=, >, <, >=, <=, IS NULL, IFNULL is in BigQueryTranslateVisitor.VisitBinary

        /// <summary>
        /// Returns true if the value of expr1 is greater than or equal to expr2, and less than or equal to expr3.
        /// </summary>
        [FunctionName("BETWEEN", SpecifiedFormatterType = typeof(BetweenFormatter))]
        public static bool Between(object expr1, object expr2, object expr3) { throw Invalid(); }

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

        class BetweenFormatter : ISpeficiedFormatter
        {
            public string Format(System.Linq.Expressions.MethodCallExpression node)
            {
                var innerTranslator = new BigQueryTranslateVisitor(0, 0, FormatOption.Flat);
                var expr1 = innerTranslator.VisitAndClearBuffer(node.Arguments[0]);
                var expr2 = innerTranslator.VisitAndClearBuffer(node.Arguments[1]);
                var expr3 = innerTranslator.VisitAndClearBuffer(node.Arguments[2]);

                return string.Format("{0} BETWEEN {1} AND {2}", expr1, expr2, expr3);
            }
        }
    }
}