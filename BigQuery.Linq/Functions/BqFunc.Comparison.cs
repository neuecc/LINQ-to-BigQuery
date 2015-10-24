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
        // =, !=, >, <, >=, <=, IS NULL, IFNULL is in BigQueryTranslateVisitor.VisitBinary

        /// <summary>
        /// &gt;. Returns true if expr1 is greater than expr2.
        /// </summary>
        [FunctionName(">", SpecifiedFormatterType = typeof(RelationalOperatorFormatter))]
        public static bool GreaterThan(string expr1, string epr2) { throw Invalid(); }

        /// <summary>
        /// &gt;=. Returns true if expr1 is greater than or equal to expr2.
        /// </summary>
        [FunctionName(">=", SpecifiedFormatterType = typeof(RelationalOperatorFormatter))]
        public static bool GreaterThanEqual(string expr1, string epr2) { throw Invalid(); }

        /// <summary>
        /// &lt;. Returns true if expr1 is less than expr2.
        /// </summary>
        [FunctionName("<", SpecifiedFormatterType = typeof(RelationalOperatorFormatter))]
        public static bool LessThan(string expr1, string epr2) { throw Invalid(); }

        /// <summary>
        /// &lt;=. Returns true if expr1 is less than or equal to expr2.
        /// </summary>
        [FunctionName("<=", SpecifiedFormatterType = typeof(RelationalOperatorFormatter))]
        public static bool LessThanEqual(string expr1, string epr2) { throw Invalid(); }

        /// <summary>
        /// Returns true if the value of expr1 is greater than or equal to expr2, and less than or equal to expr3.
        /// </summary>
        [FunctionName("BETWEEN", SpecifiedFormatterType = typeof(BetweenFormatter))]
        public static bool Between<T>(T expr1, T expr2, T expr3) { throw Invalid(); }

        /// <summary>
        /// Returns true if expr matches expr1, expr2, or any value in the parentheses.
        /// The IN keyword is an efficient shorthand for (expr = expr1 || expr = expr2 || ...).
        /// The expressions used with the IN keyword must be constants and they must match the data type of expr
        /// </summary>
        [FunctionName("IN", SpecifiedFormatterType = typeof(InFormatter))]
        public static bool In<T>(T expr, params T[] exprs) { throw Invalid(); }

        /// <summary>
        /// Returns true if expr matches expr1, expr2, or any value in the parentheses.
        /// The IN keyword is an efficient shorthand for (expr = expr1 || expr = expr2 || ...).
        /// The expressions used with the IN keyword must be constants and they must match the data type of expr
        /// Semijoin expression (i.e. "x IN (SELECT ...)") only supported in WHERE or HAVING clauses
        /// </summary>
        [FunctionName("IN", SpecifiedFormatterType = typeof(InSemijoinFormatter))]
        public static bool In<T, TExpr>(T expr, IExecutableBigQueryable<TExpr> exprs) { throw Invalid(); }

        /// <summary>
        /// Returns true if expr matches expr1, expr2, or any value in the parentheses.
        /// The IN keyword is an efficient shorthand for (expr = expr1 || expr = expr2 || ...).
        /// The expressions used with the IN keyword must be constants and they must match the data type of expr
        /// </summary>
        [FunctionName("NOT IN", SpecifiedFormatterType = typeof(InFormatter))]
        public static bool NotIn<T>(T expr, params T[] exprs) { throw Invalid(); }

        /// <summary>
        /// Returns true if expr matches expr1, expr2, or any value in the parentheses.
        /// The IN keyword is an efficient shorthand for (expr = expr1 || expr = expr2 || ...).
        /// The expressions used with the IN keyword must be constants and they must match the data type of expr
        /// Semijoin expression (i.e. "x IN (SELECT ...)") only supported in WHERE or HAVING clauses
        /// </summary>
        [FunctionName("NOT IN", SpecifiedFormatterType = typeof(InSemijoinFormatter))]
        public static bool NotIn<T, TExpr>(T expr, IExecutableBigQueryable<TExpr> exprs) { throw Invalid(); }

        /// <summary>
        /// Returns the first argument that isn't NULL.
        /// </summary>
        [FunctionName("COALESCE", SpecifiedFormatterType = typeof(CoalesceFormatter))]
        public static T Coalesce<T>(params T[] exprs)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns the largest numeric_expr parameter. All parameters must be numeric, and all parameters must be the same type. If any parameter is NULL, this function returns NULL.
        /// </summary>
        [FunctionName("GREATEST", SpecifiedFormatterType = typeof(GreatestFormatter))]
        public static long Greatest(params long?[] exprs)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns the largest numeric_expr parameter. All parameters must be numeric, and all parameters must be the same type. If any parameter is NULL, this function returns NULL.
        /// </summary>
        [FunctionName("GREATEST", SpecifiedFormatterType = typeof(GreatestFormatter))]
        public static double Greatest(params double?[] exprs)
        {
            throw Invalid();
        }

        /// <summary>Returns true if numeric_expr is positive or negative infinity.</summary>
        [FunctionName("IS_INF")]
        public static bool IsInfinity(double? numericExpr) { throw Invalid(); }

        /// <summary>Returns true if numeric_expr is the special NaN numeric value.</summary>
        [FunctionName("IS_NAN")]
        public static bool IsNAN(double? numericExpr) { throw Invalid(); }

        // IS_EXPLICITLY_DEFINED is Deprecated, expr IS NOT NULL instead.

        /// <summary>
        /// Returns the smallest numeric_expr parameter. All parameters must be numeric, and all parameters must be the same type. If any parameter is NULL, this function returns NULL.
        /// </summary>
        [FunctionName("LEAST", SpecifiedFormatterType = typeof(LeastFormatter))]
        public static long Least(params long?[] exprs)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns the smallest numeric_expr parameter. All parameters must be numeric, and all parameters must be the same type. If any parameter is NULL, this function returns NULL.
        /// </summary>
        [FunctionName("LEAST", SpecifiedFormatterType = typeof(LeastFormatter))]
        public static double Least(params double?[] exprs)
        {
            throw Invalid();
        }

        /// <summary>
        /// If expr is not null, returns expr, otherwise returns null_default. The NVL function is an alias for IFNULL.
        /// </summary>
        [FunctionName("NVL")]
        public static T Nvl<T>(T expr, T null_default)
        {
            throw Invalid();
        }

        class RelationalOperatorFormatter : ISpecifiedFormatter
        {
            public string Format(int depth, int indentSize, string fuctionName, MethodCallExpression node)
            {
                var innerTranslator = new BigQueryTranslateVisitor();
                var expr1 = innerTranslator.VisitAndClearBuffer(node.Arguments[0]);
                var expr2 = innerTranslator.VisitAndClearBuffer(node.Arguments[1]);
                return expr1 + " " + fuctionName + " " + expr2;
            }
        }

        class BetweenFormatter : ISpecifiedFormatter
        {
            public string Format(int depth, int indentSize, string fuctionName, MethodCallExpression node)
            {
                var innerTranslator = new BigQueryTranslateVisitor();
                var expr1 = innerTranslator.VisitAndClearBuffer(node.Arguments[0]);
                var expr2 = innerTranslator.VisitAndClearBuffer(node.Arguments[1]);
                var expr3 = innerTranslator.VisitAndClearBuffer(node.Arguments[2]);

                return string.Format("({0} BETWEEN {1} AND {2})", expr1, expr2, expr3);
            }
        }

        class InFormatter : ISpecifiedFormatter
        {
            public string Format(int depth, int indentSize, string fuctionName, MethodCallExpression node)
            {
                var innerTranslator = new BigQueryTranslateVisitor();
                var expr1 = innerTranslator.VisitAndClearBuffer(node.Arguments[0]);
                var arg = node.Arguments[1] as NewArrayExpression;
                var expr2 = string.Join(", ", arg.Expressions.Select(x => innerTranslator.VisitAndClearBuffer(x)));

                return string.Format("{0} {1}({2})", expr1, fuctionName, expr2);
            }
        }

        class InSemijoinFormatter : ISpecifiedFormatter
        {
            public string Format(int depth, int indentSize, string fuctionName, MethodCallExpression node)
            {
                var innerTranslator = new BigQueryTranslateVisitor();
                var expr1 = innerTranslator.VisitAndClearBuffer(node.Arguments[0]);
                var l = Expression.Lambda(node.Arguments[1]);
                var ctor = l.Compile();
                var bq = ctor.DynamicInvoke(null) as ExecutableBigQueryableBase;

                var indent = new string(' ', indentSize * (depth));
                var sb = new StringBuilder();

                sb.AppendLine(expr1 + " " + fuctionName);
                sb.Append(indent);
                sb.AppendLine("(");

                sb.AppendLine(bq.ToQueryString(depth + 1));

                sb.Append(indent);
                sb.Append(")");

                return sb.ToString();
            }
        }

        class GreatestFormatter : ISpecifiedFormatter
        {
            public string Format(int depth, int indentSize, string fuctionName, MethodCallExpression node)
            {
                var innerTranslator = new BigQueryTranslateVisitor();
                var arg = node.Arguments[0] as NewArrayExpression;
                var expr = string.Join(", ", arg.Expressions.Select(x => innerTranslator.VisitAndClearBuffer(x)));

                return string.Format("GREATEST({0})", expr);
            }
        }

        class LeastFormatter : ISpecifiedFormatter
        {
            public string Format(int depth, int indentSize, string fuctionName, MethodCallExpression node)
            {
                var innerTranslator = new BigQueryTranslateVisitor();
                var arg = node.Arguments[0] as NewArrayExpression;
                var expr = string.Join(", ", arg.Expressions.Select(x => innerTranslator.VisitAndClearBuffer(x)));

                return string.Format("LEAST({0})", expr);
            }
        }

        class CoalesceFormatter : ISpecifiedFormatter
        {
            public string Format(int depth, int indentSize, string fuctionName, MethodCallExpression node)
            {
                var innerTranslator = new BigQueryTranslateVisitor();
                var arg = node.Arguments[0] as NewArrayExpression;
                var expr = string.Join(", ", arg.Expressions.Select(x => innerTranslator.VisitAndClearBuffer(x)));

                return string.Format("COALESCE({0})", expr);
            }
        }
    }
}