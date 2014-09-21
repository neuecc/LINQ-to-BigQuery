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
        /// <summary>
        /// AVG(numeric_expr). Returns the average of the values for a group of rows computed by numeric_expr. Rows with a NULL value are not included in the calculation.
        /// </summary>
        [FunctionName("AVG")]
        public static long Average(object numericExpression)
        {
            throw Invalid();
        }

        /// <summary>
        /// CORR(numeric_expr, numeric_expr). Returns the Pearson correlation coefficient of a set of number pairs.
        /// </summary>
        [FunctionName("CORR")]
        public static int Correlation(long numericExpression1, long numericExpression2)
        {
            throw Invalid();
        }


        [FunctionName("COVAR_POP")]
        public static int CovariancePop()
        {
            throw Invalid();
        }

        [FunctionName("COVAR_SAMP")]
        public static int CovarianceSample()
        {
            throw Invalid();
        }

        /// <summary>
        /// COUNT(*). Returns the total number of values (NULL and non-NULL) in the scope of the function. Unless you are using COUNT(*) with the TOP function, it is better to explicitly specify the field to count.
        /// </summary>
        [FunctionName("COUNT", SpecifiedFormatterType = typeof(CountAllFormatter))]
        public static int Count()
        {
            throw Invalid();
        }

        [FunctionName("COUNT", SpecifiedFormatterType = typeof(CountFieldFormatter))]
        public static int Count(object field)
        {
            throw Invalid();
        }

        [FunctionName("COUNT", SpecifiedFormatterType = typeof(CountFieldFormatter))]
        public static int Count(object field, int n)
        {
            throw Invalid();
        }

        [FunctionName("COUNT(DISTINCT)", SpecifiedFormatterType = typeof(CountDistinctFormatter))]
        public static int CountDistinct(object field)
        {
            throw Invalid();
        }

        [FunctionName("COUNT(DISTINCT)", SpecifiedFormatterType = typeof(CountDistinctFormatter))]
        public static int CountDistinct(object field, int n)
        {
            throw Invalid();
        }

        [FunctionName("MAX")]
        public static long Max(object field)
        {
            throw Invalid();
        }

        [FunctionName("MIN")]
        public static long Min(object field)
        {
            throw Invalid();
        }

        class CountAllFormatter : ISpeficiedFormatter
        {
            public string Format(MethodCallExpression node)
            {
                return "COUNT(*)";
            }
        }

        class CountFieldFormatter : ISpeficiedFormatter
        {
            public string Format(MethodCallExpression node)
            {
                var innerTranslator = new BigQueryTranslateVisitor();
                var args = string.Join(", ", node.Arguments.Select(x => innerTranslator.VisitAndClearBuffer(x)));

                return "COUNT(" + args + ")";
            }
        }

        class CountDistinctFormatter : ISpeficiedFormatter
        {
            public string Format(MethodCallExpression node)
            {
                var innerTranslator = new BigQueryTranslateVisitor();
                var args = string.Join(", ", node.Arguments.Select(x => innerTranslator.VisitAndClearBuffer(x)));

                return "COUNT(DISTINCT " + args + ")";
            }
        }
    }
}
