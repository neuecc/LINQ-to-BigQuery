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
        /// <para>Returns true if expr contains the specified string argument. This is a case-sensitive comparison.</para>
        /// <para>You can use String.Contains - foo.Contains(bar), too.</para>
        /// </summary>
        [FunctionName("CONTAINS", SpecifiedFormatterType = typeof(ContainsFormatter))]
        public static bool Contains(string expr, string str)
        {
            throw Invalid();
        }

        [FunctionName("LENGTH")]
        public static long Length(string str)
        {
            throw Invalid();
        }

        class ContainsFormatter : ISpeficiedFormatter
        {
            public string Format(System.Linq.Expressions.MethodCallExpression node)
            {
                var innerTranslator = new BigQueryTranslateVisitor();
                var expr = innerTranslator.VisitAndClearBuffer(node.Arguments[0]);
                var str = innerTranslator.VisitAndClearBuffer(node.Arguments[1]);
                
                return expr + " CONTAINS " + str;
            }
        }
    }
}
