using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    public static partial class BqFunc
    {
        [FunctionName("REGEXP_MATCH", SpecifiedFormatterType = typeof(RegexpMatchFormatter))]
        public static bool RegexpMatch(string str, string regexp)
        {
            throw Invalid();
        }

        class RegexpMatchFormatter : ISpeficiedFormatter
        {
            public string Format(System.Linq.Expressions.MethodCallExpression node)
            {
                var innnerTranslator = new BigQueryTranslateVisitor(0, 0);

                var expr1 = innnerTranslator.VisitAndClearBuffer(node.Arguments[0]);
                var expr2 = innnerTranslator.VisitAndClearBuffer(node.Arguments[1]);

                var format = string.Format("REGEXP_MATCH({0}, r{1})", expr1, expr2);

                return format;
            }
        }
    }
}