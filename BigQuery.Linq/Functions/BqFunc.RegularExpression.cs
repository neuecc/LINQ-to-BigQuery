using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    public static partial class BqFunc
    {
        // BigQuery provides regular expression support using the re2 library; see that documentation for its regular expression syntax.
        // Note that the regular expressions are global matches; to start matching at the beginning of a word you must use the ^ character.

        /// <summary>
        /// Returns true if str matches the regular expression. For string matching without regular expressions, use CONTAINS instead of REGEXP_MATCH.
        /// </summary>
        [FunctionName("REGEXP_MATCH", SpecifiedFormatterType = typeof(RegexpMatchFormatter))]
        public static bool RegexpMatch(string str, string regexp)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns the portion of str that matches the capturing group within the regular expression.
        /// </summary>
        [FunctionName("REGEXP_EXTRACT", SpecifiedFormatterType = typeof(RegexpExtractFormatter))]
        public static string RegexpExtract(string str, string regexp)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns a string where any substring of orig_str that matches reg_exp is replaced with replace_str.
        /// For example, REGEXP_REPLACE ('Hello', 'lo', 'p') returns Help.
        /// </summary>
        [FunctionName("REGEXP_REPLACE", SpecifiedFormatterType = typeof(RegexpReplaceFormatter))]
        public static string RegexpReplace(string originalStr, string regexp, string replaceStr)
        {
            throw Invalid();
        }

        class RegexpMatchFormatter : ISpecifiedFormatter
        {
            public string Format(int depth, int indentSize, string fuctionName, MethodCallExpression node)
            {
                var innnerTranslator = new BigQueryTranslateVisitor(0, 0);

                var expr1 = innnerTranslator.VisitAndClearBuffer(node.Arguments[0]);
                var expr2 = innnerTranslator.VisitAndClearBuffer(node.Arguments[1]);

                var format = string.Format("REGEXP_MATCH({0}, r{1})", expr1, expr2);

                return format;
            }
        }

        class RegexpExtractFormatter : ISpecifiedFormatter
        {
            public string Format(int depth, int indentSize, string fuctionName, MethodCallExpression node)
            {
                var innnerTranslator = new BigQueryTranslateVisitor(0, 0);

                var expr1 = innnerTranslator.VisitAndClearBuffer(node.Arguments[0]);
                var expr2 = innnerTranslator.VisitAndClearBuffer(node.Arguments[1]);

                var format = string.Format("REGEXP_EXTRACT({0}, r{1})", expr1, expr2);

                return format;
            }
        }

        class RegexpReplaceFormatter : ISpecifiedFormatter
        {
            public string Format(int depth, int indentSize, string fuctionName, MethodCallExpression node)
            {
                var innnerTranslator = new BigQueryTranslateVisitor(0, 0);

                var expr1 = innnerTranslator.VisitAndClearBuffer(node.Arguments[0]);
                var expr2 = innnerTranslator.VisitAndClearBuffer(node.Arguments[1]);
                var expr3 = innnerTranslator.VisitAndClearBuffer(node.Arguments[2]);

                var format = string.Format("REGEXP_REPLACE({0}, r{1}, {2})", expr1, expr2, expr3);

                return format;
            }
        }
    }
}