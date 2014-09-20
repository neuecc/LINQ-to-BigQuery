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
        // String functions operate on string data. String constants must be enclosed with single or double quotes. String functions are case-sensitive by default and should use LATIN-1 encoding only (use UTF-8 encoding if necessary). You can append IGNORE CASE to the end of a query to enable case-insensitive matching. IGNORE CASE works only for LATIN-1 strings.

        /// <summary>
        /// Returns the concatenation of two or more strings, or NULL if any of the values are NULL.
        /// <para>You can use str1 + str2 + ..., too.</para>
        /// </summary>
        [FunctionName("CONCAT", SpecifiedFormatterType = typeof(ConcatFormatter))]
        public static string Concat(params string[] str)
        {
            throw Invalid();
        }

        /// <summary>
        /// <para>Returns true if expr contains the specified string argument. This is a case-sensitive comparison.</para>
        /// <para>You can use String.Contains - foo.Contains(bar), too.</para>
        /// </summary>
        [FunctionName("CONTAINS", SpecifiedFormatterType = typeof(ContainsFormatter))]
        public static bool Contains(string expr, string str)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns the leftmost numeric_expr characters of str. If the number is longer than str, the full string will be returned.
        /// <para>Example: LEFT('seattle', 3) returns sea.</para>
        /// </summary>
        [FunctionName("LEFT")]
        public static string Left(string str, long length)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns a numerical value for the length of the string.
        /// <para>Example: if str is '123456', LENGTH returns 6.</para>
        /// </summary>
        [FunctionName("LENGTH")]
        public static long Length(string str)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns the original string with all characters in lower case. Works for LATIN-1 characters only.
        /// </summary>
        [FunctionName("LOWER")]
        public static string Lower(string str)
        {
            throw Invalid();
        }

        /// <summary>
        /// Pads str1 on the left with str2, repeating str2 until the result string is exactly numeric_expr characters.
        /// <para>Example: LPAD('1', 7, '?') returns ??????1.</para>
        /// </summary>
        [FunctionName("LPAD")]
        public static string PadLeft(string str, int totalWidth, string paddingString)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns the righmost numeric_expr characters of str. If the number is longer than the string, it will return the whole string.
        /// <para>Example: RIGHT('kirkland', 4) returns land.</para>
        /// </summary>
        [FunctionName("RIGHT")]
        public static string Right(string str, long length)
        {
            throw Invalid();
        }

        /// <summary>
        /// Pads str1 on the right with str2, repeating str2 until the result string is exactly numeric_expr characters.
        /// <para>Example: RPAD('1', 7, '?') returns 1??????.</para>
        /// </summary>
        [FunctionName("RPAD")]
        public static string PadRight(string str, int totalWidth, string paddingString)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns a set of substrings as a repeated string. If delimiter is specified, the SPLIT function breaks str into substrings, using delimiter as the delimiter.
        /// </summary>
        [FunctionName("SPLIT")]
        public static string Split(string str)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns a set of substrings as a repeated string. If delimiter is specified, the SPLIT function breaks str into substrings, using delimiter as the delimiter.
        /// </summary>
        [FunctionName("SPLIT")]
        public static string Split(string str, string delimiter)
        {
            throw Invalid();
        }

        /// <summary>
        /// <para>Returns a substring of str, starting at index.</para>
        /// <para>If the optional max_len parameter is used, the returned string is a maximum of max_len characters long.</para>
        /// <para>Counting starts at 1, so the first character in the string is in position 1 (not zero).</para>
        /// <para>If index is 5, the substring begins with the 5th character from the left in str.</para>
        /// <para>If index is -4, the substring begins with the 4th character from the right in str.</para>
        /// <para>Example: SUBSTR('awesome', -4, 4) returns the substring some.</para>
        /// </summary>
        [FunctionName("SUBSTR")]
        public static string Substring(string str, int index)
        {
            throw Invalid();
        }

        /// <summary>
        /// <para>Returns a substring of str, starting at index.</para>
        /// <para>If the optional max_len parameter is used, the returned string is a maximum of max_len characters long.</para>
        /// <para>Counting starts at 1, so the first character in the string is in position 1 (not zero).</para>
        /// <para>If index is 5, the substring begins with the 5th character from the left in str.</para>
        /// <para>If index is -4, the substring begins with the 4th character from the right in str.</para>
        /// <para>Example: SUBSTR('awesome', -4, 4) returns the substring some.</para>
        /// </summary>
        [FunctionName("SUBSTR")]
        public static string Substring(string str, int index, int maxLength)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns the original string with all characters in upper case. Works for LATIN-1 characters only.
        /// </summary>
        [FunctionName("UPPER")]
        public static string Upper(string str)
        {
            throw Invalid();
        }

        class ConcatFormatter : ISpeficiedFormatter
        {
            public string Format(System.Linq.Expressions.MethodCallExpression node)
            {
                var innerTranslator = new BigQueryTranslateVisitor();

                var arg = node.Arguments[0] as NewArrayExpression;
                var expr = string.Join(", ", arg.Expressions.Select(x => innerTranslator.VisitAndClearBuffer(x)));
                return "CONCAT(" + expr + ")";
            }
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
