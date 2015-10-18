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
        /// You can use str1 + str2 + ..., too.
        /// </summary>
        [FunctionName("CONCAT", SpecifiedFormatterType = typeof(ConcatFormatter))]
        public static string Concat(params string[] str)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns true if expr contains the specified string argument. This is a case-sensitive comparison.
        /// You can use String.Contains - foo.Contains(bar), too.
        /// </summary>
        [FunctionName("CONTAINS", SpecifiedFormatterType = typeof(ContainsFormatter))]
        public static bool Contains(string expr, string str)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns the one-based index of the first occurrence of str2 in str1, or returns 0 if str2 does not occur in str1.
        /// </summary>
        [FunctionName("INSTR")]
        public static long InStr(string str1, string str2)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns the leftmost numeric_expr characters of str. If the number is longer than str, the full string will be returned.
        /// Example: LEFT('seattle', 3) returns sea.
        /// </summary>
        [FunctionName("LEFT")]
        public static string Left(string str, long length)
        {
            throw Invalid();
        }

        /// <summary>
        /// ReturnsRemoves characters from the left side of str1.
        /// If str2 is omitted, LTRIM removes spaces from the left side of str1.
        /// Otherwise, LTRIM removes any characters in str2 from the left side of str1 (case-sensitive).
        /// </summary>
        [FunctionName("LTRIM")]
        public static string TrimLeft(string str1)
        {
            throw Invalid();
        }

        /// <summary>
        /// ReturnsRemoves characters from the left side of str1.
        /// If str2 is omitted, LTRIM removes spaces from the left side of str1.
        /// Otherwise, LTRIM removes any characters in str2 from the left side of str1 (case-sensitive).
        /// </summary>
        [FunctionName("LTRIM")]
        public static string TrimLeft(string str1, string str2)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns a numerical value for the length of the string.
        /// Example: if str is '123456', LENGTH returns 6.
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
        /// Example: LPAD('1', 7, '?') returns ??????1.
        /// </summary>
        [FunctionName("LPAD")]
        public static string PadLeft(string str, int totalWidth, string paddingString)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns the righmost numeric_expr characters of str. If the number is longer than the string, it will return the whole string.
        /// Example: RIGHT('kirkland', 4) returns land.
        /// </summary>
        [FunctionName("RIGHT")]
        public static string Right(string str, long length)
        {
            throw Invalid();
        }

        /// <summary>
        /// Pads str1 on the right with str2, repeating str2 until the result string is exactly numeric_expr characters.
        /// Example: RPAD('1', 7, '?') returns 1??????.
        /// </summary>
        [FunctionName("RPAD")]
        public static string PadRight(string str, int totalWidth, string paddingString)
        {
            throw Invalid();
        }

        /// <summary>
        /// Removes trailing characters from the right side of str1.
        /// If str2 is omitted, RTRIM removes trailing spaces from str1.
        /// Otherwise, RTRIM removes any characters in str2 from the right side of str1 (case-sensitive).
        /// </summary>
        [FunctionName("RTRIM")]
        public static string TrimRight(string str1)
        {
            throw Invalid();
        }

        /// <summary>
        /// Removes trailing characters from the right side of str1.
        /// If str2 is omitted, RTRIM removes trailing spaces from str1.
        /// Otherwise, RTRIM removes any characters in str2 from the right side of str1 (case-sensitive).
        /// </summary>
        [FunctionName("RTRIM")]
        public static string TrimRight(string str1, string str2)
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
        /// Returns a substring of str, starting at index.
        /// If the optional max_len parameter is used, the returned string is a maximum of max_len characters long.
        /// Counting starts at 1, so the first character in the string is in position 1 (not zero).
        /// If index is 5, the substring begins with the 5th character from the left in str.
        /// If index is -4, the substring begins with the 4th character from the right in str.
        /// Example: SUBSTR('awesome', -4, 4) returns the substring some.
        /// </summary>
        [FunctionName("SUBSTR")]
        public static string Substring(string str, int index)
        {
            throw Invalid();
        }

        /// <summary>
        /// Returns a substring of str, starting at index.
        /// If the optional max_len parameter is used, the returned string is a maximum of max_len characters long.
        /// Counting starts at 1, so the first character in the string is in position 1 (not zero).
        /// If index is 5, the substring begins with the 5th character from the left in str.
        /// If index is -4, the substring begins with the 4th character from the right in str.
        /// Example: SUBSTR('awesome', -4, 4) returns the substring some.
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

        class ConcatFormatter : ISpecifiedFormatter
        {
            public string Format(int depth, int indentSize, string fuctionName, MethodCallExpression node)
            {
                var innerTranslator = new BigQueryTranslateVisitor();

                var arg = node.Arguments[0] as NewArrayExpression;
                var expr = string.Join(", ", arg.Expressions.Select(x => innerTranslator.VisitAndClearBuffer(x)));
                return "CONCAT(" + expr + ")";
            }
        }

        class ContainsFormatter : ISpecifiedFormatter
        {
            public string Format(int depth, int indentSize, string fuctionName, MethodCallExpression node)
            {
                var innerTranslator = new BigQueryTranslateVisitor();
                var expr = innerTranslator.VisitAndClearBuffer(node.Arguments[0]);
                var str = innerTranslator.VisitAndClearBuffer(node.Arguments[1]);

                return expr + " CONTAINS " + str;
            }
        }
    }
}
