using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    public static partial class BqFunc
    {
        /// <summary>Returns the absolute value of the argument.</summary>
        [FunctionName("ABS")]
        public static long Abs(long? value) { throw Invalid(); }
        /// <summary>Returns the absolute value of the argument.</summary>
        [FunctionName("ABS")]
        public static double Abs(double? value) { throw Invalid(); }

        /// <summary>Returns the arc cosine of the argument.</summary>
        [FunctionName("ACOS")]
        public static double Acos(double? value) { throw Invalid(); }

        /// <summary>Returns the arc hyperbolic cosine of the argument.</summary>
        [FunctionName("ACOSH")]
        public static double Acosh(double? value) { throw Invalid(); }

        /// <summary>Returns the arc sine of the argument.</summary>
        [FunctionName("ASIN")]
        public static double Asin(double? value) { throw Invalid(); }

        /// <summary>Returns the arc hyperbolic sine of the argument.</summary>
        [FunctionName("ASINH")]
        public static double Asinh(double? value) { throw Invalid(); }

        /// <summary>Returns the arc tangent of the argument.</summary>
        [FunctionName("ATAN")]
        public static double Atan(double? value) { throw Invalid(); }

        /// <summary>Returns the arc hyperbolic tangent of the argument.</summary>
        [FunctionName("ATANH")]
        public static double Atanh(double? value) { throw Invalid(); }

        /// <summary>Returns the arc tangent of the two arguments.</summary>
        [FunctionName("ATAN2")]
        public static double Atan2(double? value1, double? value2) { throw Invalid(); }

        /// <summary>Rounds the argument up to the nearest whole number and returns the rounded value.</summary>
        [FunctionName("CEIL")]
        public static double Ceil(double? value) { throw Invalid(); }

        /// <summary>Returns the cosine of the argument.</summary>
        [FunctionName("COS")]
        public static double Cos(double? value) { throw Invalid(); }

        /// <summary>Returns the hyperbolic cosine of the argument.</summary>
        [FunctionName("COSH")]
        public static double Cosh(double? value) { throw Invalid(); }

        /// <summary>Returns numeric_expr, converted from radians to degrees.</summary>
        [FunctionName("DEGREES")]
        public static double Degrees(double? value) { throw Invalid(); }

        /// <summary>Rounds the argument down to the nearest whole number and returns the rounded value.</summary>
        [FunctionName("FLOOR")]
        public static double Floor(double? value) { throw Invalid(); }

        /// <summary>Returns the result of raising the constant "e" - the base of the natural logarithm - to the power of numeric_expr.</summary>
        [FunctionName("EXP")]
        public static double Exp(long? value) { throw Invalid(); }

        /// <summary>Returns the result of raising the constant "e" - the base of the natural logarithm - to the power of numeric_expr.</summary>
        [FunctionName("EXP")]
        public static double Exp(double? value) { throw Invalid(); }

        /// <summary>Returns the natural logarithm of the argument.</summary>
        [FunctionName("LOG")]
        public static double Log(double? value) { throw Invalid(); }

        /// <summary>Returns the Base-2 logarithm of the argument.</summary>
        [FunctionName("LOG2")]
        public static double Log2(double? value) { throw Invalid(); }

        /// <summary>Returns the Base-10 logarithm of the argument.</summary>
        [FunctionName("LOG10")]
        public static double Log10(double? value) { throw Invalid(); }

        /// <summary>Returns the constant π. The PI() function requires parentheses to signify that it is a function, but takes no arguments in those parentheses. You can use PI() like a constant with mathematical and arithmetic functions.</summary>
        [FunctionName("PI")]
        public static double PI() { throw Invalid(); }

        /// <summary>Returns the result of raising numeric_expr1 to the power of numeric_expr2.</summary>
        [FunctionName("POW")]
        public static double Pow(double? x, double? y) { throw Invalid(); }

        /// <summary>Returns numeric_expr, converted from degrees to radians. (Note that π radians equals 180 degrees.)</summary>
        [FunctionName("RADIANS")]
        public static double Radians(double? value) { throw Invalid(); }

        /// <summary>
        /// Returns a random float value in the range 0.0 &lt;= value &lt; 1.0.
        /// Each int32_seed value always generates the same sequence of random numbers within a given query, as long as you don't use a LIMIT clause.
        /// If int32_seed is not specified, BigQuery uses the current timestamp as the seed value.
        /// </summary>
        [FunctionName("RAND")]
        public static double Random() { throw Invalid(); }

        /// <summary>
        /// Returns a random float value in the range 0.0 &lt;= value &lt; 1.0.
        /// Each int32_seed value always generates the same sequence of random numbers within a given query, as long as you don't use a LIMIT clause.
        /// If int32_seed is not specified, BigQuery uses the current timestamp as the seed value.
        /// </summary>
        [FunctionName("RAND")]
        public static double Random(int seed) { throw Invalid(); }

        /// <summary>Rounds the argument either up or down to the nearest whole number (or if specified, to the specified number of digits) and returns the rounded value.</summary>
        [FunctionName("ROUND")]
        public static double Round(double? value) { throw Invalid(); }
        /// <summary>Rounds the argument either up or down to the nearest whole number (or if specified, to the specified number of digits) and returns the rounded value.</summary>
        [FunctionName("ROUND")]
        public static double Round(double? value, int digits) { throw Invalid(); }

        /// <summary>Returns the sine of the argument.</summary>
        [FunctionName("SIN")]
        public static double Sin(double? value) { throw Invalid(); }

        /// <summary>Returns the hyperbolic sine of the argument.</summary>
        [FunctionName("SINH")]
        public static double Sinh(double? value) { throw Invalid(); }

        /// <summary>Returns the square root of the expression.</summary>
        [FunctionName("SQRT")]
        public static double Sqrt(double? value) { throw Invalid(); }

        /// <summary>Returns the tangent of the argument.</summary>
        [FunctionName("TAN")]
        public static double Tan(double? value) { throw Invalid(); }

        /// <summary>Returns the hyperbolic tangent of the argument.</summary>
        [FunctionName("TANH")]
        public static double Tanh(double? value) { throw Invalid(); }
    }
}
