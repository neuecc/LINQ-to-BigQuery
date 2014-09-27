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
        /// <para>LINQ to BigQuery characteristic function.</para>
        /// <para>Inner value unwrapped and to constant string.</para>
        /// </summary>
        [FunctionName("AsConstant", SpecifiedFormatterType = typeof(AsConstantFormatter))]
        public static T AsConstant<T>(T value)
        {
            throw Invalid();
        }

        public class AsConstantFormatter : ISpeficiedFormatter
        {
            public string Format(int depth, int indentSize, string fuctionName, System.Linq.Expressions.MethodCallExpression node)
            {
                var expr = node.Arguments[0];
                var l = Expression.Lambda(expr);
                var d = l.Compile();
                var v = d.DynamicInvoke(null);

                var str = DataTypeFormatter.Format(v);
                return str;
            }
        }
    }
}