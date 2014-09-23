using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    // expr : object
    // numeric_expr : long, double
    // timestamp: DateTime

    [AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
    internal class FunctionNameAttribute : Attribute
    {
        public string Name { get; private set; }
        public Type SpecifiedFormatterType { get; set; }

        public FunctionNameAttribute(string name)
        {
            this.Name = name;
        }
    }

    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    internal class WindowFunctionAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
    internal class WindowFunctionAlertAttribute : Attribute
    {
    }

    internal interface ISpeficiedFormatter
    {
        string Format(MethodCallExpression node);
    }

    public static partial class BqFunc
    {
        const string InvalidMessage = "Function is marker for Query Analyze. Can't call directly.";

        static InvalidOperationException Invalid()
        {
            throw new InvalidOperationException(InvalidMessage);
        }
    }
}
