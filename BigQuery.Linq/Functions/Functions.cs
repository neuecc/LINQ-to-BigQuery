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

    internal interface ISpecifiedFormatter
    {
        string Format(int depth, int indentSize, string functionName, MethodCallExpression node);
    }

    public static partial class BqFunc
    {
        const string InvalidMessage = "Function is marker for Query Analyze. Can't call directly.";

        static InvalidOperationException Invalid()
        {
            throw new InvalidOperationException(InvalidMessage);
        }

        // WITHIN clause

        /// <summary>
        /// <para>WITHIN RECORD</para>
        /// <para>The WITHIN keyword specifically works with aggregate functions to aggregate across children and repeated fields within records and nested fields.</para>
        /// <para>Aggregates data in the repeated values within the record, where a record represents an entire unique protocol buffer message, including children and repeated values.</para>
        /// </summary>
        [FunctionName("WITHIN", SpecifiedFormatterType = typeof(WithInRecordFormatter))]
        public static T WithIn<T>(T expr)
        {
            throw Invalid();
        }

        /// <summary>
        /// <para>WITHIN node_name</para>
        /// <para>The WITHIN keyword specifically works with aggregate functions to aggregate across children and repeated fields within records and nested fields.</para>
        /// <para>Aggregates data in the repeated values within the specified node, where a node is a parent node of the field in the aggregation function.</para>
        /// </summary>
        [FunctionName("WITHIN", SpecifiedFormatterType = typeof(WithInNodeFormatter))]
        public static T WithIn<T>(T expr, object node)
        {
            throw Invalid();
        }

        /// <summary>
        /// The GROUPING modifier allows you to distinguish between rows that BigQuery added because of the ROLLUP modifier, versus rows that have a NULL value.
        /// </summary>
        [FunctionName("GROUPING")]
        public static T Grouping<T>(T expr)
        {
            throw Invalid();
        }

        class WithInRecordFormatter : ISpecifiedFormatter
        {
            public string Format(int depth, int indentSize, string fuctionName, MethodCallExpression node)
            {
                var expr = node.Arguments[0];
                var str = BigQueryTranslateVisitor.BuildQuery(depth, indentSize, expr);

                return str + " WITHIN RECORD";
            }
        }

        class WithInNodeFormatter : ISpecifiedFormatter
        {
            public string Format(int depth, int indentSize, string fuctionName, MethodCallExpression node)
            {
                var expr = node.Arguments[0];
                var translater = new BigQueryTranslateVisitor(depth, indentSize);
                var nodeName = translater.VisitAndClearBuffer(node.Arguments[1]);
                var str = translater.VisitAndClearBuffer(expr);

                return str + " WITHIN " + nodeName;
            }
        }
    }
}
