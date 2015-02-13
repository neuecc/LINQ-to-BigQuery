using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    // Expression to BigQuery Query Translater
    internal class BigQueryTranslateVisitor : ExpressionVisitor
    {
        static readonly MethodInfo StringContains = typeof(string).GetMethod("Contains");

        readonly int depth = 1;
        readonly int indentSize = 2;

        StringBuilder sb = new StringBuilder();

        public BigQueryTranslateVisitor()
        {
            this.depth = 0;
            this.indentSize = 0;
        }

        public BigQueryTranslateVisitor(int depth, int indentSize)
        {
            this.depth = depth;
            this.indentSize = indentSize;
        }

        // EntryPoint
        public static string BuildQuery(int depth, int indentSize, Expression expression)
        {
            var visitor = new BigQueryTranslateVisitor(depth, indentSize);
            visitor.Visit(expression);
            return visitor.sb.ToString();
        }

        string BuildIndent()
        {
            return new string(' ', indentSize * depth);
        }

        public string VisitAndClearBuffer(Expression node)
        {
            Visit(node);
            var result = sb.ToString();
            sb.Clear();
            return result;
        }

        static readonly PropertyInfo now = typeof(DateTime).GetProperty("Now");
        static readonly PropertyInfo utcNow = typeof(DateTime).GetProperty("UtcNow");
        static readonly PropertyInfo nowOffset = typeof(DateTimeOffset).GetProperty("Now");
        static readonly PropertyInfo utcNowOffset = typeof(DateTimeOffset).GetProperty("UtcNow");
        bool FormatIfExprIsDateTime(Expression expr)
        {
            if (expr is NewExpression)
            {
                var node = expr as NewExpression;
                if (node.Constructor.DeclaringType == typeof(DateTime) || node.Constructor.DeclaringType == typeof(DateTimeOffset))
                {
                    
                    var parameters = node.Arguments.Select(x => ExpressionHelper.GetValue(x)).ToArray();
                    var datetime = node.Constructor.Invoke(parameters);
                    var v = DataTypeFormatter.Format(datetime);
                    sb.Append(v);
                    return true;
                }
            }
            if (expr is MemberExpression)
            {
                var node = expr as MemberExpression;
                if (node.Member == now) { sb.Append(DataTypeFormatter.Format(DateTime.Now)); return true; }
                if (node.Member == utcNow) { sb.Append(DataTypeFormatter.Format(DateTime.UtcNow)); return true; }
                if (node.Member == nowOffset) { sb.Append(DataTypeFormatter.Format(DateTimeOffset.Now)); return true; }
                if (node.Member == utcNowOffset) { sb.Append(DataTypeFormatter.Format(DateTimeOffset.UtcNow)); return true; }
            }

            return false;
        }

        protected override Expression VisitNew(NewExpression node)
        {
            // specialize for DateTime
            if (FormatIfExprIsDateTime(node)) return node;

            // promising type initializer(goto:VisitMemberInit)
            if (!node.Type.IsAnonymousType()) return node;

            var indent = BuildIndent();
            var innerTranslator = new BigQueryTranslateVisitor(depth, indentSize);

            var merge = node.Members.Zip(node.Arguments, (x, y) =>
            {
                var rightValue = innerTranslator.VisitAndClearBuffer(y);

                if (x.Name == rightValue.Trim('[', ']')) return "[" + x.Name + "]";

                return rightValue + " AS " + "[" + x.Name + "]";
            });

            var command = string.Join("," + Environment.NewLine,
                merge.Select(x => indent + x));

            sb.Append(command);

            return node;
        }

        protected override Expression VisitMemberInit(MemberInitExpression node)
        {
            var indent = BuildIndent();
            var innerTranslator = new BigQueryTranslateVisitor(depth, indentSize);

            var merge = node.Bindings.Select(expr =>
            {
                var assignment = expr as MemberAssignment;
                var x = assignment.Member;
                var y = assignment.Expression;

                var rightValue = innerTranslator.VisitAndClearBuffer(y);

                if (x.Name == rightValue.Trim('[', ']')) return "[" + x.Name + "]";

                return rightValue + " AS " + "[" + x.Name + "]";
            });

            var command = string.Join("," + Environment.NewLine,
                merge.Select(x => indent + x));

            sb.Append(command);

            return node;
        }

        protected override Expression VisitBinary(BinaryExpression node)
        {
            string expr;
            bool isNull = false;
            switch (node.NodeType)
            {
                // Logical operators
                case ExpressionType.AndAlso:
                    expr = "AND";
                    break;
                case ExpressionType.OrElse:
                    expr = "OR";
                    break;
                // Comparison functions
                case ExpressionType.LessThan:
                    expr = "<";
                    break;
                case ExpressionType.LessThanOrEqual:
                    expr = "<=";
                    break;
                case ExpressionType.GreaterThan:
                    expr = ">";
                    break;
                case ExpressionType.GreaterThanOrEqual:
                    expr = ">=";
                    break;
                case ExpressionType.Equal:
                    {
                        var right = node.Right;
                        isNull = (right is ConstantExpression) && ((ConstantExpression)right).Value == null;
                        expr = (isNull) ? "IS NULL" : "=";
                    }
                    break;
                case ExpressionType.NotEqual:
                    {
                        var right = node.Right;
                        isNull = (right is ConstantExpression) && ((ConstantExpression)right).Value == null;
                        expr = (isNull) ? "IS NOT NULL" : "!=";
                    }
                    break;
                case ExpressionType.Coalesce:
                    {
                        sb.Append("IFNULL(");
                        base.Visit(node.Left);
                        sb.Append(", ");
                        base.Visit(node.Right);
                        sb.Append(")");

                        return node;
                    }
                // Arithmetic operators
                case ExpressionType.Add:
                    expr = "+";
                    break;
                case ExpressionType.Subtract:
                    expr = "-";
                    break;
                case ExpressionType.Multiply:
                    expr = "*";
                    break;
                case ExpressionType.Divide:
                    expr = "/";
                    break;
                case ExpressionType.Modulo:
                    expr = "%";
                    break;
                // Bitwise functions
                case ExpressionType.And:
                    expr = "&";
                    break;
                case ExpressionType.Or:
                    expr = "|";
                    break;
                case ExpressionType.ExclusiveOr:
                    expr = "^";
                    break;
                case ExpressionType.LeftShift:
                    expr = "<<";
                    break;
                case ExpressionType.RightShift:
                    expr = ">>";
                    break;
                default:
                    throw new InvalidOperationException("Invalid node type:" + node.NodeType);
            }

            sb.Append("(");

            base.Visit(node.Left); // run to left

            sb.Append(" " + expr);

            if (!isNull)
            {
                sb.Append(" ");
                base.Visit(node.Right); // run to right
            }

            sb.Append(")");

            return node;
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            if (node.NodeType == ExpressionType.Convert)
            {
                // cast do nothing, If need BigQuery specified cast then use BqFunc.Boolean,Float, etc...
                return base.VisitUnary(node);
            }
            else if (node.NodeType == ExpressionType.Not)
            {
                if (node.Type == typeof(bool))
                {
                    sb.Append("NOT "); // Logical operator
                }
                else
                {
                    sb.Append("~"); // Bitwise operator
                }
                return base.VisitUnary(node);
            }

            throw new InvalidOperationException("Not supported unary expression:" + node);
        }

        // append field access
        protected override Expression VisitMember(MemberExpression node)
        {
            // WindowFunction, relieve property call and compile.
            if (node.Member.GetCustomAttributes<WindowFunctionAttribute>().Any())
            {
                var methodNode = node.Expression as MethodCallExpression;

                MethodCallExpression root;
                var paritionBy = (MethodCallExpression)methodNode.Object;
                if (paritionBy == null)
                {
                    root = methodNode;
                }
                else
                {
                    root = (MethodCallExpression)paritionBy.Object;
                    if (root == null)
                    {
                        root = paritionBy;
                    }
                }

                var para = root.Arguments[0] as ParameterExpression;
                var windowFunction = Expression.Lambda(methodNode, para);
                var compiledWindowFunction = windowFunction.Compile();
                var windowQuery = compiledWindowFunction.DynamicInvoke(new object[1]);
                sb.Append(windowQuery.ToString());
                return node;
            }

            // specialize for DateTime
            if (FormatIfExprIsDateTime(node)) return node;

            bool isRootIsParameter = false; // as external field or parameter
            var nodes = new List<MemberExpression>();
            var next = node;
            while (next != null)
            {
                isRootIsParameter = next.Expression.NodeType == ExpressionType.Parameter;
                nodes.Add(next);

                var nextExpr = next.Expression;
                next = nextExpr as MemberExpression;

                if (next == null)
                {
                    // skip indexer access for repeated field
                    var binaryNext = nextExpr;
                    while (binaryNext is BinaryExpression)
                    {
                        binaryNext = ((BinaryExpression)binaryNext).Left;
                        if (binaryNext is MemberExpression)
                        {
                            next = (MemberExpression)binaryNext;
                            break;
                        }
                    }
                }
            }

            if (isRootIsParameter)
            {
                sb.Append("[");
                for (int i = nodes.Count - 1; i >= 0; i--)
                {
                    sb.Append(nodes[i].Member.Name);

                    // If Nullable don't emit .Value
                    if (nodes[i].Type.IsNullable()) break;

                    if (nodes.Count != 1 && i != 0)
                    {
                        sb.Append(".");
                    }
                }
                sb.Append("]");
            }
            else
            {
                var v = ExpressionHelper.GetValue(nodes[0]);
                var str = DataTypeFormatter.Format(v);
                sb.Append(str);
            }

            return node;
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            var expr = DataTypeFormatter.Format(node.Value);
            sb.Append(expr);

            return node;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            // special case, String.Contains
            if (node.Method == StringContains)
            {
                var innerTranslator = new BigQueryTranslateVisitor();
                var expr1 = innerTranslator.VisitAndClearBuffer(node.Object);
                var expr2 = innerTranslator.VisitAndClearBuffer(node.Arguments[0]);
                sb.Append(expr1 + " CONTAINS " + expr2);
                return node;
            }

            // window function
            if (node.Method.GetCustomAttributes<WindowFunctionAlertAttribute>().Any())
            {
                throw new InvalidOperationException("WindowFunction must call .Value property");
            }

            var attr = node.Method.GetCustomAttributes<FunctionNameAttribute>().FirstOrDefault();
            if (attr == null) throw new InvalidOperationException("Not support method:" + node.Method.DeclaringType.Name + "." + node.Method.Name + " Method can only call BigQuery.Linq.Functions.*");

            if (attr.SpecifiedFormatterType != null)
            {
                var formatter = Activator.CreateInstance(attr.SpecifiedFormatterType, true) as ISpecifiedFormatter;
                sb.Append(formatter.Format(depth, indentSize, attr.Name, node));
            }
            else
            {
                sb.Append(attr.Name + "(");

                var innerTranslator = new BigQueryTranslateVisitor();
                var args = string.Join(", ", node.Arguments.Select(x => innerTranslator.VisitAndClearBuffer(x)));

                sb.Append(args);

                sb.Append(")");
            }

            return node;
        }

        protected override Expression VisitConditional(ConditionalExpression node)
        {
            var innerTranslator = new BigQueryTranslateVisitor(depth, indentSize);

            // case when ... then ... ... else .. end
            if (node.IfFalse is ConditionalExpression)
            {
                /*
                 * CASE
                   __WHEN cond THEN value
                 * __WHEN cond THEN value
                 * __ELSE value
                 * END
                 */
                var whenIndent = new string(' ', indentSize * (depth + 1));

                sb.Append("CASE");
                sb.AppendLine();

                // WHEN
                Expression right = node;
                while (right is ConditionalExpression)
                {
                    var rightNode = right as ConditionalExpression;

                    sb.Append(whenIndent);
                    sb.Append("WHEN ");
                    {
                        sb.Append(innerTranslator.VisitAndClearBuffer(rightNode.Test));
                    }

                    sb.Append(" THEN ");
                    {
                        sb.Append(innerTranslator.VisitAndClearBuffer(rightNode.IfTrue));
                    }

                    right = rightNode.IfFalse;
                    sb.AppendLine();
                }

                sb.Append(whenIndent);
                sb.Append("ELSE ");
                sb.Append(innerTranslator.VisitAndClearBuffer(right));

                // END
                sb.AppendLine();
                sb.Append(BuildIndent());
                sb.Append("END");
            }
            else
            {
                sb.Append("IF(");
                {
                    sb.Append(innerTranslator.VisitAndClearBuffer(node.Test));
                }
                sb.Append(", ");
                {
                    sb.Append(innerTranslator.VisitAndClearBuffer(node.IfTrue));
                }
                sb.Append(", ");
                {
                    sb.Append(innerTranslator.VisitAndClearBuffer(node.IfFalse));
                }
                sb.Append(")");
            }
            return node;
        }
    }
}