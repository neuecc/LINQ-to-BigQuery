using System;
using System.Collections.Generic;
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

        protected override Expression VisitNew(NewExpression node)
        {
            var indent = BuildIndent();
            var innerTranslator = new BigQueryTranslateVisitor(0, 0);

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
                // cast do nothing, use BqFunc.Boolean,Float, etc...

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
            else if (node.NodeType == ExpressionType.Quote)
            {

            }

            throw new InvalidOperationException("Not supported unary expression:" + node);
        }

        // append field access
        protected override Expression VisitMember(MemberExpression node)
        {
            var nodes = new List<MemberExpression>();
            var next = node;
            while (next != null)
            {
                nodes.Add(next);
                next = next.Expression as MemberExpression;
            }

            sb.Append("[");
            for (int i = nodes.Count - 1; i >= 0; i--)
            {
                sb.Append(nodes[i].Member.Name);
                if (nodes.Count != 1 && i != 0)
                {
                    sb.Append(".");
                }
            }
            sb.Append("]");

            return node;
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            string expr = "";
            var typeCode = (node.Value == null)
                ? TypeCode.Empty
                : Type.GetTypeCode(node.Value.GetType());
            switch (typeCode)
            {
                case TypeCode.Boolean:
                    var b = (bool)node.Value;
                    expr = (b == true) ? "true" : "false";
                    break;
                case TypeCode.Char:
                case TypeCode.String:
                    expr = "\'" + node.Value + "\'";
                    break;
                case TypeCode.DateTime:
                    // TODO:if value is DateTime, need format!
                    expr = node.Value.ToString();
                    break;
                case TypeCode.DBNull:
                case TypeCode.Empty:
                    sb.Append("NULL");
                    break;
                case TypeCode.Decimal:
                case TypeCode.Single:
                case TypeCode.Double:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.SByte:
                case TypeCode.Byte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                    if (node.Value.GetType().IsEnum)
                    {
                        sb.Append("\'" + node.Value + "\'");
                    }
                    else
                    {
                        sb.Append(node.Value);
                    }
                    break;
                case TypeCode.Object:
                    // TODO:it's record?
                    break;
                default:
                    throw new InvalidOperationException();
            }

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
            if (node.Method.GetCustomAttributes<WindowFunctionAttribute>().Any())
            {
                MethodCallExpression root;
                var paritionBy = (MethodCallExpression)node.Object;
                if (paritionBy == null)
                {
                    root = node;
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
                var windowFunction = Expression.Lambda(node, para);
                var compiledWindowFunction = windowFunction.Compile();
                var windowQuery = compiledWindowFunction.DynamicInvoke(new object[1]);
                sb.Append(windowQuery.ToString());
                return node;
            }

            var attr = node.Method.GetCustomAttributes<FunctionNameAttribute>().FirstOrDefault();
            if (attr == null) throw new InvalidOperationException("Not support method:" + node.Method.DeclaringType.Name + "." + node.Method.Name + " Method can only call BigQuery.Linq.Functions.*");

            if (attr.SpecifiedFormatterType != null)
            {
                var formatter = Activator.CreateInstance(attr.SpecifiedFormatterType, true) as ISpeficiedFormatter;
                sb.Append(formatter.Format(node));
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
            var innerTranslator = new BigQueryTranslateVisitor();

            // case when ... then ... ... else .. end
            // TODO:need more clean format
            if (node.IfFalse is ConditionalExpression)
            {
                sb.Append("CASE");

                sb.Append(Environment.NewLine);

                Expression right = node;
                while (right is ConditionalExpression)
                {
                    var rightNode = right as ConditionalExpression;

                    // left
                    sb.Append(" WHEN ");
                    {
                        sb.Append(innerTranslator.VisitAndClearBuffer(rightNode.Test));
                    }

                    sb.Append(" THEN ");
                    {
                        sb.Append(innerTranslator.VisitAndClearBuffer(rightNode.IfTrue));
                    }

                    right = rightNode.IfFalse;
                }

                sb.Append(" ELSE ");
                sb.Append(innerTranslator.VisitAndClearBuffer(right));

                sb.Append(" END");
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