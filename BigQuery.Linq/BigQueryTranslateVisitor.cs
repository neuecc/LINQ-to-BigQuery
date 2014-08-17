using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    // Expression to BigQuery Query Translate
    internal class BigQueryTranslateVisitor : ExpressionVisitor
    {
        readonly int depth = 1;
        readonly int indentSize = 2;
        readonly FormatOption option;

        StringBuilder sb = new StringBuilder();

        public BigQueryTranslateVisitor(int depth, int indentSize, FormatOption option)
        {
            this.depth = depth;
            this.indentSize = indentSize;
            this.option = option;
        }

        // EntryPoint
        public static string BuildQuery(string command, int depth, int indentSize, FormatOption option, Expression expression)
        {
            var visitor = new BigQueryTranslateVisitor(depth, indentSize, option);

            visitor.sb.Append(command);
            if (option == FormatOption.Indent)
            {
                visitor.sb.Append(Environment.NewLine);
            }
            else
            {
                visitor.sb.Append(" ");
            }

            visitor.Visit(expression);
            return visitor.sb.ToString();
        }

        void AppendIndent()
        {
            if (option == FormatOption.Indent)
            {
                sb.Append(new string(' ', indentSize * depth));
            }
        }

        protected override Expression VisitNew(NewExpression node)
        {
            var targetType = node.Type;

            var indent = new string(' ', depth * indentSize);
            var command = string.Join("," + Environment.NewLine,
                targetType.GetProperties().Select(x => indent + x.Name));

            sb.Append(command);

            return node;
        }

        protected override Expression VisitBinary(BinaryExpression node)
        {
            string expr;
            bool isNull = false;
            switch (node.NodeType)
            {
                case ExpressionType.AndAlso:
                    expr = "&&";
                    break;
                case ExpressionType.OrElse:
                    expr = "||";
                    break;
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
                default:
                    throw new InvalidOperationException("Invalid node type:" + node.NodeType);
            }

            base.Visit(node.Left); // run to left

            sb.Append(" " + expr + " ");

            if (!isNull)
            {
                base.Visit(node.Right); // run to right
            }

            return node;
        }

        // Casting functions(lack of HexString convert)
        protected override Expression VisitUnary(UnaryExpression node)
        {
            if (node.NodeType == ExpressionType.Convert)
            {
                var typeCode = Type.GetTypeCode(node.Type);
                switch (typeCode)
                {
                    case TypeCode.Boolean:
                        sb.Append("BOOLEAN(");
                        break;
                    case TypeCode.Int16:
                    case TypeCode.Int32:
                    case TypeCode.Int64:
                        sb.Append("INTEGER(");
                        break;
                    case TypeCode.Single:
                    case TypeCode.Double:
                        sb.Append("FLOAT(");
                        break;
                    case TypeCode.String:
                        sb.Append("STRING(");
                        break;
                    default:
                        throw new InvalidOperationException("Not supported cast type:" + node.Type);
                }

                var expr = base.VisitUnary(node);

                sb.Append(")");

                return expr;
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

            // for record type
            for (int i = nodes.Count - 1; i >= 0; i--)
            {
                sb.Append(nodes[i].Member.Name);
                if (nodes.Count != 1 && i != 0)
                {
                    sb.Append(".");
                }
            }

            return node;
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            // TODO:if value is DateTime, need format!
            sb.Append(node.Value);
            return base.VisitConstant(node);
        }
    }
}