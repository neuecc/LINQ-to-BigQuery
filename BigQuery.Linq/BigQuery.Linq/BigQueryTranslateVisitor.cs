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

            sb.Append("  "); // indent depth

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
    }









    // TODO:delete



    internal class FieldSelectExpressionVisitor : ExpressionVisitor
    {
        List<MemberExpression> members = new List<MemberExpression>();

        public static string BuildString(Expression expression)
        {
            var visitor = new FieldSelectExpressionVisitor();
            visitor.Visit(expression);
            // TODO: method call and alias...
            return "  " + string.Join(", ", visitor.members.Select(x => x.Member.Name));
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            members.Add(node);
            return base.VisitMember(node);
        }
    }

    public class ConditionExpressionVisitor : ExpressionVisitor
    {
        // ?? => IFNULL(expr, null_default)
        StringBuilder sb = new StringBuilder();

        public static string Build(Expression expression)
        {
            var visitor = new ConditionExpressionVisitor();
            visitor.Visit(expression);
            return visitor.sb.ToString();
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

            sb.Append("  "); // indent depth

            base.Visit(node.Left); // run to left

            sb.Append(" " + expr + " ");

            if (!isNull)
            {
                base.Visit(node.Right); // run to right
            }

            return node;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            sb.Append(node.Member.Name);
            return base.VisitMember(node);
        }

        protected override Expression VisitConditional(ConditionalExpression node)
        {
            return base.VisitConditional(node);
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            return base.VisitUnary(node);
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            sb.Append(node.Value);
            return base.VisitConstant(node);
        }
    }

    public class MyExpressionVisitor : ExpressionVisitor
    {
        int depth = 1;
        int indentSize = 2;

        StringBuilder commandBuilder = new StringBuilder();

        public static string BuildCommand(Expression expression)
        {
            var visitor = new MyExpressionVisitor();
            visitor.Visit(expression);
            return visitor.commandBuilder.ToString();
        }

        protected override Expression VisitNew(NewExpression node)
        {
            var targetType = node.Type;

            var indent = new string(' ', depth * indentSize);
            var command = string.Join("," + Environment.NewLine,
                targetType.GetProperties().Select(x => indent + x.Name));

            commandBuilder.Append(command);

            return node;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            return base.VisitMethodCall(node);
        }

        protected override Expression VisitInvocation(InvocationExpression node)
        {
            return base.VisitInvocation(node);
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            return base.VisitMember(node);
        }
    }
}