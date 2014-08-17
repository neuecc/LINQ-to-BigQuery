using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    public class BigQueryContext
    {
        // connection

        public AfterFromBigQueryable<T> From<T>()
        {
            return new AfterFromBigQueryable<T>(typeof(T).Name, null);
        }


        public AfterFromBigQueryable<T> From<T>(string tableName)
        {
            return new AfterFromBigQueryable<T>(tableName, null);
        }

        public AfterFromBigQueryable<T> From<T>(string tableName, T dynamicSchema)
        {
            return new AfterFromBigQueryable<T>(tableName, null);
        }

        public AfterFromBigQueryable<T> From<T>(BigQueryable<T> nestedSource)
        {
            return new AfterFromBigQueryable<T>(nestedSource);
        }
    }

    public abstract class BigQueryable
    {
        protected BigQueryable parent { get; set; }

        protected abstract string BuildCommand();
    }

    public abstract class BigQueryable<T> : BigQueryable
    {
        public IEnumerable<T> AsEnumerable()
        {
            throw new NotImplementedException();
        }

        public string BuildQuery()
        {
            string query;
            if (this is AfterSelectBigQueryable<T>)
            {
                query = this.ToString();
            }
            else
            {
                var wrapped = AfterSelectBigQueryable<T>.Create(this, () =>
                {
                    return string.Join(", ", typeof(T).GetProperties().Select(x => x.Name));
                });

                query = wrapped.ToString();
            }

            return query;
        }

        public override string ToString()
        {
            if (this is AfterSelectBigQueryable<T>)
            {
                return (parent != null)
                    ? BuildCommand() + Environment.NewLine + parent.ToString()
                    : BuildCommand();
            }
            else
            {
                return (parent != null)
                    ? parent.ToString() + Environment.NewLine + BuildCommand()
                    : BuildCommand();
            }
        }
    }

    public class AfterSelectBigQueryable<T> : BigQueryable<T>, IEnumerable<T>
    {
        Func<string> buildCommand;

        public AfterSelectBigQueryable(BigQueryable parent)
        {
            this.parent = parent;
        }

        public static AfterSelectBigQueryable<T> Create(BigQueryable parent, Func<string> buildCommand)
        {
            return new AfterSelectBigQueryable<T>(parent) { buildCommand = buildCommand };
        }

        public AfterFromBigQueryable<T> AsNestedQuery()
        {
            return new AfterFromBigQueryable<T>(this);
        }

        public IEnumerator<T> GetEnumerator()
        {
            return AsEnumerable().GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        protected override string BuildCommand()
        {
            return "SELECT" + Environment.NewLine + buildCommand();
        }
    }

    public class AfterLimitBigQueryable<TSource> : BigQueryable<TSource>
    {
        public AfterLimitBigQueryable(BigQueryable parent)
        {
            this.parent = parent;
        }

        public AfterSelectBigQueryable<TResult> Select<TResult>(Expression<Func<TSource, TResult>> selector)
        {
            return AfterSelectBigQueryable<TResult>.Create(this, () => MyExpressionVisitor.BuildCommand(selector));
        }

        protected override string BuildCommand()
        {
            throw new NotImplementedException();
        }
    }

    public class AfterOrderByBigQueryable<T> : AfterLimitBigQueryable<T>
    {
        public AfterOrderByBigQueryable(BigQueryable parent) : base(parent) { }

        public AfterLimitBigQueryable<T> Limit()
        {
            throw new NotImplementedException();
        }

        public AfterOrderByBigQueryable<T> ThenBy()
        {
            throw new NotImplementedException();
        }
    }

    // OrderedBigQueryable

    public class AfterHavingBigQueryable<T> : AfterLimitBigQueryable<T>
    {
        public AfterHavingBigQueryable(BigQueryable parent) : base(parent) { }

        public AfterLimitBigQueryable<T> Limit()
        {
            throw new NotImplementedException();
        }

        public AfterOrderByBigQueryable<T> OrderBy()
        {
            throw new NotImplementedException();
        }
    }


    public class AfterGroupByBigQueryable<T> : AfterHavingBigQueryable<T>
    {
        public Func<string> buildCommand { get; set; }
        public bool each { get; set; }

        public AfterGroupByBigQueryable(BigQueryable parent) : base(parent) { }

        public AfterHavingBigQueryable<T> Having()
        {
            throw new NotImplementedException();
        }

        protected override string BuildCommand()
        {
            return "GROUP " + ((each) ? "EACH BY " : "BY ")
                + Environment.NewLine + buildCommand();
        }
    }

    public class AfterWhereBigQueryable<T> : AfterGroupByBigQueryable<T>
    {
        public AfterWhereBigQueryable(BigQueryable parent) : base(parent) { }

        public AfterGroupByBigQueryable<T> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector, bool each = false)
        {
            return new AfterGroupByBigQueryable<T>(this)
            {
                buildCommand = () => FieldSelectExpressionVisitor.BuildString(keySelector),
                each = each
            };
        }

        protected override string BuildCommand()
        {
            return "WHERE" + Environment.NewLine + buildCommand();
        }
    }

    // public AfterSelectBigQueryable<TResult> Select<TResult>(Expression<Func<TSource, TResult>> selector)

    public class AfterJoinBigQueryable<T> : AfterWhereBigQueryable<T>
    {
        public AfterJoinBigQueryable(BigQueryable parent) : base(parent) { }

        public AfterWhereBigQueryable<T> Where(Expression<Func<T, bool>> predicate)
        {
            return new AfterWhereBigQueryable<T>(this)
            {
                buildCommand = () => ConditionExpressionVisitor.Build(predicate)
            };
        }
    }

    public class AfterTableDecoratedBigQueryable<T> : AfterJoinBigQueryable<T>
    {
        public AfterTableDecoratedBigQueryable(BigQueryable parent)
            : base(parent)
        {

        }

        public AfterJoinBigQueryable<T> Join()
        {
            throw new NotImplementedException();
        }

    }

    public class AfterFromBigQueryable<T> : AfterTableDecoratedBigQueryable<T>
    {
        readonly string tableName;

        public AfterFromBigQueryable(BigQueryable subselect)
            : base(subselect)
        {

        }

        public AfterFromBigQueryable(string tableName, BigQueryable parent)
            : base(parent)
        {
            this.tableName = tableName;
        }

        public AfterTableDecoratedBigQueryable<T> WithSnapshot()
        {
            throw new NotImplementedException();
        }

        public AfterTableDecoratedBigQueryable<T> WithRange()
        {
            throw new NotImplementedException();
        }

        protected override string BuildCommand()
        {
            return "FROM " + Environment.NewLine + "  " + tableName;
        }
    }

    class Hoge
    {
        public double MyProperty { get; set; }
    }


    class Huga
    {
        public double MyProperty2 { get; set; }
    }

    public class HogeMoge
    {
        public string Hoge { get; set; }
        public string Huga { get; set; }
        public int Tako { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
            //new MySource().Execute(
            // var context = new BigQueryContext().From<int>().Select();


            var query = new BigQueryContext()
                .From<HogeMoge>()
                .Where(x => x.Tako == 1000 && x.Huga == null)
                .GroupBy(x => new { x.Hoge, x.Huga })
                .Select(x => new { x.Hoge, x.Huga })
                .ToString();
            var b = true;
            Expression<Func<int>> e = () => (b) ? 100 : 200;


            Console.WriteLine(query);
        }
    }


    public class ConditionalToCaseExpressionVisitor : ExpressionVisitor
    {

    }



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
