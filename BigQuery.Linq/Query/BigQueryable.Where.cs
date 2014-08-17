using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Query
{
    public class WhereBigQueryable<TSource> : GroupByBigQueryable<TSource>
    {
        readonly Expression<Func<TSource, bool>> predicate;

        internal WhereBigQueryable(BigQueryable parent) : base(parent) { }
        internal WhereBigQueryable(BigQueryable parent, Expression<Func<TSource, bool>> predicate)
            : base(parent)
        {
            this.predicate = predicate;
        }

        // combine where
        public virtual WhereBigQueryable<TSource> Where(Expression<Func<TSource, bool>> predicate)
        {
            var newBody = Expression.AndAlso(this.predicate.Body, predicate.Body);
            var newPredicate = Expression.Lambda<Func<TSource, bool>>(newBody, this.predicate.Parameters);

            return new WhereBigQueryable<TSource>(Parent, newPredicate);
        }

        public GroupByBigQueryable<TSource> GroupBy<TKey>(Expression<Func<TSource, TKey>> keySelector, bool each = false)
        {
            return new GroupByBigQueryable<TSource>(this)
            {
                // buildCommand = () => FieldSelectExpressionVisitor.BuildString(keySelector),
                each = each
            };
        }

        internal override string ToString(int depth, int indentSize, FormatOption option)
        {
            if (depth < 1) throw new ArgumentOutOfRangeException("depth:" + depth);

            var sb = new StringBuilder();
            var command = BigQueryTranslateVisitor.BuildQuery("WHERE", depth, indentSize, option, predicate);

            sb.Append(command);

            if (Parent != null)
            {
                if (option == FormatOption.Indent)
                {
                    sb.AppendLine();
                }
                else
                {
                    sb.Append(" ");
                }
                sb.Append(Parent.ToString(depth, indentSize, option));
            }

            return sb.ToString();
        }
    }
}
