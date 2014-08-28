using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    public enum JoinType
    {
        Inner = 0,
        InnerEach = 1,
        LeftOuter = 2,
        LeftOuterEach = 3
    }

    internal enum InternalJoinType
    {
        Inner = 0,
        InnerEach = 1,
        LeftOuter = 2,
        LeftOuterEach = 3,
        Cross = 4
    }
}

namespace BigQuery.Linq.Query
{
    // [[INNER|LEFT OUTER|CROSS] JOIN [EACH] table_2|(subselect2) [[AS] tablealias2]
    // ON join_condition_1 [... AND join_condition_N ...]]+

    internal class JoinBigQueryable<TOuter, TInner, TResult> : BigQueryable, IJoinBigQueryable<TResult>
    {
        readonly InternalJoinType joinType;
        readonly string joinTableName;
        readonly ISubqueryBigQueryable<TInner> joinTable;
        readonly Expression<Func<TOuter, TInner, TResult>> aliasSelector;
        readonly Expression<Func<TResult, bool>> joinCondition;

        internal JoinBigQueryable(
            IBigQueryable parent,
            InternalJoinType joinType,
            string joinTableName,
            IExecutableBigQueryable<TInner> joinTable,
            Expression<Func<TOuter, TInner, TResult>> aliasSelector,
            Expression<Func<TResult, bool>> joinCondition)
            : base(parent)
        {
            this.joinType = joinType;
            this.joinTableName = joinTableName;
            var sub = joinTable as ISubqueryBigQueryable<TInner>;
            if (sub != null)
            {
                this.joinTable = sub;
            }
            else if (joinTable != null)
            {
                this.joinTable = joinTable.AsSubquery();
            }
            this.aliasSelector = aliasSelector;
            this.joinCondition = joinCondition;
        }

        public override string ToString(int depth, int indentSize, FormatOption option)
        {
            var sb = new StringBuilder();
            switch (joinType)
            {
                case InternalJoinType.Inner:
                    sb.Append("INNER JOIN");
                    break;
                case InternalJoinType.InnerEach:
                    sb.Append("INNER JOIN EACH");
                    break;
                case InternalJoinType.LeftOuter:
                    sb.Append("LEFT OUTER JOIN");
                    break;
                case InternalJoinType.LeftOuterEach:
                    sb.Append("LEFT OUTER JOIN EACH");
                    break;
                case InternalJoinType.Cross:
                    sb.Append("CROSS JOIN");
                    break;
                default:
                    throw new InvalidOperationException();
            }

            sb.Append(" ");

            // table select
            if (joinTableName != null)
            {
                sb.Append(joinTableName);
            }
            else if (joinTable != null)
            {
                sb.Append("(");

                sb.Append(joinTable.ToString()); // depth &indent?

                sb.Append(")");
            }
            else
            {
                throw new InvalidOperationException();
            }

            // alias select
            // 1.same type, 2.another name, 3.last
            // TODO:not yet done!

            // join condition
            if (joinType != InternalJoinType.Cross)
            {
                sb.Append("ON ");
                var on = BigQueryTranslateVisitor.BuildQuery("", 0, 0, FormatOption.Flat, joinCondition);
                sb.Append(on);
            }

            return sb.ToString();
        }
    }
}