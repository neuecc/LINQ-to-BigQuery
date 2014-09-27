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
        LeftOuterEach = 3,
        RightOuter = 4,
        RightOuterEach = 5,
        FullOuter = 6,
        FullOuterEach = 7
    }

    internal enum InternalJoinType
    {
        Inner = 0,
        InnerEach = 1,
        LeftOuter = 2,
        LeftOuterEach = 3,
        RightOuter = 4,
        RightOuterEach = 5,
        FullOuter = 6,
        FullOuterEach = 7,
        Cross = 8,
        CrossEach = 9,
    }

    internal interface IJoinBigQueryable
    {
        string[] GetAliasNames();
    }
}

namespace BigQuery.Linq.Query
{
    // [[INNER|LEFT OUTER|CROSS] JOIN [EACH] table_2|(subselect2) [[AS] tablealias2]
    // ON join_condition_1 [... AND join_condition_N ...]]+

    internal class JoinBigQueryable<TOuter, TInner, TResult> : BigQueryable, IJoinBigQueryable<TResult>, IJoinBigQueryable
    {
        readonly InternalJoinType joinType;
        readonly string joinTableName;
        readonly SubqueryBigQueryable<TInner> joinTable;
        readonly FlattenBigQueryable<TInner> joinFlattenTable;
        readonly Expression<Func<TOuter, TInner, TResult>> aliasSelector;
        readonly Expression<Func<TResult, bool>> joinCondition;

        internal override int Order
        {
            get { return 2; }
        }

        public string[] GetAliasNames()
        {
            var aliasExpr = aliasSelector.Body as NewExpression;
            return aliasExpr.Members.Select(x => x.Name).ToArray();
        }

        internal JoinBigQueryable(
            IBigQueryable parent,
            InternalJoinType joinType,
            string joinTableName,
            IExecutableBigQueryable<TInner> joinTable,
            IFlattenBigQueryable<TInner> joinFlattenTable,
            Expression<Func<TOuter, TInner, TResult>> aliasSelector,
            Expression<Func<TResult, bool>> joinCondition)
            : base(parent)
        {
            this.joinType = joinType;
            this.joinTableName = joinTableName;
            this.joinFlattenTable = joinFlattenTable as FlattenBigQueryable<TInner>;
            var sub = joinTable as SubqueryBigQueryable<TInner>;
            if (sub != null)
            {
                this.joinTable = sub;
            }
            else if (joinTable != null)
            {
                this.joinTable = joinTable.AsSubquery() as SubqueryBigQueryable<TInner>;
            }
            this.aliasSelector = aliasSelector;
            this.joinCondition = joinCondition;
        }

        public override string BuildQueryString(int depth)
        {
            var sb = new StringBuilder();
            sb.Append(Indent(depth));

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
                case InternalJoinType.RightOuter:
                    sb.Append("RIGHT OUTER JOIN");
                    break;
                case InternalJoinType.RightOuterEach:
                    sb.Append("RIGHT OUTER JOIN EACH");
                    break;
                case InternalJoinType.FullOuter:
                    sb.Append("FULL OUTER JOIN");
                    break;
                case InternalJoinType.FullOuterEach:
                    sb.Append("FULL OUTER JOIN EACH");
                    break;
                case InternalJoinType.Cross:
                    sb.Append("CROSS JOIN");
                    break;
                case InternalJoinType.CrossEach:
                    sb.Append("CROSS JOIN EACH");
                    break;
                default:
                    throw new InvalidOperationException();
            }

            // table select
            if (joinTableName != null)
            {
                sb.AppendLine();
                sb.Append(Indent(depth + 1));
                sb.Append(joinTableName);
            }
            else if (joinTable != null)
            {
                sb.AppendLine();
                sb.Append(joinTable.BuildQueryStringWithoutFrom(depth));
            }
            else if (joinFlattenTable != null)
            {
                sb.Append(joinFlattenTable.BuildQueryStringWithoutFrom(depth));
            }
            else
            {
                throw new InvalidOperationException();
            }

            // alias select
            var aliasExpr = aliasSelector.Body as NewExpression;
            var aliasName = aliasExpr.Members.Last().Name;
            sb.Append(" AS " + aliasName.EscapeBq());

            // join condition
            if (joinType != InternalJoinType.Cross && joinType != InternalJoinType.CrossEach)
            {
                sb.Append(" ON ");
                var on = BigQueryTranslateVisitor.BuildQuery(0, 0, joinCondition);
                sb.Append(on);
            }

            return sb.ToString();
        }
    }
}