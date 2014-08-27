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
        Inner,
        LeftOuter,
        Cross
    }
}

namespace BigQuery.Linq.Query
{

    // TODO:table & expression

    internal class JoinBigQueryable<T> : BigQueryable, IJoinBigQueryable<T>
    {
        readonly JoinType joinType;
        readonly bool each;

        internal JoinBigQueryable(IBigQueryable parent, JoinType joinType, bool each)
            : base(parent)
        {
            this.joinType = joinType;
            this.each = each;
        }

        public override string ToString(int depth, int indentSize, FormatOption option)
        {
            throw new NotImplementedException();
        }
    }
}