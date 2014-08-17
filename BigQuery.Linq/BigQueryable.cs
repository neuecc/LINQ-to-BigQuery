
namespace BigQuery.Linq
{
    // BigQuery Query Reference : https://developers.google.com/bigquery/query-reference

    // Syntax:
    /*
    SELECT expr1 [[AS] alias1] [, expr2 [[AS] alias2], ...]
        [agg_function(expr3) WITHIN expr4]
        [FROM [(FLATTEN(table_name1|(subselect1)] [, table_name2|(subselect2), ...)]
        [[INNER|LEFT OUTER|CROSS] JOIN [EACH] table_2|(subselect2) [[AS] tablealias2]
          ON join_condition_1 [... AND join_condition_N ...]]+
        [WHERE condition]
        [GROUP [EACH] BY field1|alias1 [, field2|alias2, ...]]
        [HAVING condition]
        [ORDER BY field1|alias1 [DESC|ASC] [, field2|alias2 [DESC|ASC], ...]]
        [LIMIT n]
        ;
     */

    public abstract class BigQueryable
    {
        internal BigQueryable Parent { get; private set; }

        internal BigQueryContext QueryContext { get; private set; }

        protected BigQueryable(BigQueryable parent)
        {
            this.Parent = parent;
            this.QueryContext = parent.QueryContext;
        }

        protected BigQueryable(BigQueryContext context)
        {
            this.Parent = null;
            this.QueryContext = context;
        }

        public override string ToString()
        {
            return ToString(1, QueryContext.IndentSize, QueryContext.FormatOption);
        }

        public string ToString(FormatOption option)
        {
            return ToString(1, QueryContext.IndentSize, option);
        }

        public string ToString(int indentSize, FormatOption option)
        {
            return ToString(1, indentSize, option);
        }

        internal abstract string ToString(int depth, int indentSize, FormatOption option);
    }

    public abstract class BigQueryable<T> : BigQueryable
    {
        protected BigQueryable(BigQueryable parent)
            : base(parent)
        {

        }

        protected BigQueryable(BigQueryContext context)
            : base(context)
        {
        }
    }

    internal class RootBigQueryable<T> : BigQueryable<T>
    {
        public RootBigQueryable(BigQueryContext context)
            : base(context)
        {

        }

        internal override string ToString(int depth, int indentSize, FormatOption option)
        {
            throw new System.NotImplementedException();
        }
    }
}