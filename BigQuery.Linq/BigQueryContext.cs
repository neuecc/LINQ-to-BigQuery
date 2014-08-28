using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using BigQuery.Linq.Functions;
using BigQuery.Linq.Query;

namespace BigQuery.Linq
{
    public class BigQueryContext
    {
        // connection

        public int IndentSize { get; set; }
        public FormatOption FormatOption { get; set; }

        public BigQueryContext()
        {
            IndentSize = 2;
            FormatOption = Linq.FormatOption.Indent;
        }

        public IFromBigQueryable<T> From<T>()
        {
            return new FromBigQueryable<T>(typeof(T).Name, new RootBigQueryable<T>(this));
        }

        public IFromBigQueryable<T> From<T>(string tableName)
        {
            return new FromBigQueryable<T>(tableName, new RootBigQueryable<T>(this));
        }

        public IFromBigQueryable<T> From<T>(string tableName, T dynamicSchema)
        {
            return new FromBigQueryable<T>(tableName, new RootBigQueryable<T>(this));
        }

        public ISubqueryBigQueryable<T> From<T>(IExecutableBigQueryable<T> nestedSource)
        {
            return new SubqueryBigQueryable<T>(nestedSource);
        }

        public IExecutableBigQueryable<T> Select<T>(Expression<Func<T>> selector)
        {
            var unusedParameter = Expression.Parameter(typeof(T), "_");
            var wrapped = Expression.Lambda<Func<T, T>>(selector.Body, unusedParameter);

            return new SelectBigQueryable<T, T>(new RootBigQueryable<T>(this), wrapped);
        }

        public IEnumerable<T> Query<T>(string query)
        {
            throw new NotImplementedException();
        }
    }
}
