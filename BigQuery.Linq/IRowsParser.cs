using System.Collections.Generic;
using Google.Apis.Bigquery.v2.Data;

namespace BigQuery.Linq
{
    /// <summary>
    /// Parses rows as a specified type
    /// </summary>
    public interface IRowsParser
    {
        IEnumerable<T> Parse<T>(TableSchema schema, IEnumerable<TableRow> rows, bool isDynamic);
    }
}