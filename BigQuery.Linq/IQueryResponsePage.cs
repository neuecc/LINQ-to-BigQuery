using System;
using System.Threading;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    public interface IQueryResponsePage<T>
    {
        TimeSpan ExecutionTime { get; }
        T[] Rows { get; }
        ulong? TotalRows { get; }

        QueryResponsePage<T> NextPage();
        Task<QueryResponsePage<T>> NextPageAsync(CancellationToken token = default(CancellationToken));
    }
}