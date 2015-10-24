using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    public class DuplicateNamingStorage
    {
        Dictionary<string, int> d = new Dictionary<string, int>();

        public int StoreName(string name)
        {
            lock (d)
            {
                int currentCount;
                if (d.TryGetValue(name, out currentCount))
                {
                    currentCount++;
                    d[name] = currentCount;
                    return currentCount;
                }
                else
                {
                    d[name] = 0;
                    return -1;
                }
            }
        }
    }
}