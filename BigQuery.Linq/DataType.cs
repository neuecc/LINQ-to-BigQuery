using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    public enum DataType
    {
        String,
        Integer, // long
        Float, // double
        Boolean,
        Timestamp, // DateTime
        Record // object
    }
}