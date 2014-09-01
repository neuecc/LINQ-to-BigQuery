using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    public static partial class BqFunc
    {
        [FunctionName("ABS")]
        public static int Abs(long numericExpr) { throw Invalid(); }
        [FunctionName("ABS")]
        public static int Abs(double numericExpr) { throw Invalid(); }
    }
}
