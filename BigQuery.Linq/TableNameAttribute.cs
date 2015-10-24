using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class TableNameAttribute : Attribute
    {
        public string TableName { get; private set; }

        public TableNameAttribute(string tableName)
        {
            this.TableName = tableName;
        }
    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class TablePrefixAttribute : Attribute
    {
        public string TablePrefix { get; private set; }

        public TablePrefixAttribute(string tablePrefix)
        {
            this.TablePrefix = tablePrefix;
        }
    }

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
    public class ColumnNameAttribute : Attribute
    {
        public string ColumnName { get; private set; }

        public ColumnNameAttribute(string columnName)
        {
            this.ColumnName = columnName;
        }
    }
}
