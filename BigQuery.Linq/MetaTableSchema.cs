using Google.Apis.Bigquery.v2.Data;
using System;
using System.Collections.Generic;
using System.Linq;

namespace BigQuery.Linq
{
    public class MetaTableSchema
    {
        readonly static HashSet<string> ReservedIdentifiers = new HashSet<string>
        {
            "abstract",
            "as",
            "base",
            "bool",
            "break",
            "byte",
            "case",
            "catch",
            "char",
            "checked",
            "class",
            "const",
            "continue",
            "decimal",
            "default",
            "delegate",
            "do",
            "double",
            "else",
            "enum",
            "event",
            "explicit",
            "extern",
            "false",
            "finally",
            "fixed",
            "float",
            "for",
            "foreach",
            "goto",
            "if",
            "implicit",
            "in",
            "in",
            "int",
            "interface",
            "internal",
            "is",
            "lock",
            "long",
            "namespace",
            "new",
            "null",
            "object",
            "operator",
            "out",
            "out",
            "override",
            "params",
            "private",
            "protected",
            "public",
            "readonly",
            "ref",
            "return",
            "sbyte",
            "sealed",
            "short",
            "sizeof",
            "stackalloc",
            "static",
            "string",
            "struct",
            "switch",
            "this",
            "throw",
            "true",
            "try",
            "typeof",
            "uint",
            "ulong",
            "unchecked",
            "unsafe",
            "ushort",
            "using",
            "virtual",
            "void",
            "volatile",
            "while",
        };


        public MetaTable TableInfo { get; private set; }
        public IList<TableFieldSchema> Fields { get; private set; }

        public MetaTableSchema(MetaTable tableInfo, IList<TableFieldSchema> fields)
        {
            this.TableInfo = tableInfo;
            this.Fields = fields;
        }

        string ToCSharpType(string type, string mode)
        {
            var isNullable = (mode == "NULLABLE");
            switch (type)
            {
                case "STRING":
                    return "string";
                case "INTEGER":
                    return "long" + (isNullable ? "?" : "");
                case "FLOAT":
                    return "double" + (isNullable ? "?" : "");
                case "BOOLEAN":
                    return "bool" + (isNullable ? "?" : "");
                case "TIMESTAMP":
                    return "DateTimeOffset" + (isNullable ? "?" : "");
                case "RECORD":
                    throw new NotSupportedException("Currently, Record is not support.");
                default:
                    throw new InvalidOperationException();
            }
        }

        public string BuildCSharpClass()
        {
            var props = Fields.Select(x =>
            {
                var type = ToCSharpType(x.Type, x.Mode);
                var name = x.Name;
                if (ReservedIdentifiers.Contains(name))
                {
                    name = "@" + name;
                }
                return string.Format("    public {0} {1} {{ get; set; }}", type, name);
            });

            var format = @"[TableName(""{0}"")]
public class {1}
{{
{2}
}}";
            var result = string.Format(format, TableInfo.ToFullTableName(), TableInfo.table_id, string.Join(Environment.NewLine, props));

            return result;
        }

        public override string ToString()
        {
            return TableInfo.ToFullTableName();
        }
    }
}