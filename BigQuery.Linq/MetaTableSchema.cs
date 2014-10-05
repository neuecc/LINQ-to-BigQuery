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

        static string ToCSharpType(string name, string type, string mode)
        {
            var isNullable = (mode == "NULLABLE");
            var isArray = (mode == "REPEATED");
            switch (type)
            {
                case "STRING":
                    return "string" + (isArray ? "[]" : "");
                case "INTEGER":
                    return "long" + (isNullable ? "?" : "") + (isArray ? "[]" : "");
                case "FLOAT":
                    return "double" + (isNullable ? "?" : "") + (isArray ? "[]" : "");
                case "BOOLEAN":
                    return "bool" + (isNullable ? "?" : "") + (isArray ? "[]" : "");
                case "TIMESTAMP":
                    return "DateTimeOffset" + (isNullable ? "?" : "") + (isArray ? "[]" : "");
                case "RECORD":
                    return name + (isNullable ? "?" : "") + (isArray ? "[]" : "");
                default:
                    throw new InvalidOperationException();
            }
        }

        static string InnerBuildCSharpClass(string className, IList<TableFieldSchema> fields, Dictionary<string, string> innerClasses)
        {
            var props = fields.Select(x =>
            {
                var name = x.Name;
                if (ReservedIdentifiers.Contains(name))
                {
                    name = "@" + name;
                }
                var type = ToCSharpType(name, x.Type, x.Mode);
                if (x.Type == "RECORD" && !innerClasses.ContainsKey(name))
                {
                    innerClasses[name] = InnerBuildCSharpClass(name, x.Fields, innerClasses);
                }

                return string.Format("    public {0} {1} {{ get; set; }}", type, name);
            });

            var format = @"public class {0}
{{
{1}
}}";
            var result = string.Format(format, className, string.Join(Environment.NewLine, props));

            return result;
        }


        public string BuildCSharpClass()
        {
            var innerClasses = new Dictionary<string, string>();
            var props = Fields.Select(x =>
            {
                var name = x.Name;
                if (ReservedIdentifiers.Contains(name))
                {
                    name = "@" + name;
                }
                var type = ToCSharpType(name, x.Type, x.Mode);
                if (x.Type == "RECORD" && !innerClasses.ContainsKey(name))
                {
                    innerClasses[name] = InnerBuildCSharpClass(name, x.Fields, innerClasses);
                }

                return string.Format("    public {0} {1} {{ get; set; }}", type, name);
            });

            var format = @"[TableName(""{0}"")]
public class {1}
{{
{2}
}}";
            var result = string.Format(format, TableInfo.ToFullTableName(), TableInfo.table_id, string.Join(Environment.NewLine, props));

            return string.Join(Environment.NewLine, new[] { result }.Concat(innerClasses.Select(x => Environment.NewLine + x.Value)));
        }

        public override string ToString()
        {
            return TableInfo.ToFullTableName();
        }
    }
}