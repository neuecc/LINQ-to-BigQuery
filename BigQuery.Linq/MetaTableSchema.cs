using Google.Apis.Bigquery.v2.Data;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

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
                    return name + (isArray ? "[]" : "");
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


        public string BuildCSharpClass(bool outTablePrefixClassIfMatched = false)
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

            var className = TableInfo.table_id;
            if (Regex.IsMatch(className, "^[0123456789]"))
            {
                className = "_" + className;
            }

            var regex = new Regex(@"\d{8}]$");
            var fullname = TableInfo.ToFullTableName();
            string attr;
            if (outTablePrefixClassIfMatched && regex.IsMatch(fullname))
            {
                attr = $"[TablePrefix(\"{regex.Replace(fullname, "]")}\")]";
                className = regex.Replace(className + "]", "").TrimEnd('_', ']');
            }
            else
            {
                attr = $"[TableName(\"{fullname}\")]";
            }

            var tostring = $@"
    public override string ToString()
    {{
        return """"
{string.Join(Environment.NewLine, Fields.Select(x => $"             + \"{x.Name} : \" + {x.Name} + \"|\""))}
             ;
    }}";

        var format = @"{0}
public class {1}
{{
{2}
{3}
}}";
            var result = string.Format(format, attr, className, string.Join(Environment.NewLine, props), tostring);

            return string.Join(Environment.NewLine, new[] { result }.Concat(innerClasses.Select(x => Environment.NewLine + x.Value)));
        }

        /// <summary>
        /// Showing the table schema same as bq --format=prettyjson show
        /// </summary>
        public string ToJsonSchema(Formatting formatting = Formatting.Indented)
        {
            using (var sw = new StringWriter())
            using (var jw = new JsonTextWriter(sw))
            {
                jw.Formatting = formatting;

                jw.WriteStartObject();

                jw.WritePropertyName("id"); jw.WriteValue(TableInfo.project_id + ":" + TableInfo.dataset_id + "." + TableInfo.table_id);
                jw.WritePropertyName("creationTime"); jw.WriteValue(TableInfo.creation_time);
                jw.WritePropertyName("lastModifiedTime"); jw.WriteValue(TableInfo.last_modified_time);
                jw.WritePropertyName("numBytes"); jw.WriteValue(TableInfo.size_bytes);
                jw.WritePropertyName("numRows"); jw.WriteValue(TableInfo.row_count);
                jw.WritePropertyName("type"); jw.WriteValue((TableInfo.type == 2) ? "VIEW" : "TABLE");
                jw.WritePropertyName("tableReference");
                {
                    jw.WriteStartObject();
                    jw.WritePropertyName("projectId"); jw.WriteValue(TableInfo.project_id);
                    jw.WritePropertyName("datasetId"); jw.WriteValue(TableInfo.dataset_id);
                    jw.WritePropertyName("tableId"); jw.WriteValue(TableInfo.table_id);
                    jw.WriteEndObject();
                }
                jw.WritePropertyName("schema");
                {
                    jw.WriteStartObject();
                    jw.WritePropertyName("fields");
                    {
                        jw.WriteStartArray();
                        foreach (var field in Fields)
                        {
                            DataTypeUtility.WriteJsonSchema(jw, field);
                        }
                        jw.WriteEndArray();
                    }
                    jw.WriteEndObject();
                }

                jw.WriteEndObject();

                return sw.ToString();
            }
        }
        public override string ToString()
        {
            return TableInfo.ToFullTableName();
        }
    }
}