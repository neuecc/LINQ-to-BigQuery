using Google.Apis.Bigquery.v2.Data;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace BigQuery.Linq
{
    public class GroupedMetaTableSchema
    {
        public MetaTableSchema[] MetaTableSchemas { get; set; }
        public bool IsGrouped { get; set; }
        public string TablePrefix { get; set; }
        public string ShortTablePrefix { get; set; }
    }

    public class BuildCodeResult
    {
        public MetaTableSchema MetaTableSchema { get; set; }
        public bool IsTableName { get; set; }
        public bool IsTablePrefix { get; set; }
        public bool IsRecordClass { get; set; }
        public string ClassName { get; set; }
        public string Code { get; set; }
    }

    public class MetaTableSchema
    {
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

        static string InnerBuildCSharpClass(string className, IList<TableFieldSchema> fields, Dictionary<string, string> innerClasses, DuplicateNamingStorage namingStorage)
        {
            var props = fields.Select(x =>
            {
                var name = NameConverter.ConvertSafeName(x.Name);

                var type = ToCSharpType(name, x.Type, x.Mode);
                if (x.Type == "RECORD")
                {
                    var innerStoreCount = namingStorage.StoreName(name);
                    var newName = name;
                    if (innerStoreCount != -1)
                    {
                        newName += "__" + innerStoreCount;
                    }

                    type = newName;
                    innerClasses[newName] = InnerBuildCSharpClass(newName, x.Fields, innerClasses, namingStorage);
                }

                return $"    [ColumnName(\"{x.Name}\")]public {type} {name} {{ get; set; }}";
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
            return string.Join(Environment.NewLine, BuildCSharpClasses(outTablePrefixClassIfMatched).Select(x => x.Code));
        }

        /// <summary>
        /// not flatten nested table.
        /// </summary>
        public BuildCodeResult[] BuildCSharpClasses(bool outTablePrefixClassIfMatched = false, DuplicateNamingStorage namingStorage = null)
        {
            if (namingStorage == null) namingStorage = new DuplicateNamingStorage();

            var innerClasses = new Dictionary<string, string>();
            var props = Fields.Select(x =>
            {
                var name = NameConverter.ConvertSafeName(x.Name);

                var type = ToCSharpType(name, x.Type, x.Mode);
                if (x.Type == "RECORD")
                {
                    var innerStoreCount = namingStorage.StoreName(name);
                    var newName = name;
                    if (innerStoreCount != -1)
                    {
                        newName += "__" + innerStoreCount;
                    }

                    type = newName;
                    innerClasses[newName] = InnerBuildCSharpClass(newName, x.Fields, innerClasses, namingStorage);
                }

                return $"    [ColumnName(\"{x.Name}\")]public {type} {name} {{ get; set; }}";
            });

            var className = NameConverter.ConvertSafeName(TableInfo.table_id);

            var regex = new Regex(@"\d{8}]$");
            var fullname = TableInfo.ToFullTableName();
            bool isTable;
            string attr;
            if (outTablePrefixClassIfMatched && regex.IsMatch(fullname))
            {
                isTable = false;
                attr = $"[TablePrefix(\"{regex.Replace(fullname, "]")}\")]";
                className = regex.Replace(className + "]", "").TrimEnd('_', ']');
            }
            else
            {
                isTable = true;
                attr = $"[TableName(\"{fullname}\")]";
            }

            // already stored, incr naming
            var storeCount = namingStorage.StoreName(className);
            if (storeCount != -1)
            {
                className += "__" + storeCount;
            }

            var format = @"{0}
public class {1}
{{
{2}
}}";
            var result = string.Format(format, attr, className, string.Join(Environment.NewLine, props));

            return new[] { new BuildCodeResult { ClassName = className, Code = result, IsTableName = isTable, IsTablePrefix = !isTable, MetaTableSchema = this } }
                .Concat(innerClasses.Select(x => new BuildCodeResult { ClassName = x.Key, Code = x.Value, IsRecordClass = true }))
                .ToArray();
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

        public string ToClassName(bool outTablePrefixClassIfMatched)
        {
            var className = NameConverter.ConvertSafeName(TableInfo.table_id);

            var regex = new Regex(@"\d{8}]$");
            var fullname = TableInfo.ToFullTableName();
            if (outTablePrefixClassIfMatched && regex.IsMatch(fullname))
            {
                className = regex.Replace(className + "]", "").TrimEnd('_', ']');
            }

            return className;
        }

        public override string ToString()
        {
            return TableInfo.ToFullTableName();
        }
    }
}