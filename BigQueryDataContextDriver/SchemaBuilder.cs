using System;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using BigQuery.Linq;
using Google.Apis.Bigquery.v2.Data;
using LINQPad.Extensibility.DataContext;
using Microsoft.CSharp;

namespace BigQuery.Linq
{
    public class Schema
    {
        public string DatasetName { get; set; }
        public GroupedMetaTableSchema[] GroupedMetaTableSchemas { get; set; }
    }

    public class SchemaBuilder
    {
        public Schema[] Schemas { get; private set; }
        readonly BigQueryContext context;


        public SchemaBuilder(BigQueryContext context, Schema[] schemas)
        {
            this.context = context;
            this.Schemas = schemas;
        }

        public static async Task<SchemaBuilder> FromTableInfosAsync(BigQueryContext context)
        {
            var datasets = await context.GetAllDatasetsAsync();

            var list = new List<Schema>();
            foreach (var dataset in datasets)
            {
                var tables = await context.GetFastTableSchemasAsync(dataset);

                var schema = new Schema()
                {
                    DatasetName = dataset,
                    GroupedMetaTableSchemas = tables,
                };

                list.Add(schema);
            }

            return new SchemaBuilder(context, list.ToArray());
        }

        // return namespaces
        public string[] CompileTo(AssemblyName assemblyToBuild, string namespaceName)
        {
            var namespaces = new List<string>();

            var code = new StringBuilder();
            code.AppendLine("using System;");
            code.AppendLine("using BigQuery.Linq;");

            foreach (var schema in Schemas)
            {
                var tableCodes = schema.GroupedMetaTableSchemas
                     .SelectMany(x =>
                     {
                         var concat = Enumerable.Empty<string>();
                         if (x.IsGrouped)
                         {
                             concat = new[] { x.MetaTableSchemas.First().BuildCSharpClass(outTablePrefixClassIfMatched: true) };
                         }

                         return concat.Concat(x.MetaTableSchemas.Select(y => y.BuildCSharpClass(outTablePrefixClassIfMatched: false)));
                     });


                var typeCode = string.Join(Environment.NewLine + Environment.NewLine, tableCodes);

                var template = $@"
namespace {namespaceName}.@{schema.DatasetName.Replace("-", "_")}
{{
{typeCode}
}}";

                code.Append(template);
                namespaces.Add($"{namespaceName}.@{schema.DatasetName.Replace("-", "_")}");
            }

            CompilerResults results;
            using (var codeProvider = new CSharpCodeProvider(new Dictionary<string, string>() { { "CompilerVersion", "v3.5" } }))
            {
                var options = new CompilerParameters(
                    new[]
                    {
                        "System.dll", "System.Core.dll", "System.Xml.dll",
                        typeof(BigQueryContext).Assembly.Location
                    },
                    assemblyToBuild.CodeBase,
                    true);
                results = codeProvider.CompileAssemblyFromSource(options, code.ToString());
            }
            if (results.Errors.Count > 0)
            {
                throw new Exception
                    ("Cannot compile typed context: " + results.Errors[0].ErrorText + " (line " + results.Errors[0].Line + ")");
            }

            return namespaces.ToArray();
        }

        public List<ExplorerItem> BuildExplorerItems()
        {
            var matchTableRange = new Regex(@"\d{8}$");

            var list = new List<ExplorerItem>();

            foreach (var dataset in this.Schemas)
            {
                var root = new ExplorerItem(dataset.DatasetName, ExplorerItemKind.Category, ExplorerIcon.Box);

                // regularTable = 1, view = 2, tableRange = 3
                var lookup = dataset.GroupedMetaTableSchemas.ToLookup(x =>
                {
                    return (x.IsGrouped)
                        ? 3
                        : x.MetaTableSchemas.First().TableInfo.type;
                });

                // View/Table = this.From<T>()
                // Range = this.FromDateRange<T>()

                var tableRanges = new ExplorerItem("TableRanges", ExplorerItemKind.Category, ExplorerIcon.Schema)
                {
                    Children = lookup[3].Select(g =>
                    {
                        var x = g.MetaTableSchemas.First();

                        var groupingChild = new ExplorerItem($"Tables ({g.MetaTableSchemas.Length})", ExplorerItemKind.Category, ExplorerIcon.Table)
                        {
                            Children = g.MetaTableSchemas.Select(y => new ExplorerItem(y.TableInfo.table_id, ExplorerItemKind.QueryableObject, ExplorerIcon.Table)
                            {
                                DragText = $"From<{y.TableInfo.table_id}>()",
                                ToolTipText = y.TableInfo.ToFullTableName(),
                            }).ToList()
                        };

                        var propertyList = new List<ExplorerItem> { groupingChild };
                        propertyList.AddRange(BuildColumnExplorerItems(x.Fields));

                        var item = new ExplorerItem(g.ShortTablePrefix, ExplorerItemKind.QueryableObject, ExplorerIcon.Schema)
                        {
                            DragText = $"From<{g.TablePrefix}>()", // needs to change range prefix
                            ToolTipText = x.TableInfo.ToFullTableName(),
                            Children = propertyList
                        };
                        return item;
                    }).ToList()
                };

                var views = new ExplorerItem("Views", ExplorerItemKind.Category, ExplorerIcon.View)
                {
                    Children = lookup[2].Select(g =>
                    {
                        var x = g.MetaTableSchemas.First();
                        var item = new ExplorerItem(x.TableInfo.table_id, ExplorerItemKind.QueryableObject, ExplorerIcon.View)
                        {
                            DragText = $"From<{x.TableInfo.table_id}>()",
                            ToolTipText = x.TableInfo.ToFullTableName(),
                            Children = BuildColumnExplorerItems(x.Fields)
                        };
                        return item;
                    }).ToList()
                };

                var tables = new ExplorerItem("Tables", ExplorerItemKind.Category, ExplorerIcon.Table)
                {
                    Children = lookup[1].Select(g =>
                    {
                        var x = g.MetaTableSchemas.First();
                        var item = new ExplorerItem(x.TableInfo.table_id, ExplorerItemKind.QueryableObject, ExplorerIcon.Table)
                        {
                            DragText = $"From<{x.TableInfo.table_id}>()",
                            ToolTipText = x.TableInfo.ToFullTableName(),
                            Children = BuildColumnExplorerItems(x.Fields)
                        };
                        return item;
                    }).ToList()
                };

                // RangeTables

                root.Children = new List<ExplorerItem> { tableRanges, views, tables };
                list.Add(root);
            }

            return list;
        }

        List<ExplorerItem> BuildColumnExplorerItems(IList<TableFieldSchema> fields)
        {
            return fields.Select(x =>
            {
                var isNullable = x.Mode == "NULLABLE";
                var isArray = x.Mode == "REPEATED";
                var suffix = isNullable ? "?"
                    : isArray ? "[]"
                    : "";

                var item = new ExplorerItem($"{x.Name}{suffix} ({x.Type})", ExplorerItemKind.Property, ExplorerIcon.Column)
                {
                    ToolTipText = x.Description
                };
                return item;
            })
            .ToList();
        }
    }
}