using System;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using BigQuery.Linq;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2;
using Google.Apis.Bigquery.v2.Data;
using Google.Apis.Services;
using Google.Apis.Util.Store;
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
        readonly DriverProperty property;


        public SchemaBuilder(BigQueryContext context, DriverProperty property, Schema[] schemas)
        {
            this.context = context;
            this.Schemas = schemas;
            this.property = property;
        }

        public static async Task<SchemaBuilder> FromTableInfosAsync(BigQueryContext context, DriverProperty property)
        {
            string[] datasets;
            if (property.ContextIsOnlyDataSet)
            {
                datasets = new[] { property.ContextDataSet };
            }
            else
            {
                datasets = await context.GetAllDatasetsAsync();
            }

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

            return new SchemaBuilder(context, property, list.ToArray());
        }

        // return namespaces
        public string[] CompileTo(AssemblyName assemblyToBuild, string namespaceName, out BuildCodeResult[] generatedBuildCodes)
        {
            var namespaces = new List<string>();
            var generatedCodes = new List<BuildCodeResult>();

            var code = new StringBuilder();
            code.AppendLine("using System;");
            code.AppendLine("using System.IO;");
            code.AppendLine("using System.Text;");
            code.AppendLine("using System.Threading;");
            code.AppendLine("using Google.Apis.Auth.OAuth2;");
            code.AppendLine("using Google.Apis.Bigquery.v2;");
            code.AppendLine("using Google.Apis.Services;");
            code.AppendLine("using Google.Apis.Util.Store;");
            code.AppendLine("using BigQuery.Linq;");

            code.Append(BuildCustomContext(namespaceName, property.ContextJsonAuthenticationKey, property.ContextUser, property.ContextProjectId));


            foreach (var schema in Schemas.Where(x => x.GroupedMetaTableSchemas.Any()))
            {
                var namingStoragePerDataset = new DuplicateNamingStorage();

                var tableCodes = schema.GroupedMetaTableSchemas
                     .SelectMany(x =>
                     {
                         var concat = Enumerable.Empty<BuildCodeResult[]>();
                         if (x.IsGrouped)
                         {
                             var f = x.MetaTableSchemas.First();
                             concat = new[] { f.BuildCSharpClasses(outTablePrefixClassIfMatched: true, namingStorage: namingStoragePerDataset) };
                         }

                         return concat.Concat(x.MetaTableSchemas.Select(y => y.BuildCSharpClasses(outTablePrefixClassIfMatched: false, namingStorage: namingStoragePerDataset)));
                     })
                     .SelectMany(xs => xs)
                     .Distinct(x => x.ClassName)
                     .ToArray();

                var typeCode = string.Join(Environment.NewLine + Environment.NewLine, tableCodes.Select(x => x.Code));

                var template = $@"
namespace {namespaceName}.@{schema.DatasetName.Replace("-", "_").Replace(":", "_")}
{{
{typeCode}
}}";

                code.Append(template);
                namespaces.Add($"{namespaceName}.@{schema.DatasetName.Replace("-", "_").Replace(":", "_")}");
                generatedCodes.AddRange(tableCodes);
            }

            CompilerResults results;
            using (var codeProvider = new CSharpCodeProvider(new Dictionary<string, string>() { { "CompilerVersion", "v4.0" } }))
            {
                var options = new CompilerParameters(
                    new[]
                    {
                        "mscorlib.dll", "System.dll", "System.Core.dll", "System.Xml.dll",
                        typeof(BigQueryContext).Assembly.Location,
                        typeof(BigqueryService).Assembly.Location,
                        typeof(GoogleWebAuthorizationBroker).Assembly.Location,
                        typeof(BaseClientService).Assembly.Location,
                        typeof(Google.GoogleApiException).Assembly.Location,
                        typeof(Google.Apis.Auth.JsonWebToken).Assembly.Location,
                        typeof(FileDataStore).Assembly.Location
                    }.Distinct().ToArray(),
                    assemblyToBuild.CodeBase,
                    true);
                results = codeProvider.CompileAssemblyFromSource(options, code.ToString());
            }
            
            if (results.Errors.Count > 0)
            {
                throw new Exception
                    ("Cannot compile typed context: " + results.Errors[0].ErrorText + " (line " + results.Errors[0].Line + ")");
            }

            generatedBuildCodes = generatedCodes.ToArray();
            return namespaces.ToArray();
        }

        public string BuildCustomContext(string namespaceName, string json, string user, string projectId)
        {
            json = json.Replace("\"", "\"\"");

            var template = $@"
namespace {namespaceName}
{{
    public class CustomBigQueryContext : BigQueryContext
    {{
        public CustomBigQueryContext()
        {{
            var json = @""{json}"";
            var user = ""{user}"";
            var projectId = ""{projectId}"";

            using (var ms = new MemoryStream(Encoding.UTF8.GetBytes(json)))
            {{
                var userCredential = GoogleWebAuthorizationBroker.AuthorizeAsync(ms,
                    new[] {{ BigqueryService.Scope.Bigquery }},
                    user,
                    CancellationToken.None, new FileDataStore(""LINQ-to-BigQuery-for-"" + projectId))
                    .GetAwaiter().GetResult();

                var bigquery = new BigqueryService(new BaseClientService.Initializer
                {{
                    ApplicationName = ""LINQ to BigQuery"",
                    HttpClientInitializer = userCredential
                }});

                this.BigQueryService = bigquery;
                this.ProjectId = projectId;
            }}

            this.TimeoutMs = (long)TimeSpan.FromMinutes(3).TotalMilliseconds;
        }}
    }}
}}
";

            return template;
        }


        public List<ExplorerItem> BuildExplorerItems(BuildCodeResult[] generatedCodes)
        {
            var lookupDictionary = generatedCodes
                .Where(x => x.IsTable)
                .Distinct(x => x.MetaTableSchema)
                .ToDictionary(x => x.MetaTableSchema);

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

                var tableRanges = new ExplorerItem("Ranges", ExplorerItemKind.Category, ExplorerIcon.Schema)
                {
                    Children = lookup[3].Select(g =>
                    {
                        var x = g.MetaTableSchemas.First();

                        var groupingChild = new ExplorerItem($"Tables ({g.MetaTableSchemas.Length})", ExplorerItemKind.Category, ExplorerIcon.Table)
                        {
                            Children = g.MetaTableSchemas.Select(y =>
                            {
                                // TODO:Here is buggy, sometimes fetch range table
                                var className = lookupDictionary.GetOrDefault(y)?.ClassName ?? y.ToClassName(false);

                                return new ExplorerItem(y.TableInfo.table_id, ExplorerItemKind.QueryableObject, ExplorerIcon.Table)
                                {
                                    DragText = $"From<{className}>()",
                                    ToolTipText = y.TableInfo.ToFullTableName(),
                                };
                            }).ToList()
                        };

                        var propertyList = new List<ExplorerItem> { groupingChild };
                        propertyList.AddRange(BuildColumnExplorerItems(x.Fields));

                        var classNameCheck = lookupDictionary.GetOrDefault(x)?.ClassName ?? x.ToClassName(true);

                        var item = new ExplorerItem(g.ShortTablePrefix, ExplorerItemKind.QueryableObject, ExplorerIcon.Schema)
                        {
                            DragText = $"FromDateRange<{classNameCheck}>()",
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
                        var className = lookupDictionary.GetOrDefault(x)?.ClassName ?? x.ToClassName(false);

                        var item = new ExplorerItem(x.TableInfo.table_id, ExplorerItemKind.QueryableObject, ExplorerIcon.View)
                        {
                            DragText = $"From<{className}>()",
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
                        var className = lookupDictionary.GetOrDefault(x)?.ClassName ?? x.ToClassName(false);

                        var item = new ExplorerItem(x.TableInfo.table_id, ExplorerItemKind.QueryableObject, ExplorerIcon.Table)
                        {
                            DragText = $"From<{className}>()",
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

                var item = new ExplorerItem($"{x.Name} ({x.Type}{suffix})", ExplorerItemKind.Property, ExplorerIcon.Column)
                {
                    ToolTipText = x.Description
                };
                return item;
            })
            .ToList();
        }
    }
}