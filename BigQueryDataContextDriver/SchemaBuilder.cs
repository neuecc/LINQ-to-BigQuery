using System;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using BigQuery.Linq;
using LINQPad.Extensibility.DataContext;
using Microsoft.CSharp;

namespace BigQuery.Linq
{
    // TODO:Need to serializable
    public class Schema
    {
        public string DatasetName { get; set; }
        public MetaTableSchema[] MetaTableSchemas { get; set; }
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
                var tables = await context.GetCollectedTableSchemasAsync(dataset);

                var schema = new Schema()
                {
                    DatasetName = dataset,
                    MetaTableSchemas = tables,
                };

                list.Add(schema);
            }

            return new SchemaBuilder(context, list.ToArray());
        }

        public void CompileTo(AssemblyName assemblyToBuild, string namespaceName)
        {
            var typeCode = string.Join(Environment.NewLine + Environment.NewLine, Schemas.SelectMany(x => x.MetaTableSchemas).Select(x => x.BuildCSharpClass(outTablePrefixClassIfMatched: true)));

            var code = $@"
{namespaceName}
{{
    {typeCode}
}}";

            CompilerResults results;
            using (var codeProvider = new CSharpCodeProvider(new Dictionary<string, string>() { { "CompilerVersion", "v3.5" } }))
            {
                var options = new CompilerParameters("System.dll System.Core.dll System.Xml.dll ".Split(),
                    assemblyToBuild.CodeBase,
                    true);
                results = codeProvider.CompileAssemblyFromSource(options, code);
            }
            if (results.Errors.Count > 0)
            {
                throw new Exception
                    ("Cannot compile typed context: " + results.Errors[0].ErrorText + " (line " + results.Errors[0].Line + ")");
            }
        }

        public List<ExplorerItem> BuildExplorerItems()
        {
            var matchTableRange = new Regex(@"\d{8}]$");

            var list = new List<ExplorerItem>();

            foreach (var dataset in this.Schemas)
            {
                var root = new ExplorerItem(dataset.DatasetName, ExplorerItemKind.Category, ExplorerIcon.Box);

                // regularTable = 1, view = 2, tableRange = 3
                var lookup = dataset.MetaTableSchemas.ToLookup(x =>
                {
                    if (x.TableInfo.type == 1 && matchTableRange.IsMatch(x.TableInfo.table_id))
                    {
                        return 3;
                    }
                    return x.TableInfo.type;
                });





                // root.Children = new List<ExplorerItem>


                list.Add(root);
            }

            return list;
        }
    }
}

// ExplorerItem Sample

//var list = new List<ExplorerItem>()
//{
//    new ExplorerItem("access", ExplorerItemKind.Category, ExplorerIcon.Box)
//    {
//        Children = new List<ExplorerItem>
//        {
//            new ExplorerItem("TableRanges", ExplorerItemKind.Category, ExplorerIcon.Schema)
//            {
//                Children = new List<ExplorerItem>
//                {
//                    new ExplorerItem("Login_", ExplorerItemKind.QueryableObject, ExplorerIcon.Schema)
//                    {
//                        Children = new List<ExplorerItem>
//                        {
//                            new ExplorerItem("timestamp (TIMESTAMP)", ExplorerItemKind.Property, ExplorerIcon.Column),
//                            new ExplorerItem("sourceHost (STRING)", ExplorerItemKind.Property, ExplorerIcon.Column),
//                            new ExplorerItem("gitRevision (STRING)", ExplorerItemKind.Property, ExplorerIcon.Column),
//                        },
//                        IsEnumerable = true
//                    }
//                }
//            },
//            new ExplorerItem("Views", ExplorerItemKind.Category, ExplorerIcon.View)
//            {
//                Children = new List<ExplorerItem>
//                {
//                    new ExplorerItem("Login_Current", ExplorerItemKind.QueryableObject, ExplorerIcon.View)
//                    {
//                        Children = new List<ExplorerItem>
//                        {
//                            new ExplorerItem("timestamp (TIMESTAMP)", ExplorerItemKind.Property, ExplorerIcon.Column),
//                            new ExplorerItem("sourceHost (STRING)", ExplorerItemKind.Property, ExplorerIcon.Column),
//                            new ExplorerItem("gitRevision (STRING)", ExplorerItemKind.Property, ExplorerIcon.Column),
//                        },
//                        DragText = "hogemoge!!!"
//                    }
//                }
//            },
//            new ExplorerItem("Tables", ExplorerItemKind.Category, ExplorerIcon.Table)
//            {
//                Children = new List<ExplorerItem>
//                {
//                    new ExplorerItem("Login_20151014", ExplorerItemKind.QueryableObject, ExplorerIcon.Table)
//                    {
//                        Children = new List<ExplorerItem>
//                        {
//                            new ExplorerItem("timestamp (TIMESTAMP)", ExplorerItemKind.Property, ExplorerIcon.Column),
//                            new ExplorerItem("sourceHost (STRING)", ExplorerItemKind.Property, ExplorerIcon.Column),
//                            new ExplorerItem("gitRevision (STRING)", ExplorerItemKind.Property, ExplorerIcon.Column),
//                        }
//                    }
//                }
//            },
//        }
//    }
//};
