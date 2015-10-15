using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Google.Apis.Bigquery.v2;
using LINQPad.Extensibility.DataContext;
using Newtonsoft.Json;

namespace BigQuery.Linq
{
    public class BigQueryDataContextDriver : DynamicDataContextDriver
    {
        bool tryFromCache = true;

        public BigQueryDataContextDriver()
        {
            AppDomain.CurrentDomain.AssemblyResolve += CurrentDomain_AssemblyResolve;
        }

        public override string Author
        {
            get
            {
                return "neuecc";
            }
        }

        public override string Name
        {
            get
            {
                return "LINQ to BigQuery Driver";
            }
        }

        // 1. Show Dialog, get configuration. true is continue, false is cancel.
        public override bool ShowConnectionDialog(IConnectionInfo cxInfo, bool isNewConnection)
        {
            // set to the cxInfo.DriverData
            //bool? result = new ConnectionDialog(cxInfo).ShowDialog();
            //return result == true;

            tryFromCache = false;

            return true;
        }

        // 2. Title of listitem
        public override string GetConnectionDescription(IConnectionInfo cxInfo)
        {
            return cxInfo.DisplayName;
        }

        // 3. explore item and generate assembly
        public override List<ExplorerItem> GetSchemaAndBuildAssembly(IConnectionInfo cxInfo, AssemblyName assemblyToBuild, ref string nameSpace, ref string typeName)
        {
            var property = new DriverProperty(cxInfo);

            var context = ContextHelper.GetWhiteContext(); // TODO:From DriverProperty

            SchemaBuilder schemaBuilder = null;
            if (tryFromCache)
            {
                tryFromCache = false;

                var schemaCache = property.Cache;
                if (schemaCache != null)
                {
                    try
                    {
                        var deserialized = JsonConvert.DeserializeObject<Schema[]>(schemaCache);
                        schemaBuilder = new SchemaBuilder(context, deserialized);
                    }
                    catch
                    {
                    }
                }
            }

            if (schemaBuilder == null)
            {
                schemaBuilder = SchemaBuilder.FromTableInfosAsync(context).Result;
                var cacheString = JsonConvert.SerializeObject(schemaBuilder.Schemas, Formatting.None);
                property.Cache = cacheString;
            }

            var list = schemaBuilder.BuildExplorerItems();
            namespacesToAdd = schemaBuilder.CompileTo(assemblyToBuild, nameSpace);

            return list;
        }

        // 4,5...

        string[] namespacesToAdd = new string[0];

        public override IEnumerable<string> GetNamespacesToAdd(IConnectionInfo cxInfo)
        {
            return base.GetNamespacesToAdd(cxInfo).Concat(namespacesToAdd).Distinct();
        }

        public override IEnumerable<string> GetAssembliesToAdd(IConnectionInfo cxInfo)
        {
            return base.GetAssembliesToAdd(cxInfo)
                .Concat(new[] 
                {
                    typeof(BigQueryContext).Assembly.Location,
                    typeof(BigqueryService).Assembly.Location
                })
                .Distinct();
        }

        private Assembly CurrentDomain_AssemblyResolve(object sender, ResolveEventArgs args)
        {
            // Fucking System.Net.Http.Primitives.dll
            var dllName = args.Name.Split(',')[0] + ".dll";
            return LoadAssemblySafely(Path.GetDirectoryName(typeof(Linq.BigQueryContext).Assembly.Location) + "\\" + dllName);
        }
    }
}

// checking exploreritem icons

//var list = new List<ExplorerItem>()
//{
//    new ExplorerItem("Category", ExplorerItemKind.Category, ExplorerIcon.Blank),
//    new ExplorerItem("CollectionLink - Box", ExplorerItemKind.CollectionLink, ExplorerIcon.Box),
//    new ExplorerItem("Parameter - Column", ExplorerItemKind.Parameter, ExplorerIcon.Column),
//    new ExplorerItem("Property - Inherited", ExplorerItemKind.Property, ExplorerIcon.Inherited),
//    new ExplorerItem("QueryableObject - Key", ExplorerItemKind.QueryableObject, ExplorerIcon.Key),
//    new ExplorerItem("ReferenceLink - LinkedDatabase", ExplorerItemKind.ReferenceLink, ExplorerIcon.LinkedDatabase),
//    new ExplorerItem("Schema - ManyToMany", ExplorerItemKind.Schema, ExplorerIcon.ManyToMany),
//    new ExplorerItem("ManyToOne", ExplorerItemKind.Category, ExplorerIcon.ManyToOne),
//    new ExplorerItem("OneToMany", ExplorerItemKind.Category, ExplorerIcon.OneToMany),
//    new ExplorerItem("OneToOne", ExplorerItemKind.Category, ExplorerIcon.OneToOne),
//    new ExplorerItem("Parameter", ExplorerItemKind.Category, ExplorerIcon.Parameter),
//    new ExplorerItem("ScalarFunction", ExplorerItemKind.Category, ExplorerIcon.ScalarFunction),
//    new ExplorerItem("Schema", ExplorerItemKind.Category, ExplorerIcon.Schema),
//    new ExplorerItem("StoredProc", ExplorerItemKind.Category, ExplorerIcon.StoredProc),
//    new ExplorerItem("Table", ExplorerItemKind.Category, ExplorerIcon.Table),
//    new ExplorerItem("TableFunction", ExplorerItemKind.Category, ExplorerIcon.TableFunction),
//    new ExplorerItem("View", ExplorerItemKind.Category, ExplorerIcon.View),
//};