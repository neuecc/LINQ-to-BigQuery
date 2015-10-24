using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Remoting;
using Google.Apis.Bigquery.v2;
using LINQPad.Extensibility.DataContext;
using Newtonsoft.Json;

namespace BigQuery.Linq
{
    public class BigQueryDataContextDriver : DynamicDataContextDriver
    {

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

        public BigQueryDataContextDriver()
        {
            AppDomain.CurrentDomain.AssemblyResolve += CurrentDomain_AssemblyResolve;
        }

        // 1. Show Dialog, get configuration. true is continue, false is cancel.
        public override bool ShowConnectionDialog(IConnectionInfo cxInfo, bool isNewConnection)
        {
            // set to the cxInfo.DriverData
            var window = new ConnectionDialog(cxInfo);
            bool? result = window.ShowDialog();

            if (result == true)
            {
                return true;
            }
            else
            {
                return false;
            }
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

            var context = ContextHelper.GetContext(property.ContextJsonAuthenticationKey, property.ContextUser, property.ContextProjectId);

            SchemaBuilder schemaBuilder = null;

            var useCache = false; // for debugging shortcut
            if (useCache)
            {
                var schemaCache = property.GetTempCache();
                if (schemaCache != null)
                {
                    try
                    {
                        var deserialized = JsonConvert.DeserializeObject<Schema[]>(schemaCache);
                        schemaBuilder = new SchemaBuilder(context, property, deserialized);
                    }
                    catch
                    {

                    }
                }
            }

            if (schemaBuilder == null)
            {
                schemaBuilder = SchemaBuilder.FromTableInfosAsync(context, property).GetAwaiter().GetResult();
                var cacheString = JsonConvert.SerializeObject(schemaBuilder.Schemas, Formatting.None);
                property.SetTempCache(cacheString);
            }

            BuildCodeResult[] generatedCodes;
            var namespacesToAdd = schemaBuilder.CompileTo(assemblyToBuild, nameSpace, out generatedCodes);
            var list = schemaBuilder.BuildExplorerItems(generatedCodes);

            property.NamespacesToAdd = namespacesToAdd; // write to file...
            typeName = "CustomBigQueryContext";

            return list;
        }

        // 4,5...

        public override ParameterDescriptor[] GetContextConstructorParameters(IConnectionInfo cxInfo)
        {
            // With Embedded CustomBigQueryContext
            return new[]
            {
                new ParameterDescriptor("json", typeof(string).FullName),
                new ParameterDescriptor("user", typeof(string).FullName),
                new ParameterDescriptor("projectId", typeof(string).FullName)
            };
        }

        public override object[] GetContextConstructorArguments(IConnectionInfo cxInfo)
        {
            var property = new DriverProperty(cxInfo);

            return new object[]
            {
                property.ContextJsonAuthenticationKey,
                property.ContextUser,
                property.ContextProjectId
            };
        }

        public override IEnumerable<string> GetNamespacesToAdd(IConnectionInfo cxInfo)
        {
            var asm = typeof(DynamicDataContextDriver).Assembly; // LINQPad.exe
            Version version;
            if (!Version.TryParse(asm.GetCustomAttribute<AssemblyFileVersionAttribute>()?.Version ?? "", out version))
            {
                version = new Version(1, 0, 0, 0);
            }

            var prop = new DriverProperty(cxInfo);

            return base.GetNamespacesToAdd(cxInfo)
                .Concat(prop.NamespacesToAdd)
                .Concat(new[]
                {
                    "System.Linq",
                    "BigQuery.Linq",
                    "System.Windows.Forms.DataVisualization.Charting"
                })
                .Concat(new Version(5, 0, 0, 0) <= version ? new[] { "static BigQuery.Linq.BqFunc" } : new string[0])
                .Distinct();
        }

        public override IEnumerable<string> GetAssembliesToAdd(IConnectionInfo cxInfo)
        {
            return base.GetAssembliesToAdd(cxInfo)
                .Concat(new[]
                {
                    typeof(System.Windows.Forms.Form).Assembly.Location,
                    typeof(System.Windows.Forms.DataVisualization.Charting.Chart).Assembly.Location,
                    typeof(BigQueryContext).Assembly.Location,
                    typeof(BigqueryService).Assembly.Location,
                    typeof(Google.Apis.Auth.OAuth2.GoogleWebAuthorizationBroker).Assembly.Location,
                    typeof(Google.Apis.Services.BaseClientService).Assembly.Location,
                    typeof(Google.GoogleApiException).Assembly.Location,
                    typeof(Google.Apis.Auth.JsonWebToken).Assembly.Location,
                    typeof(Google.Apis.Util.Store.FileDataStore).Assembly.Location
                })
                .Distinct();
        }

        private Assembly CurrentDomain_AssemblyResolve(object sender, ResolveEventArgs args)
        {
            // Fucking System.Net.Http.Primitives.dll
            var dllName = args.Name.Split(',')[0] + ".dll";
            var dllPath = Path.GetDirectoryName(typeof(Linq.BigQueryContext).Assembly.Location) + "\\" + dllName;
            if (File.Exists(dllPath))
            {
                return LoadAssemblySafely(Path.GetDirectoryName(typeof(Linq.BigQueryContext).Assembly.Location) + "\\" + dllName);
            }
            else
            {
                return null;
            }
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