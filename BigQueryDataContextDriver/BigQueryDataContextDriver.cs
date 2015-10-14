using System;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BigQuery.Linq;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2;
using Google.Apis.Services;
using Google.Apis.Util.Store;
using LINQPad.Extensibility.DataContext;
using Microsoft.CSharp;

namespace ClassLibrary2
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

        // 1. Show Dialog, get configuration. true is continue, false is cancel.
        public override bool ShowConnectionDialog(IConnectionInfo cxInfo, bool isNewConnection)
        {
            // set to the cxInfo.DriverData
            //bool? result = new ConnectionDialog(cxInfo).ShowDialog();
            //return result == true;

            return true;
        }

        // 2. Title of listitem
        public override string GetConnectionDescription(IConnectionInfo cxInfo)
        {
            return "BigQueryContext";
        }

        // 3. explore item and generate assembly
        public override List<ExplorerItem> GetSchemaAndBuildAssembly(IConnectionInfo cxInfo, AssemblyName assemblyToBuild, ref string nameSpace, ref string typeName)
        {
            var list = new List<ExplorerItem>()
            {
                new ExplorerItem("access", ExplorerItemKind.Category, ExplorerIcon.Box)
                {
                    Children = new List<ExplorerItem>
                    {
                        new ExplorerItem("TableRanges", ExplorerItemKind.Category, ExplorerIcon.Schema)
                        {
                            Children = new List<ExplorerItem>
                            {
                                new ExplorerItem("Login_", ExplorerItemKind.QueryableObject, ExplorerIcon.Schema)
                                {
                                    Children = new List<ExplorerItem>
                                    {
                                        new ExplorerItem("timestamp (TIMESTAMP)", ExplorerItemKind.Property, ExplorerIcon.Column),
                                        new ExplorerItem("sourceHost (STRING)", ExplorerItemKind.Property, ExplorerIcon.Column),
                                        new ExplorerItem("gitRevision (STRING)", ExplorerItemKind.Property, ExplorerIcon.Column),
                                    },
                                    IsEnumerable = true
                                }
                            }
                        },
                        new ExplorerItem("Views", ExplorerItemKind.Category, ExplorerIcon.View)
                        {
                            Children = new List<ExplorerItem>
                            {
                                new ExplorerItem("Login_Current", ExplorerItemKind.QueryableObject, ExplorerIcon.View)
                                {
                                    Children = new List<ExplorerItem>
                                    {
                                        new ExplorerItem("timestamp (TIMESTAMP)", ExplorerItemKind.Property, ExplorerIcon.Column),
                                        new ExplorerItem("sourceHost (STRING)", ExplorerItemKind.Property, ExplorerIcon.Column),
                                        new ExplorerItem("gitRevision (STRING)", ExplorerItemKind.Property, ExplorerIcon.Column),
                                    },
                                    DragText = "hogemoge!!!"
                                }
                            }
                        },
                        new ExplorerItem("Tables", ExplorerItemKind.Category, ExplorerIcon.Table)
                        {
                            Children = new List<ExplorerItem>
                            {
                                new ExplorerItem("Login_20151014", ExplorerItemKind.QueryableObject, ExplorerIcon.Table)
                                {
                                    Children = new List<ExplorerItem>
                                    {
                                        new ExplorerItem("timestamp (TIMESTAMP)", ExplorerItemKind.Property, ExplorerIcon.Column),
                                        new ExplorerItem("sourceHost (STRING)", ExplorerItemKind.Property, ExplorerIcon.Column),
                                        new ExplorerItem("gitRevision (STRING)", ExplorerItemKind.Property, ExplorerIcon.Column),
                                    }
                                }
                            }
                        },
                    }
                }
            };

            // Test Compile
            CompilerResults results;
            using (var codeProvider = new CSharpCodeProvider(new Dictionary<string, string>() { { "CompilerVersion", "v3.5" } }))
            {
                var options = new CompilerParameters("System.dll System.Core.dll System.Xml.dll ".Split(),
                    assemblyToBuild.CodeBase,
                    true);
                results = codeProvider.CompileAssemblyFromSource(options, $"namespace {nameSpace} {{ public class Hoge {{ public int Huga {{ get; set; }} }} }}");
            }
            if (results.Errors.Count > 0)
            {
                throw new Exception
                    ("Cannot compile typed context: " + results.Errors[0].ErrorText + " (line " + results.Errors[0].Line + ")");
            }


            typeName = "Hoge";




            return list;
        }


        // others to override...



        public override void InitializeContext(IConnectionInfo cxInfo, object context, QueryExecutionManager executionManager)
        {
            base.InitializeContext(cxInfo, context, executionManager);
        }

        public override void TearDownContext(IConnectionInfo cxInfo, object context, QueryExecutionManager executionManager, object[] constructorArguments)
        {
            base.TearDownContext(cxInfo, context, executionManager, constructorArguments);
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
