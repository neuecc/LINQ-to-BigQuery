using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using LINQPad.Extensibility.DataContext;

namespace BigQuery.Linq
{
    public class DriverProperty
    {
        readonly IConnectionInfo cxInfo;
        readonly XElement driverData;

        public DriverProperty(IConnectionInfo cxInfo)
        {
            this.cxInfo = cxInfo;
            this.driverData = cxInfo.DriverData;
        }

        public string DisplayName
        {
            get
            {
                return cxInfo.DisplayName;
            }
            set
            {
                cxInfo.DisplayName = value;
            }
        }

        public string ContextJsonAuthenticationKey
        {
            get { return (string)driverData.Element(nameof(ContextJsonAuthenticationKey)); }
            set { driverData.SetElementValue(nameof(ContextJsonAuthenticationKey), value); }
        }

        public string ContextUser
        {
            get { return (string)driverData.Element(nameof(ContextUser)); }
            set { driverData.SetElementValue(nameof(ContextUser), value); }
        }

        public string ContextProjectId
        {
            get { return (string)driverData.Element(nameof(ContextProjectId)); }
            set { driverData.SetElementValue(nameof(ContextProjectId), value); }
        }

        public bool ContextIsOnlyDataSet
        {
            get { return (bool?)driverData.Element(nameof(ContextIsOnlyDataSet)) ?? false; }
            set { driverData.SetElementValue(nameof(ContextIsOnlyDataSet), value); }
        }

        public string ContextDataSet
        {
            get { return (string)driverData.Element(nameof(ContextDataSet)); }
            set { driverData.SetElementValue(nameof(ContextDataSet), value); }
        }

        public string[] NamespacesToAdd
        {
            get
            {
                var path = GetTempPathInfo("ns");
                path.Directory.Create();
                return path.Exists ? File.ReadAllText(path.FullName).Split(',') : new string[0];
            }
            set
            {
                var path = GetTempPathInfo("ns");
                path.Directory.Create();
                File.WriteAllText(path.FullName, string.Join(",", value));
            }
        }

        public string GetConnectionIdentity()
        {
            return (ContextUser + "_" + ContextProjectId).Replace("@", "_").Replace("\\", "_").Replace("/", "_");
        }

        FileInfo GetTempPathInfo(string type) => new FileInfo(Path.Combine(Path.GetTempPath(), "LINQtoBigQueryDriver", type, GetConnectionIdentity(), "." + type));

        public string GetTempCache()
        {
            var path = GetTempPathInfo("cache");
            path.Directory.Create(); // create directory if not exists
            return path.Exists ? File.ReadAllText(path.FullName) : null;
        }

        public void SetTempCache(string cache)
        {
            var path = GetTempPathInfo("cache");
            path.Directory.Create();
            File.WriteAllText(path.FullName, cache);
        }

        // unfortunately can't work
        //public bool ForceSave()
        //{
        //    cxInfo.Persist = true;
        //    var saveToDisk = cxInfo.GetType().GetMethod("SaveToDisk", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public);
        //    if (saveToDisk != null)
        //    {
        //        return (bool)saveToDisk.Invoke(cxInfo, null);
        //    }
        //    return false;
        //}
    }
}