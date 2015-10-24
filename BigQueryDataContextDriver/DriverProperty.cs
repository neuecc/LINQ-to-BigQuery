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

        void WithEncrypt(string elementName, string value)
        {
            driverData.SetElementValue(elementName, cxInfo.Encrypt(value));
        }

        string WithDecrypt(string elementName)
        {
            var v = (string)driverData.Element(elementName);
            if (v == null) return null;
            return cxInfo.Decrypt(v);
        }

        public string ContextJsonAuthenticationKey
        {
            get { return WithDecrypt(nameof(ContextJsonAuthenticationKey)); }
            set { WithEncrypt(nameof(ContextJsonAuthenticationKey), value); }
        }

        public string ContextUser
        {
            get { return WithDecrypt(nameof(ContextUser)); }
            set { WithEncrypt(nameof(ContextUser), value); }
        }

        public string ContextProjectId
        {
            get { return WithDecrypt(nameof(ContextProjectId)); }
            set { WithEncrypt(nameof(ContextProjectId), value); }
        }

        public bool ContextIsOnlyDataSet
        {
            get { return (bool?)driverData.Element(nameof(ContextIsOnlyDataSet)) ?? false; }
            set { driverData.SetElementValue(nameof(ContextIsOnlyDataSet), value); }
        }

        public string ContextDataSet
        {
            get { return WithDecrypt(nameof(ContextDataSet)); }
            set { WithEncrypt(nameof(ContextDataSet), value); }
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
            return (ContextUser + "_" + ContextProjectId + "_" + ContextIsOnlyDataSet + "_" + (ContextDataSet ?? ""))
                .Replace("@", "_")
                .Replace("\\", "_")
                .Replace("/", "_")
                .Replace(":", "_");
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

        // unfortunately doesn't work
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