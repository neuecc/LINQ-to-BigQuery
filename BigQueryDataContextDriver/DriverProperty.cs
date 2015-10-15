using System;
using System.Collections.Generic;
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

        public string Cache
        {
            get
            {
                return (string)driverData.Element(nameof(Cache));
            }
            set
            {
                driverData.SetElementValue(nameof(Cache), value);
            }
        }
    }
}
