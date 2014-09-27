using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Tests
{
    [TableName("[fh-bigquery:geocode.geolite_city_bq_b2b]")]
    public class geolite_city_bq_b2b
    {
        public long? startIpNum { get; set; }
        public long? endIpNum { get; set; }
        public long? locId { get; set; }
        public string country { get; set; }
        public string region { get; set; }
        public string city { get; set; }
        public string postalCode { get; set; }
        public string latitude { get; set; }
        public string longitude { get; set; }
        public string metroCode { get; set; }
        public string areaCode { get; set; }
        public long? classA { get; set; }
        public string startIp { get; set; }
        public string endIp { get; set; }
        public string countryLabel { get; set; }
        public long? classB { get; set; }
    }
}
