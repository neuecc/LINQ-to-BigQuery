using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Tests
{
    [TableName("[publicdata:samples.gsod]")]
    public class gsod
    {
        public long station_number { get; set; }
        public long? wban_number { get; set; }
        public long year { get; set; }
        public long month { get; set; }
        public long day { get; set; }
        public double? mean_temp { get; set; }
        public long? num_mean_temp_samples { get; set; }
        public double? mean_dew_point { get; set; }
        public long? num_mean_dew_point_samples { get; set; }
        public double? mean_sealevel_pressure { get; set; }
        public long? num_mean_sealevel_pressure_samples { get; set; }
        public double? mean_station_pressure { get; set; }
        public long? num_mean_station_pressure_samples { get; set; }
        public double? mean_visibility { get; set; }
        public long? num_mean_visibility_samples { get; set; }
        public double? mean_wind_speed { get; set; }
        public long? num_mean_wind_speed_samples { get; set; }
        public double? max_sustained_wind_speed { get; set; }
        public double? max_gust_wind_speed { get; set; }
        public double? max_temperature { get; set; }
        public bool? max_temperature_explicit { get; set; }
        public double? min_temperature { get; set; }
        public bool? min_temperature_explicit { get; set; }
        public double? total_precipitation { get; set; }
        public double? snow_depth { get; set; }
        public bool? fog { get; set; }
        public bool? rain { get; set; }
        public bool? snow { get; set; }
        public bool? hail { get; set; }
        public bool? thunder { get; set; }
        public bool? tornado { get; set; }
    }
}
