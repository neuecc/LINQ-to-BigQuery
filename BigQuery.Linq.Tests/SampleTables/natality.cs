using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq.Tests
{
    [TableName("[publicdata:samples.natality]")]
    public class natality
    {
        public long source_year { get; set; }
        public long? year { get; set; }
        public long? month { get; set; }
        public long? day { get; set; }
        public long? wday { get; set; }
        public string state { get; set; }
        public bool is_male { get; set; }
        public long? child_race { get; set; }
        public double? weight_pounds { get; set; }
        public long? plurality { get; set; }
        public long? apgar_1min { get; set; }
        public long? apgar_5min { get; set; }
        public string mother_residence_state { get; set; }
        public long? mother_race { get; set; }
        public long? mother_age { get; set; }
        public long? gestation_weeks { get; set; }
        public string lmp { get; set; }
        public bool? mother_married { get; set; }
        public string mother_birth_state { get; set; }
        public bool? cigarette_use { get; set; }
        public long? cigarettes_per_day { get; set; }
        public bool? alcohol_use { get; set; }
        public long? drinks_per_week { get; set; }
        public long? weight_gain_pounds { get; set; }
        public long? born_alive_alive { get; set; }
        public long? born_alive_dead { get; set; }
        public long? born_dead { get; set; }
        public long? ever_born { get; set; }
        public long? father_race { get; set; }
        public long? father_age { get; set; }
        public long? record_weight { get; set; }
    }
}