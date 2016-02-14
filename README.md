LINQ to BigQuery [![build status](https://ci.appveyor.com/api/projects/status/github/neuecc/linq-to-bigquery)](https://ci.appveyor.com/project/neuecc/linq-to-bigquery)
================
LINQ to BigQuery is C# LINQ Provider for [Google BigQuery](https://cloud.google.com/bigquery/). It also enables Desktop GUI Client with [LINQPad](http://www.linqpad.net) and plug-in driver.

Installation
---
Binary from NuGet, [LINQ-to-BigQuery](https://nuget.org/packages/LINQ-to-BigQuery)

```
PM> Install-Package LINQ-to-BigQuery
```

LINQPad Driver can install from LINQ Pad driver gallery.    
"Add connection" -> "View more drivers..." -> clcik "LINQ to BigQuery" - "Download & Enabled driver" link.    
Note:Configuration's JSON is OAuth2 JSON Generate from GCP Management Page.

LastUpdate, ver 0.7.2(2016-02-15). See [All Release Notes](https://github.com/neuecc/LINQ-to-BigQuery/releases)

LINQ with LINQPad
---
[LINQPad](http://www.linqpad.net) is Desktop GUI Client for BigQuery!

![bq_linqpad_first](https://cloud.githubusercontent.com/assets/46207/10714939/78e5bcb8-7b47-11e5-9262-c802040011f8.png)

* Schema tree window in side explorer
* "this" as BigQueryContext with authenticated connection
* load dependented assembly and namespaces automatically(such as "BigQuery.Linq" namespace)
* generate/load class of schema dynamically
* add utility dump for BigQuery(DumpQuery/DumpRun/DumpRunToArray/DumpChart/DumpGroupChart//DumpToExcel)
* of course you can save/load query in file

Result tab(Rows is expandable to grid view)

![bq_linqpad_results](https://cloud.githubusercontent.com/assets/46207/10714349/946fcc84-7b2f-11e5-8a0c-567da3522751.png)

Legendary Dump to Excel, perfect integration to Excel:)

![legendary_dump_to_excel](https://cloud.githubusercontent.com/assets/46207/10721962/968632b4-7bee-11e5-9b48-904fcb48e75f.gif)

Why LINQ?
---
Strongly Typed with IntelliSense(and function documents).

![bq_intellisense](https://cloud.githubusercontent.com/assets/46207/4381901/acc1b576-437d-11e4-872c-5df1cafbae13.jpg)

![bq_intellisense_func](https://cloud.githubusercontent.com/assets/46207/4381902/acc2f72e-437d-11e4-8e3b-a79147b6f044.jpg)

All BigQuery functions is under BqFunc(except string.Contains). If you use C# 6.0(or LINQPad 5) you can enable static using.

```csharp
// C# 6.0 Using Static
using static BigQuery.Linq.BqFunc;
```

LINQ to BigQuery is LINQ but is not IQueryable. It's constraint is old good method chain. The rule is

```
From((+TableDecorate)+Flatten) -> Join -> Where -| -> OrderBy(ThenBy) -> Select ->                     | -> Limit -> IgnoreCase
                                                 | -> Select | -> GroupBy -> Having -> OrderBy(ThenBy) | -> IgnoreCase
                                                             | -> OrderBy(ThenBy) ->                   |
```
After Select, you can call...

```
Run/RunAsync - Execute query with QueryResponse. 
ToArray/ToArrayAsync - Execute query and return rows.
AsEnumerable - Execute query and return rows, it's deferred(but resultset is not streaming).
RunDry - Dry run it's receive execution plan.
ToString - Build query string. It no needs network connection and BigqueryService. 
Into - Query as Subquery(same as From(query))
```

Powerfull Tabledecorator/Table wildcard integration.

```csharp
// Table Decorators - WithRange(relative or absolute), WithSnapshot 

// FROM [githubarchive:github.timeline@-900000-]
.From<github_timeline>().WithRange(TimeSpan.FromMinutes(15))

// FROM [githubarchive:github.timeline@1411398000000000]
.From<github_timeline>().WithSnapshot(DateTimeOffset.Parse("2014-09-23"))

// Table wildcard functions - FromDateRange, FromDateRangeStrict, FromTableQuery

// FROM (TABLE_DATE_RANGE([mydata], TIMESTAMP('2013-11-10'), TIMESTAMP('2013-12-01')))
.FromDateRange<mydata>("mydata", DateTimeOffset.Parse("2013-11-10"), DateTimeOffset.Parse("2013-12-1"))

// FROM (TABLE_QUERY([mydata], "([table_id] CONTAINS 'oo' AND (LENGTH([table_id]) >= 4))"))
.FromTableQuery<mydata>("mydata", x => x.table_id.Contains("oo") && BqFunc.Length(x.table_id) >= 4)

// FROM (TABLE_QUERY([mydata], "REGEXP_MATCH([table_id], r'^boo[\d]{3,5}')"))
.FromTableQuery<mydata>("mydata", x => BqFunc.RegexpMatch(x.table_id, "^boo[\\d]{3,5}"))
```

Timezone convention
---
LINQ to BigQuery converts local time to UTC automatically for BigQuery's query string. Result set of BigQuery converts UTC to local time automatically, too. If you want to use UTC, configure `BigQueryContext.IsConvertResultUtcToLocalTime`. 

```csharp
var context = new BigQueryContext();
context.IsConvertResultUtcToLocalTime = false; // default is true
```

My thought of LINQ
---
LINQ to BigQuery is LINQ but is not IQueryable. It's my answer of LINQ.

IQueryable isn't necessary for LINQ to translate SQL. I need all BigQuery's sql can write LINQ to BigQuery, all specialized sql can execute, all function can execute(include window function). [BigQuery's SQL](https://developers.google.com/bigquery/query-reference) different with standard Sql. I must support Multiple FROM, WITHIN, JOIN EACH, GROUP EACH BY, FLATTEN, IGNORECASE, etc) and LINQ to BigQuery is done.
 
Therefore control order, protect runtime error, satisfy all syntax. "IQueryable is all database's abastraction" is fantasy, LINQ needs specialized each Database. But LINQ to Xxx needs LINQ's atmosphere, filter as Where, projection as Select, order as OrderBy/ThenBy, like convention over configuration.
 
I think "IQueryable is dead, Long live Expression". Of course Expression Tree has performance issue, but execute BigQuery is very expensive so LINQ to BigQuery can ignore cost of Experssion relatively.

Basics
---
```
// create BigQueryContext with BigqueryService
var context = new BigQueryContext(/* BigqueryService, "projectId" */);

// From -> write Queries -> Run
var result = context.From<github_timeline>()
   .Select(x => x.repository_url)
   .Run();
```

tableName is provided from class definition's TableNameAttribute.

```csharp
[TableName("[publicdata:samples.github_timeline]")]
public class github_timeline
{
    public string repository_url { get; set; }
    public bool? repository_has_downloads { get; set; }
    public string repository_created_at { get; set; }
    public bool? repository_has_issues { get; set; }
    // snip...(200 lines)
	public string url { get; set; }
	public string type { get; set; }
}
```

If table separated in YYYYMMDD suffix(for FromDateRange), you can use TablePrefixAttribute's table and `BigQueryContext.BuildCSharpClass` supports generate class.

```csharp
[TablePrefix("[mytable.LoginInfo]")]
public class Login
{
    public DateTimeOffset timestamp { get; set; }
    public string url { get; set; }
    public string userAgent { get; set; }
}

// Generate all tableinfo and if table's suffix is YYYYMMDD, generates [TablePrefix] table.
var codes = context.BuildCSharpClass("mydataset");
```

TablePrefix's class is used by `FromDateRange`, `FromDateRangeStrict`.

```csharp 
// TABLE_DATE_RANGE([mytable.LoginInfo], TIMESTAMP('2015-03-03'), TIMESTAMP('2015-03-09'))
context.FromDateRange<LoginInfo>(new DateTime(2015, 3, 4), new DateTime(2015, 3, 10));

// You can write Today and One Date
context.FromDateRange<LoginInfo>();
context.FromDateRange<LoginInfo>(new DateTime(2015, 3, 4));
```

Class definition can generate from MetaTable and it's helper method.

```csharp
new MetaTable("publicdata", "samples", "github_timeline")
	.GetTableSchema(context.BigQueryService)
	.BuildCSharpClass();

// or generate all tableinfo with guess table/tableprefix
context.BuildCSharpClass("mydataset");
```

Advanced Sample
---
Show github new repository every month by language.

![bq_linqpaddemo_graph](https://cloud.githubusercontent.com/assets/46207/4381900/acc0ff3c-437d-11e4-8c7b-cfa3487d76c8.jpg)

Code

```csharp
From<github_timeline>()
  .Where(x => x.repository_language != null && x.repository_fork == "false")
  .Select(x => new
  {
      x.repository_url,
      x.repository_created_at,
      language = LastValue(x, y => y.repository_language)
          .PartitionBy(y => y.repository_url)
          .OrderBy(y => y.created_at)
          .Value
  })
  .Into()
  .Select(x => new
  {
      x.language,
      yyyymm = StrftimeUtcUsec(BqFunc.ParseUtcUsec(x.repository_created_at), "%Y-%m"),
      count = CountDistinct(x.repository_url)
  })
  .GroupBy(x => new { x.language, x.yyyymm })
  .Having(x => GreaterThanEqual(x.yyyymm, "2010-01"))
  .Into()
  .Select(x => new
  {
      x.language,
      x.yyyymm,
      x.count,
      ratio = RatioToReport(x, y => y.count)
          .PartitionBy(y => y.yyyymm)
          .OrderBy(y => y.count)
          .Value
  })
  .Into()
  .Select(x => new
  {
      x.language,
      x.count,
      x.yyyymm,
      percentage = Round(x.ratio * 100, 2)
  })
  .OrderBy(x => x.yyyymm)
  .ThenByDescending(x => x.percentage)
  .Run()  // ↑BigQuery
  .Dump() // ↓LINQ to Objects(and LINQPad)
  .ToArray()
  .GroupBy(x => x.language)
  .DumpGroupChart(x => x.yyyymm, x => x.percentage);
```

It's query.

```sql
SELECT
  [language],
  [count],
  [yyyymm],
  ROUND(([ratio] * 100), 2) AS [percentage]
FROM
(
  SELECT
    [language],
    [yyyymm],
    [count],
    RATIO_TO_REPORT([count]) OVER (PARTITION BY [yyyymm] ORDER BY [count]) AS [ratio]
  FROM
  (
    SELECT
      [language],
      STRFTIME_UTC_USEC(PARSE_UTC_USEC([repository_created_at]), '%Y-%m') AS [yyyymm],
      COUNT(DISTINCT [repository_url]) AS [count]
    FROM
    (
      SELECT
        [repository_url],
        [repository_created_at],
        LAST_VALUE([repository_language]) OVER (PARTITION BY [repository_url] ORDER BY [created_at]) AS [language]
      FROM
        [githubarchive:github.timeline]
      WHERE
        (([repository_language] IS NOT NULL) AND ([repository_fork] = 'false'))
    )
    GROUP BY
      [language],
      [yyyymm]
    HAVING
      [yyyymm] >= '2010-01'
  )
)
ORDER BY
  [yyyymm], [percentage] DESC
```

Realtime Streaming Sample with Rx

```csharp
// [githubarchive:github.timeline@1411511274158-1411511574167]
// [githubarchive:github.timeline@1411511574167-1411511874174]
// [githubarchive:github.timeline@1411511874174-1411512174175]
// ...
Observable.Timer(TimeSpan.Zero, TimeSpan.FromMinutes(5))
    .Timestamp()
	.Buffer(2, 1) // Buffer Window
	.SelectMany(xs =>
	{
		this.UseQueryCache = false;
		return this.From<github_timeline>().WithRange(xs[0].Timestamp, xs[1].Timestamp)
			.Select(x => new { x.repository_name, x.created_at })
			.ToArrayAsync();
	})
	.Dump();
```

More query samples, see:[BigQuery.Linq.Tests](https://github.com/neuecc/LINQ-to-BigQuery/tree/master/BigQuery.Linq.Tests). You can see How to Join? How to Window? etc...

Create table, insert data samples, see:[SampleApps/StoreTwitterTimeline](https://github.com/neuecc/LINQ-to-BigQuery/blob/master/SampleApps/StoreTwitterTimeline/).

Author Info
---
Yoshifumi Kawai(a.k.a. neuecc) is software developer in Japan.  
He is Director/CTO at Grani, Inc.  
Grani is top social game developer in Japan.  
He awarded Microsoft MVP for Visual C# since 2011.  
He is known by creator of [linq.js](http://linqjs.codeplex.com/)(LINQ to Objects for JavaScript)  
Another his LINQ Project is [UniRx - Reactive Extensions for Unity](https://github.com/neuecc/UniRx)

Blog: http://neue.cc/ (JPN)  
Twitter: https://twitter.com/neuecc (JPN)

License
---
This library is under MIT License.
