LINQ-to-BigQuery
================
LINQ to BigQuery is C# LINQ Provider for [Google BigQuery](https://cloud.google.com/bigquery/). It also enables Desktop query editor and dump to chart with [LINQPad](http://www.linqpad.net).

Installation
---
binary from NuGet, [LINQ-to-BigQuery](https://nuget.org/packages/LINQ-to-BigQuery)

```
PM> Install-Package LINQ-to-BigQuery
```

LINQ with LINQPad
---
[LINQPad](http://www.linqpad.net) is editor for BigQuery!

Query and chart tab(Dump chart code, see:[LINQPad Integration section](https://github.com/neuecc/LINQ-to-BigQuery#linqpad-integration)

![bq_linqpad_first](https://cloud.githubusercontent.com/assets/46207/4381898/acbc7124-437d-11e4-99c4-aacddb4bad9c.jpg)

Result tab(Rows is expandable to grid view)

![bq_linqpad_results](https://cloud.githubusercontent.com/assets/46207/4381897/acba029a-437d-11e4-89e1-2e4a12cb5c48.jpg)

Why LINQ?
---
Strongly Typed with IntelliSense(and function documents).

![bq_intellisense](https://cloud.githubusercontent.com/assets/46207/4381901/acc1b576-437d-11e4-872c-5df1cafbae13.jpg)

![bq_intellisense_func](https://cloud.githubusercontent.com/assets/46207/4381902/acc2f72e-437d-11e4-8e3b-a79147b6f044.jpg)

All BigQuery functions is under BqFunc(except string.Contains). If you use C# 6.0(next version!), enable static using.

```csharp
// C# 6.0 Using Static
using BigQuery.Linq.BqFunc;
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

Class definition can generate from MetaTable and it's helper method.

```csharp
new MetaTable("publicdata", "samples", "github_timeline")
	.GetTableSchema(context.BigQueryService)
	.BuildCSharpClass();
```

If table is under your project, you can get all tableinfo.

```csharp
// Get All tableinfo(table_id, creation_time, row_count, size_bytes, etc...)
var tableInfos = context.GetAllTableInfo("mydataset");
// ToString - Human readable info
tableInfos.Select(x => x.ToString()).Dump();

// Get TableSchema
var schema = tableInfos[0].GetTableSchema(context.BigQueryService);

// Build C# class definition
schema.BuildCSharpClass().Dump();
```

Advanced Sample
---
Show github new repository every month by language.

![bq_linqpaddemo_graph](https://cloud.githubusercontent.com/assets/46207/4381900/acc0ff3c-437d-11e4-8c7b-cfa3487d76c8.jpg)

Code

```csharp
Query.GetContext()
    .From<github_timeline>()
    .Where(x => x.repository_language != null && x.repository_fork == "false")
    .Select(x => new
    {
        x.repository_url,
        x.repository_created_at,
        language = BqFunc.LastValue(x, y => y.repository_language)
            .PartitionBy(y => y.repository_url)
            .OrderBy(y => y.created_at)
            .Value
    })
    .Into()
    .Select(x => new
    {
        x.language,
        yyyymm = BqFunc.StrftimeUtcUsec(BqFunc.ParseUtcUsec(x.repository_created_at), "%Y-%m"),
        count = BqFunc.CountDistinct(x.repository_url)
    })
    .GroupBy(x => new { x.language, x.yyyymm })
    .Having(x => BqFunc.GreaterThanEqual(x.yyyymm, "2010-01"))
    .Into()
    .Select(x => new
    {
        x.language,
        x.yyyymm,
        x.count,
        ratio = BqFunc.RatioToReport(x, y => y.count)
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
        percentage = BqFunc.Round(x.ratio * 100, 2)
    })
    .OrderBy(x => x.yyyymm)
    .ThenByDescending(x => x.percentage)
    .Run()  // ↑BigQuery
    .Dump() // ↓LINQ to Objects(and LINQPad)
    .Rows
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
// [githubarchive:github.timeline@1411511274158000-1411511574167000]
// [githubarchive:github.timeline@1411511574167000-1411511874174000]
// [githubarchive:github.timeline@1411511874174000-1411512174175000]
// ...
Observable.Timer(TimeSpan.Zero, TimeSpan.FromMinutes(5))
    .Timestamp()
	.Buffer(2, 1) // Buffer Window
	.SelectMany(xs =>
	{
		var context = Query.GetContext();
		context.UseQueryCache = false;
		return context.From<github_timeline>().WithRange(xs[0].Timestamp, xs[1].Timestamp)
			.Select(x => new { x.repository_name, x.created_at })
			.ToArrayAsync();
	})
	.Dump();
```

More query samples, see:[BigQuery.Linq.Tests](https://github.com/neuecc/LINQ-to-BigQuery/tree/master/BigQuery.Linq.Tests). You can see How to Join? How to Window? etc...

LINQPad Integration
---
Put following code on LINQPad's My Extensions and configure auth json and projectId.

```csharp
// Import this namespaces
BigQuery.Linq
System.Windows.Forms.DataVisualization.Charting
Google.Apis.Auth.OAuth2
Google.Apis.Bigquery.v2
Google.Apis.Util.Store
Google.Apis.Services

public static class Query
{
    public static BigQueryContext GetContext()
    {
        BigQueryContext context;
        // Replace this JSON. OAuth2 JSON Generate from GCP Management Page. 
        var json = @"{""installed"":{""auth_uri"":""https://accounts.google.com/o/oauth2/auth"",""client_secret"":"""",""token_uri"":""https://accounts.google.com/o/oauth2/token"",""client_email"":"""",""redirect_uris"":[""urn:ietf:wg:oauth:2.0:oob"",""oob""],""client_x509_cert_url"":"""",""client_id"":"""",""auth_provider_x509_cert_url"":""https://www.googleapis.com/oauth2/v1/certs""}}";

        using (var ms = new MemoryStream(Encoding.UTF8.GetBytes(json)))
        {
            // Open Browser, Accept Auth
            var userCredential = GoogleWebAuthorizationBroker.AuthorizeAsync(ms,
                new[] { BigqueryService.Scope.Bigquery },
                "user",
                CancellationToken.None, new FileDataStore(@"LINQ-to-BigQuery")) // localcache
                .Result;

            var bigquery = new BigqueryService(new BaseClientService.Initializer
            {
                ApplicationName = "LINQ to BigQuery",
                HttpClientInitializer = userCredential
            });

            context = new BigQueryContext(bigquery, "write your project id");
        }
        // Timeout or other options
        context.TimeoutMs = (long)TimeSpan.FromMinutes(1).TotalMilliseconds;
        return context;
    }
}

public static class MyExtensions
{
    public static QueryResponse<T> DumpRun<T>(this IExecutableBigQueryable<T> source)
    {
        return source.Run().Dump();
    }

	public static IEnumerable<T> DumpChart<T>(this IEnumerable<T> source, Func<T, object> xSelector, Func<T, object> ySelector, SeriesChartType chartType = SeriesChartType.Column, bool isShowXLabel = false)
	{
	    var chart = new Chart();
		chart.ChartAreas.Add(new ChartArea());
		var series = new Series{ ChartType = chartType};
	    foreach (var item in source)
	    {
		    var x = xSelector(item);
			var y = ySelector(item);
		    var index = series.Points.AddXY(x, y);
			series.Points[index].ToolTip = item.ToString();
			if(isShowXLabel) series.Points[index].Label = x.ToString();
	    }
	    chart.Series.Add(series);
	    chart.Dump("Chart");
		return source;
	}
	
	public static IEnumerable<IGrouping<TKey, T>> DumpGroupChart<TKey, T>(this IEnumerable<IGrouping<TKey, T>> source, Func<T, object> xSelector, Func<T, object> ySelector, SeriesChartType chartType = SeriesChartType.Line)
	{
	    var chart = new Chart();
		chart.ChartAreas.Add(new ChartArea());
	    foreach (var g in source)
	    {
		    var series = new Series{ ChartType = chartType };
			foreach(var item in g)
			{
				var x = xSelector(item);
				var y = ySelector(item);
				var index = series.Points.AddXY(x, y);
				series.Points[index].ToolTip = item.ToString();
			}
			chart.Series.Add(series);
	    }
	    chart.Dump("Chart");
		return source;
	}
}
```

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
