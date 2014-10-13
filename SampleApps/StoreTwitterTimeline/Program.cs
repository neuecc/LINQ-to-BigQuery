using BigQuery.Linq;
using CoreTweet;
using CoreTweet.Streaming;
using CoreTweet.Streaming.Reactive;
using Google.Apis.Bigquery.v2.Data;
using Google.Apis.Util;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Reflection;

namespace StoreTwitterTimeline
{
    class Program
    {
        static TableFieldSchema[] GetSchemas()
        {
            var schemas = DataTypeUtility.ToTableFieldSchema(typeof(Status), (pi) =>
            {
                // avoid circular reference
                if (pi == typeof(Place).GetProperty("ContainedWithin"))
                {
                    return new TableFieldSchema
                    {
                        Name = "contained_within_id",
                        Type = DataType.String.ToIdentifier(),
                        Mode = "REPEATED"
                    };
                }
                if (pi == typeof(Status).GetProperty("RetweetedStatus"))
                {
                    return new TableFieldSchema
                    {
                        Name = "retweeted_status_id",
                        Type = DataType.Integer.ToIdentifier(),
                    };
                }
                if (pi == typeof(User).GetProperty("Status"))
                {
                    return new TableFieldSchema
                    {
                        Name = "status_id",
                        Type = DataType.Integer.ToIdentifier()
                    };
                }

                // avoid complex type

                if (pi.PropertyType == typeof(double[][][])) // Place/BoundingBox/Coordinates
                {
                    return new TableFieldSchema
                    {
                        Name = pi.GetCustomAttribute<JsonPropertyAttribute>().PropertyName,
                        Type = DataType.String.ToIdentifier()
                    };
                }

                // Type mapping
                if (pi.PropertyType == typeof(Uri))
                {
                    return new TableFieldSchema
                    {
                        Name = pi.GetCustomAttribute<JsonPropertyAttribute>().PropertyName,
                        Type = DataType.String.ToIdentifier()
                    };
                }
                if (pi.PropertyType == typeof(Dictionary<string, object>))
                {
                    return new TableFieldSchema
                    {
                        Name = pi.GetCustomAttribute<JsonPropertyAttribute>().PropertyName,
                        Type = DataType.String.ToIdentifier()
                    };
                }

                return null;
            });

            return schemas;
        }


        class StatusResolver : Newtonsoft.Json.Serialization.DefaultContractResolver
        {
            protected override IList<Newtonsoft.Json.Serialization.JsonProperty> CreateProperties(Type type, MemberSerialization memberSerialization)
            {
                var result = base.CreateProperties(type, memberSerialization);

                if (type == typeof(Place))
                {
                    var target = result.First(x => x.PropertyName == "contained_within");
                    target.PropertyName = "contained_within_id";
                    target.PropertyType = typeof(string[]);
                    target.ValueProvider = new GenericValueProvider<Place>(x => (x.ContainedWithin != null) ? x.ContainedWithin.Select(y => y.Id).ToArray() : new string[0]);
                }
                if (type == typeof(Status))
                {
                    var target = result.First(x => x.PropertyName == "retweeted_status");
                    target.PropertyName = "retweeted_status_id";
                    target.PropertyType = typeof(long?);
                    target.ValueProvider = new GenericValueProvider<Status>(x => (x.RetweetedStatus != null) ? (long?)x.RetweetedStatus.Id : null);
                }
                if (type == typeof(User))
                {
                    var target = result.First(x => x.PropertyName == "status");
                    target.PropertyName = "status_id";
                    target.PropertyType = typeof(long?);
                    target.ValueProvider = new GenericValueProvider<User>(x => (x.Status != null) ? (long?)x.Status.Id : null);
                }
                if (type == typeof(BoundingBox))
                {
                    var target = result.First(x => x.PropertyName == "coordinates");
                    target.PropertyType = typeof(string);
                    target.ValueProvider = new GenericValueProvider<BoundingBox>(x => JsonConvert.SerializeObject(x.Coordinates));
                }
                if (type == typeof(Coordinates))
                {
                    var target = result.First(x => x.PropertyName == "coordinates");
                    target.Ignored = true; // ignore double private field
                }

                return result;
            }
        }

        class GenericValueProvider<T> : IValueProvider
        {
            readonly Func<T, object> getValue;

            public GenericValueProvider(Func<T, object> getValue)
            {
                this.getValue = getValue;
            }

            public object GetValue(object target)
            {
                return getValue((T)target);
            }

            public void SetValue(object target, object value)
            {

            }
        }

        static void CreateTable()
        {
            var context = Account.GetContext();
            var service = context.BigQueryService;
            var schema = GetSchemas();

            new MetaTable(context.ProjectId, "twitter", "sample")
                .CreateTable(service, schema, "Twitter Streaming Timeline:Sample");

            new MetaTable(context.ProjectId, "twitter", "user")
                .CreateTable(service, schema, "Twitter Streaming Timeline:User");

            new MetaTable(context.ProjectId, "twitter", "error")
                .CreateTable(service, DataTypeUtility.ToTableFieldSchema(new
                {
                    Type = "",
                    StackTrace = "",
                    Message = "",
                    Source = "",
                }));
        }

        static void Main(string[] args)
        {
            // If not created table yet, commentout and create table!
            // CreateTable();
            // return;

            var context = Account.GetContext();
            var token = Account.GetTokens();
            var resolverSettings = new JsonSerializerSettings { ContractResolver = new StatusResolver() };

            var sampleCount = 0;
            var userCount = 0;
            var errorTable = new MetaTable(context.ProjectId, "twitter", "error");

            var sample = new MetaTable(context.ProjectId, "twitter", "sample");
            var sampleInsert = token.Streaming.StartObservableStream(CoreTweet.Streaming.StreamingType.Sample)
                .OfType<StatusMessage>()
                .Select(x => x.Status)
                .Buffer(TimeSpan.FromSeconds(10), 100)
                .SelectMany(tweets =>
                {
                    sampleCount += tweets.Count;
                    return sample.InsertAllAsync(
                            context.BigQueryService,
                            tweets,
                            new ExponentialBackOff(),
                            insertIdSelector: x => x.Id.ToString(),
                            serializerSettings: resolverSettings)
                        .ToObservable();
                })
                .Do(_ => { }, ex => errorTable.InsertAllAsync(context.BigQueryService, new[] { ex }).Wait())
                .Retry();

            var user = new MetaTable(context.ProjectId, "twitter", "user");
            var userInsert = token.Streaming.StartObservableStream(CoreTweet.Streaming.StreamingType.User)
                .OfType<StatusMessage>()
                .Select(x => x.Status)
                .Buffer(TimeSpan.FromSeconds(10), 100)
                .SelectMany(tweets =>
                {
                    userCount += tweets.Count;
                    return user.InsertAllAsync(
                            context.BigQueryService,
                            tweets,
                            new ExponentialBackOff(),
                            insertIdSelector: x => x.Id.ToString(),
                            serializerSettings: resolverSettings)
                        .ToObservable();
                })
                .Do(_ => { }, ex => errorTable.InsertAllAsync(context.BigQueryService, new[] { ex }).Wait())
                .Retry();

            // start insert
            new[] { sampleInsert, userInsert }.Merge().Subscribe();

            // wait(synchronous timer) and output status
            Observable.Interval(TimeSpan.FromSeconds(10), Scheduler.CurrentThread)
                .Subscribe(x =>
                {
                    Console.WriteLine("Sample:" + sampleCount + " | " + "User:" + userCount);
                });
        }
    }
}