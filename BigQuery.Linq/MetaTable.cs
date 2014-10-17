using Google.Apis.Bigquery.v2;
using Google.Apis.Bigquery.v2.Data;
using Google.Apis.Util;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    /// <summary>
    /// Table meta data shown by __TABLES__.
    /// </summary>
    public class MetaTable
    {
        /// <summary>name of the project.</summary>
        public string project_id { get; set; }
        /// <summary>name of the dataset.</summary>
        public string dataset_id { get; set; }
        /// <summary>name of the table.</summary>
        public string table_id { get; set; }
        /// <summary>time, in milliseconds since 1/1/1970 UTC, that the table was created. This is the same as the creation_time field on the table.</summary>
        public long creation_time { get; set; }
        /// <summary>time, in milliseconds since 1/1/1970 UTC, that the table was updated (either metadata or table contents).</summary>
        public long last_modified_time { get; set; }
        /// <summary>number of rows in the table.</summary>
        public long row_count { get; set; }
        /// <summary>total size in bytes of the table.</summary>
        public long size_bytes { get; set; }
        /// <summary>whether it is a view (2) or regular table (1).</summary>
        public long type { get; set; }

        public MetaTable()
        {

        }

        public MetaTable(string fullTableId)
        {
            var first = fullTableId.UnescapeBq().Split(':');
            this.project_id = first[0];
            var second = first[1].Split('.');
            this.dataset_id = second[0];
            this.table_id = string.Concat(second.Skip(1));
        }

        public MetaTable(string project_id, string dataset_id, string table_id)
        {
            this.project_id = project_id;
            this.dataset_id = dataset_id;
            this.table_id = table_id;
        }

        public MetaTableSchema GetTableSchema(BigqueryService service)
        {
            var response = service.Tables.Get(project_id, dataset_id, table_id).Execute();
            return new MetaTableSchema(this, response.Schema.Fields);
        }

        public async Task<MetaTableSchema> GetTableSchemaAsync(BigqueryService service)
        {
            var response = await service.Tables.Get(project_id, dataset_id, table_id).ExecuteAsync().ConfigureAwait(false);
            return new MetaTableSchema(this, response.Schema.Fields);
        }

        /// <param name="description">[Optional] A user-friendly description of this table.</param>
        /// <param name="expirationTime">
        /// <para>[Optional] The time when this table expires, in milliseconds since the epoch.</para>
        /// <para>If not present, the table will persist indefinitely. Expired tables will</para>
        /// <para>be deleted and their storage reclaimed.</para>
        /// </param>
        /// <param name="friendlyName">[Optional] A descriptive name for this table.</param>
        public void CreateTable(BigqueryService service, TableFieldSchema[] fields, string description = null, long? expirationTime = null, string friendlyName = null)
        {
            var r = service.Tables.Insert(new Table()
            {
                Description = description,
                ExpirationTime = expirationTime,
                FriendlyName = friendlyName,
                TableReference = new TableReference
                {
                    ProjectId = this.project_id,
                    DatasetId = this.dataset_id,
                    TableId = this.table_id,
                },
                Schema = new TableSchema()
                {
                    Fields = fields
                }
            }, this.project_id, this.dataset_id).Execute();

            if (r.CreationTime != null)
            {
                this.creation_time = r.CreationTime.Value;
            }
            if(r.LastModifiedTime != null)
            {
                this.last_modified_time = r.LastModifiedTime.Value;
            }
        }

        /// <param name="description">[Optional] A user-friendly description of this table.</param>
        /// <param name="expirationTime">
        /// <para>[Optional] The time when this table expires, in milliseconds since the epoch.</para>
        /// <para>If not present, the table will persist indefinitely. Expired tables will</para>
        /// <para>be deleted and their storage reclaimed.</para>
        /// </param>
        /// <param name="friendlyName">[Optional] A descriptive name for this table.</param>
        public async Task CreateTableAsync(BigqueryService service, TableFieldSchema[] fields, string description = null, long? expirationTime = null, string friendlyName = null)
        {
            var r = await service.Tables.Insert(new Table()
            {
                Description = description,
                ExpirationTime = expirationTime,
                FriendlyName = friendlyName,
                TableReference = new TableReference
                {
                    ProjectId = this.project_id,
                    DatasetId = this.dataset_id,
                    TableId = this.table_id,
                },
                Schema = new TableSchema()
                {
                    Fields = fields
                }
            }, this.project_id, this.dataset_id).ExecuteAsync().ConfigureAwait(false);

            this.creation_time = r.CreationTime.Value;
            this.last_modified_time = r.LastModifiedTime.Value;
        }

        /// <param name="retryStrategy">If not null, try retry.</param>
        public async Task InsertAllAsync<T>(BigqueryService service, IEnumerable<T> data, IBackOff retryStrategy = null, Func<T, string> insertIdSelector = null, JsonSerializerSettings serializerSettings = null)
        {
            if (insertIdSelector == null)
            {
                insertIdSelector = _ => Guid.NewGuid().ToString();
            }

            var rows = data
                .Select(x => new Google.Apis.Bigquery.v2.Data.TableDataInsertAllRequest.RowsData
                {
                    InsertId = insertIdSelector(x),
                    Json = JsonConvert.DeserializeObject<Dictionary<string, object>>(JsonConvert.SerializeObject(x, serializerSettings))
                })
                .Where(x => x.Json != null)
                .ToArray();

            if (!rows.Any()) return;

            var request = service.Tabledata.InsertAll(new TableDataInsertAllRequest
            {
                Rows = rows
            }, this.project_id, this.dataset_id, this.table_id);

            var retry = 0;
            TableDataInsertAllResponse response = null;
            Exception lastError;
            do
            {
                try
                {
                    lastError = null;
                    response = await request.ExecuteAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    lastError = ex;
                }

                if (retryStrategy == null) break;
                if (response != null && response.InsertErrors == null) break;

                retry++;
                var nextDelay = retryStrategy.GetNextBackOff(retry);
                if (nextDelay == TimeSpan.MinValue) break;

                await Task.Delay(nextDelay).ConfigureAwait(false);
            } while (true);

            if (lastError != null)
            {
                var exception = new InsertAllFailedException("", lastError)
                {
                    RetryCount = retry,
                    InternalErrorInfos = new InsertAllFailedException.ErrorInfo[0],
                };

                throw exception;
            }

            if (response.InsertErrors != null && response.InsertErrors.Any())
            {
                var errorMessages = response.InsertErrors.Zip(rows, (x, r) =>
                {
                    return x.Errors.Select(e =>
                    {
                        return new InsertAllFailedException.ErrorInfo
                        {
                            Index = x.Index,
                            DebugInfo = e.DebugInfo,
                            ETag = e.ETag,
                            Location = e.Location,
                            Message = e.Message,
                            Reason = e.Reason,
                            PostRawJSON = JsonConvert.SerializeObject(r.Json, Formatting.None)
                        };
                    });
                }).SelectMany(xs => xs);

                var exception = new InsertAllFailedException
                {
                    RetryCount = retry,
                    InternalErrorInfos = errorMessages.ToArray()
                };

                throw exception;
            }
        }

        public string ToFullTableName()
        {
            var tableInfo = string.Format("[{0}:{1}.{2}]", project_id, dataset_id, table_id);
            return tableInfo;
        }

        public override string ToString()
        {
            var tableInfo = string.Format("[{0}:{1}.{2}]", project_id, dataset_id, table_id);
            var sizeInfo = string.Format("Size:{0}, RowCount:{1}", size_bytes.ToHumanReadableSize(), row_count);
            var time = string.Format("Created:{0}, LastModified:{1}", creation_time.FromTimestampMilliSeconds(), last_modified_time.FromTimestampMilliSeconds());
            return tableInfo + sizeInfo + ", " + time;
        }
    }

    public class InsertAllFailedException : Exception
    {
        public int RetryCount { get; internal set; }
        public ErrorInfo[] InternalErrorInfos { get; internal set; }

        public class ErrorInfo
        {
            public long? Index { get; internal set; }
            public string DebugInfo { get; internal set; }
            public string ETag { get; internal set; }
            public string Location { get; internal set; }
            public string Message { get; internal set; }
            public string Reason { get; internal set; }
            public string PostRawJSON { get; internal set; }

            public override string ToString()
            {
                return string.Format(@"Index:{0}
DebugInfo:{1}
ETag:{2}
Location:{3}
Message:{4}
Reason:{5}
PostRawJSON:{6}", Index, DebugInfo, ETag, Location, Message, Reason, PostRawJSON);
            }
        }

        public InsertAllFailedException()
        {

        }

        public InsertAllFailedException(string message)
            : base(message)
        {

        }

        public InsertAllFailedException(string message, Exception innerException)
            : base(message, innerException)
        {

        }
    }
}
