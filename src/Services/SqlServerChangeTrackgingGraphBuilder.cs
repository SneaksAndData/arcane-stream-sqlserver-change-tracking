using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;
using Arcane.Framework.Contracts;
using Arcane.Framework.Services.Base;
using Arcane.Framework.Sinks.Models;
using Arcane.Framework.Sinks.Parquet;
using Arcane.Framework.Sources.Exceptions;
using Arcane.Framework.Sources.SqlServer;
using Arcane.Framework.Sources.SqlServer.Exceptions;
using Arcane.Stream.SqlServerChangeTracking.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Parquet.Data;
using Snd.Sdk.Metrics.Base;
using Snd.Sdk.Storage.Base;

namespace Arcane.Stream.SqlServerChangeTracking.Services;

public class SqlServerChangeTrackingGraphBuilder(
    IBlobStorageWriter blobStorageWriter,
    MetricsService metricsService,
    ILogger<SqlServerChangeTrackingGraphBuilder> logger
    ) : IStreamGraphBuilder<SqlServerChangeTrackingStreamContext>
{
    public IRunnableGraph<(UniqueKillSwitch, Task)> BuildGraph(SqlServerChangeTrackingStreamContext context)
    {
        context.LoadSecretsFromEnvironment();
        try
        {  
            var streamMeta = context.GetStreamMetadata().GetOrElse(new StreamMetadata(Option<StreamPartition[]>.None));
            var source = SqlServerChangeTrackingSource.Create(
                connectionString: context.ConnectionString,
                schemaName: context.Schema,
                tableName: context.Table,
                partitioningExpression: streamMeta.Partitions.GetOrElse([]).FirstOrDefault(sp => sp.IsDatePartition)?.FieldExpression,
                TimeSpan.FromSeconds(context.ChangeCaptureInterval),
                context.CommandTimeout,
                context.LookbackInterval,
                context.IsBackfilling,
                context.IsBackfilling);

            var schema = source.GetParquetSchema();
            var dimensions = source.GetDefaultTags().GetAsDictionary(context, context.StreamId);

            var parquetSink = ParquetSink.Create(
                parquetSchema: schema,
                storageWriter: blobStorageWriter,
                parquetFilePath: $"{context.SinkLocation}/{context.StreamId}",
                rowGroupsPerFile: context.GroupsPerFile,
                createSchemaFile: true,
                dataSinkPathSegment: context.IsBackfilling ? "backfill" : "data",
                dropCompletionToken: context.IsBackfilling,
                streamMetadata: context.GetStreamMetadata().GetOrElse(new StreamMetadata(Option<StreamPartition[]>.None)));

            return Source
                .FromGraph(source)
                .GroupedWithin(context.RowsPerGroup, context.GroupingInterval)
                .Select(grp =>
                {
                    var rows = grp.ToList();
                    metricsService.Increment(DeclaredMetrics.ROWS_INCOMING, dimensions, rows.Count);
                    return rows.AsRowGroup(schema);
                })
                .ViaMaterialized(KillSwitches.Single<List<DataColumn>>(), Keep.Right)
                .ToMaterialized(parquetSink, Keep.Both);
        }
        catch (Exception ex)
        {
            if (ex is SqlException { Number: 4998 or 22105 } rootCause)
            {
                logger.LogError(ex, "Schema mismatched in attempt to activate stream {streamId}", context.StreamId);
                throw new SchemaMismatchException(rootCause);
            }
            
            if (ex is SqlException { Number: 35 or 0 })
            {
                throw new SqlServerConnectionException(context.StreamKind, ex);
            }
            
            if (ex is SqlException sqlException)
            {
                logger.LogError(ex, "Error while creating stream for {streamId}. Got {exception} with {number} and {errorCode}",
                    context.StreamId, nameof(SqlException), sqlException.Number, sqlException.ErrorCode);
                throw;
            }
            
            logger.LogError(ex, "Error while creating stream for {streamId}", context.StreamId);
            throw;
        }
    }
}
