using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams;
using Akka.Streams.Dsl;
using Arcane.Framework.Contracts;
using Arcane.Framework.Services.Base;
using Arcane.Framework.Sinks.Parquet;
using Arcane.Framework.Sources.SqlServer;
using Arcane.Stream.SqlServerChangeTracking.Models;
using Parquet.Data;
using Snd.Sdk.Metrics.Base;
using Snd.Sdk.Storage.Base;

namespace Arcane.Stream.SqlServerChangeTracking.Services;

public class SqlServerChangeTrackingGraphBuilder(IBlobStorageWriter blobStorageWriter, MetricsService metricsService)
    : IStreamGraphBuilder<SqlServerChangeTrackingStreamContext>
{
    public IRunnableGraph<(UniqueKillSwitch, Task)> BuildGraph(SqlServerChangeTrackingStreamContext context)
    {
        context.LoadSecretsFromEnvironment();
        var source = SqlServerChangeTrackingSource.Create(context.ConnectionString,
            context.Schema,
            context.Table,
            context.StreamKind,
            context.ChangeCaptureInterval,
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
            dropCompletionToken: context.IsBackfilling);
        
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
}
