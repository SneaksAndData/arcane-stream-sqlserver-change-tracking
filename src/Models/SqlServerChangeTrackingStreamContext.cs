using System;
using System.Linq;
using System.Text.Json.Serialization;
using Akka.Util;
using Arcane.Framework.Configuration;
using Arcane.Framework.Services.Base;
using Arcane.Framework.Services.Models;
using Arcane.Framework.Sinks.Extensions;
using Arcane.Framework.Sinks.Models;

namespace Arcane.Stream.SqlServerChangeTracking.Models;

public class SqlServerChangeTrackingStreamContext : IStreamContext, IStreamContextWriter
{
    /// <summary>
    /// Sql Server connection string.
    /// </summary>
    public string ConnectionString { get; set; }

    /// <summary>
    /// Table schema.
    /// </summary>
    public string Schema { get; set; }

    /// <summary>
    /// Table name.
    /// </summary>
    public string Table { get; set; }

    /// <summary>
    /// Number of rows per parquet rowgroup.
    /// </summary>
    public int RowsPerGroup { get; set; }

    /// <summary>
    /// Max time to wait for rowsPerGroup to accumulate.
    /// </summary>
    [JsonConverter(typeof(SecondsToTimeSpanConverter))]
    [JsonPropertyName("groupingIntervalSeconds")]
    public TimeSpan GroupingInterval { get; set; }

    /// <summary>
    /// Number of row groups per file.
    /// </summary>
    public int GroupsPerFile { get; set; }

    /// <summary>
    /// Data location for parquet files.
    /// </summary>
    public string SinkLocation { get; set; }
    
    /// <summary>
    /// Number of seconds to look back when determining first set of changes to extract.
    /// </summary>
    public int LookbackInterval { get; set; }

    /// <summary>
    /// Number of seconds to wait for result before sql commands should time out.
    /// </summary>
    public int CommandTimeout { get; set; }

    /// <summary>
    /// How long to wait before polling for next result set.
    /// </summary>
    [JsonPropertyName("changeCaptureIntervalSeconds")]
    public int ChangeCaptureInterval { get; set; }

    /// <inheritdoc cref="IStreamContext.StreamId"/>
    public string StreamId { get; private set; }

    /// <inheritdoc cref="IStreamContext.IsBackfilling"/>
    public bool IsBackfilling { get; private set; }

    /// <inheritdoc cref="IStreamContext.StreamKind"/>
    public string StreamKind { get; private set; }

    /// <summary>
    /// Property to hold stream metadata received from the stream context.
    /// </summary>
    [JsonPropertyName("streamMetadata")]
    public StreamMetadataDefinition StreamMetadata { get; set; }

    /// <inheritdoc cref="Framework.Sinks.Models.StreamMetadata"/>
    public Option<StreamMetadata> GetStreamMetadata()
    {
        if (this.StreamMetadata is null)
        {
            return Option<StreamMetadata>.None;
        }

        var partitions = this.StreamMetadata
            .Partitions
            .Append(this.StreamMetadata.DatePartition)
            .Select(partition => partition.ToStreamPartition())
            .ToArray();
        return new StreamMetadata(partitions);
    }

    /// <inheritdoc cref="IStreamContextWriter.SetStreamId"/>
    public void SetStreamId(string streamId)
    {
        this.StreamId = streamId;
    }

    /// <inheritdoc cref="IStreamContextWriter.SetBackfilling"/>
    public void SetBackfilling(bool isRunningInBackfillMode)
    {
        this.IsBackfilling = isRunningInBackfillMode;
    }

    /// <inheritdoc cref="IStreamContextWriter.SetStreamKind"/>
    public void SetStreamKind(string streamKind)
    {
        this.StreamKind = streamKind;
    }
    
    public void LoadSecretsFromEnvironment()
    {
        this.ConnectionString = this.GetSecretFromEnvironment("CONNECTIONSTRING");
    }
    
    private string GetSecretFromEnvironment(string secretName)
        => Environment.GetEnvironmentVariable($"{nameof(Arcane)}__{secretName}".ToUpperInvariant());
}
