using System;
using System.Threading.Tasks;
using Akka.Util.Extensions;
using Arcane.Framework.Contracts;
using Arcane.Framework.Providers.Hosting;
using Arcane.Framework.Services.Base;
using Arcane.Framework.Sources.SqlServer.Exceptions;
using Arcane.Stream.SqlServerChangeTracking.Models;
using Arcane.Stream.SqlServerChangeTracking.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;
using Snd.Sdk.Logs.Providers;
using Snd.Sdk.Metrics.Configurations;
using Snd.Sdk.Metrics.Providers;
using Snd.Sdk.Storage.Providers;
using Snd.Sdk.Storage.Providers.Configurations;

Log.Logger = DefaultLoggingProvider.CreateBootstrapLogger(nameof(Arcane));

int exitCode;
try
{
    exitCode = await Host.CreateDefaultBuilder(args)
        .AddDatadogLogging((_, _, conf) => conf.EnrichWithCustomProperties().WriteTo.Console())
        .ConfigureRequiredServices(services
            => services.AddStreamGraphBuilder<SqlServerChangeTrackingGraphBuilder, SqlServerChangeTrackingStreamContext>())
        .ConfigureAdditionalServices((services, context) =>
        {
            services.AddSingleton<IInterruptionToken>(sp => sp.GetRequiredService<IStreamLifetimeService>());
            services.AddDatadogMetrics(configuration: DatadogConfiguration.UnixDomainSocket(context.ApplicationName));
            services.AddAwsS3Writer(AmazonStorageConfiguration.CreateFromEnv());
        })
        .Build()
        .RunStream<SqlServerChangeTrackingStreamContext>(Log.Logger, (exception, _)=>
        {
            Log.Logger.Error(exception, "Fatal exception occured");
            return exception switch
            {
                SqlServerConnectionException => Task.FromResult(ExitCodes.RESTART.AsOption()),
                _ => Task.FromResult(ExitCodes.FATAL.AsOption())
            };
        });
}
catch (Exception ex)
{
    Log.Fatal(ex, "Host terminated unexpectedly");
    return ExitCodes.FATAL;
}
finally
{
    await Log.CloseAndFlushAsync();
}

return exitCode;
