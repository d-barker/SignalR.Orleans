using Microsoft.Extensions.DependencyInjection;
using SignalR.Orleans;

// ReSharper disable once CheckNamespace
namespace Orleans.Hosting;

public static class IClientBuilderExtensions
{
    public static IClientBuilder UseSignalR(this IClientBuilder builder, Action<SignalRClientConfig>? configure = null)
    {
        var cfg = new SignalRClientConfig();
        configure?.Invoke(cfg);
        return builder.UseSignalR(cfg);
    }

    public static IClientBuilder UseSignalR(this IClientBuilder builder, SignalRClientConfig? config = null)
    {
        config ??= new SignalRClientConfig();
        return builder.AddMemoryStreams(SignalROrleansConstants.SIGNALR_ORLEANS_STREAM_PROVIDER);
    }

    /// <summary>
    /// Registers an <see cref="OrleansSignalRConnectionMonitor"/> as the client's
    /// <see cref="IClientConnectionRetryFilter"/>, so the SignalR.Orleans hub lifetime manager
    /// re-establishes its stream subscriptions automatically after a full cluster recycle (for example,
    /// scaling the silo from one replica to many) — without restarting the hub server process.
    ///
    /// <para>
    /// This sets the client's retry filter. If the application already has its own
    /// <see cref="IClientConnectionRetryFilter"/>, pass it via
    /// <see cref="OrleansSignalRConnectionMonitorOptions.InnerRetryFilter"/> so its retry/back-off policy
    /// is preserved while the monitor still emits the reconnect signal.
    /// </para>
    /// </summary>
    public static IClientBuilder AddSignalRBackplaneSelfHealing(
        this IClientBuilder builder,
        Action<OrleansSignalRConnectionMonitorOptions>? configure = null)
    {
        if (configure is not null)
        {
            builder.Services.Configure(configure);
        }

        builder.Services.AddSingleton<OrleansSignalRConnectionMonitor>();
        builder.Services.AddSingleton<IClientConnectionRetryFilter>(
            sp => sp.GetRequiredService<OrleansSignalRConnectionMonitor>());

        return builder;
    }
}
