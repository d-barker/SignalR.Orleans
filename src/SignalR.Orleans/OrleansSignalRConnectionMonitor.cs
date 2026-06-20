using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans;

namespace SignalR.Orleans;

/// <summary>
/// Options for <see cref="OrleansSignalRConnectionMonitor"/>.
/// </summary>
public sealed class OrleansSignalRConnectionMonitorOptions
{
    /// <summary>
    /// Delay between failed Orleans cluster (re)connection attempts. Default: 5 seconds.
    /// Ignored when <see cref="InnerRetryFilter"/> is set.
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Maximum number of consecutive failed connection attempts before giving up (the retry filter
    /// returns <c>false</c>). <c>0</c> (default) retries indefinitely — the correct behaviour for a hub
    /// server that must rejoin the cluster whenever it returns. Ignored when
    /// <see cref="InnerRetryFilter"/> is set.
    /// </summary>
    public int MaxRetryAttempts { get; set; }

    /// <summary>
    /// Optional existing retry policy to delegate the keep-retrying decision (and its back-off) to.
    /// When set, the monitor still raises <see cref="OrleansSignalRConnectionMonitor.ConnectionLost"/>
    /// but defers the retry decision to this filter, so an application's existing connection policy is
    /// preserved rather than replaced.
    /// </summary>
    public IClientConnectionRetryFilter? InnerRetryFilter { get; set; }
}

/// <summary>
/// An <see cref="IClientConnectionRetryFilter"/> that doubles as a connection-loss signal for the
/// SignalR.Orleans backplane.
///
/// <para>
/// An Orleans <em>client</em> stream subscription does not survive a full cluster recycle (for example,
/// scaling the silo from one replica to many restarts every silo): the client reconnects to a new
/// cluster generation, but the hub server's in-memory <c>SERVER_STREAM</c> / <c>ALL_STREAM</c> observers
/// are orphaned and nothing re-establishes them, so messages stop reaching clients until the hub server
/// process is restarted. Orleans invokes <see cref="ShouldRetryConnectionAttempt"/> on every failed
/// connection attempt, which is a prompt and reliable "the cluster is unreachable" signal.
/// <see cref="OrleansHubLifetimeManager{THub}"/> subscribes to <see cref="ConnectionLost"/> and
/// re-establishes its stream subscriptions as soon as the cluster is reachable again.
/// </para>
///
/// <para>Register it on the Orleans client with <c>clientBuilder.AddSignalRBackplaneSelfHealing()</c>.</para>
/// </summary>
public sealed class OrleansSignalRConnectionMonitor : IClientConnectionRetryFilter
{
    private readonly OrleansSignalRConnectionMonitorOptions _options;
    private readonly ILogger<OrleansSignalRConnectionMonitor> _logger;
    private int _attempts;

    public OrleansSignalRConnectionMonitor(
        IOptions<OrleansSignalRConnectionMonitorOptions> options,
        ILogger<OrleansSignalRConnectionMonitor> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    /// <summary>
    /// Raised on every failed cluster connection attempt — i.e. while the connection is down. Handlers
    /// must be fast and must not throw; a throwing handler is logged and swallowed so it can never break
    /// the Orleans reconnect loop. Listeners are expected to coalesce signals (recovery is single-flight).
    /// </summary>
    public event Action? ConnectionLost;

    /// <summary>
    /// Resets the consecutive-failure counter. Called by a hub lifetime manager once it has confirmed the
    /// cluster is reachable and re-established its subscriptions, so
    /// <see cref="OrleansSignalRConnectionMonitorOptions.MaxRetryAttempts"/> is applied per outage rather
    /// than for the lifetime of the process.
    /// </summary>
    public void NotifyReconnected() => Interlocked.Exchange(ref _attempts, 0);

    /// <inheritdoc />
    public async Task<bool> ShouldRetryConnectionAttempt(Exception exception, CancellationToken cancellationToken)
    {
        // Signal first so listeners learn about the outage on the very first failed attempt, regardless
        // of which retry policy decides whether to keep going.
        RaiseConnectionLost();

        if (_options.InnerRetryFilter is { } inner)
        {
            return await inner.ShouldRetryConnectionAttempt(exception, cancellationToken);
        }

        var attempt = Interlocked.Increment(ref _attempts);

        if (_options.MaxRetryAttempts > 0 && attempt >= _options.MaxRetryAttempts)
        {
            _logger.LogError(exception,
                "Giving up reconnecting to the Orleans cluster after {Attempts} failed attempt(s).", attempt);
            return false;
        }

        _logger.LogWarning(exception,
            "Orleans cluster connection attempt {Attempt} failed; retrying in {Delay}.", attempt, _options.RetryDelay);

        try
        {
            await Task.Delay(_options.RetryDelay, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            return false;
        }

        return true;
    }

    private void RaiseConnectionLost()
    {
        var handler = ConnectionLost;
        if (handler is null)
        {
            return;
        }

        try
        {
            handler();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "A ConnectionLost handler threw; continuing the Orleans reconnect loop.");
        }
    }
}
