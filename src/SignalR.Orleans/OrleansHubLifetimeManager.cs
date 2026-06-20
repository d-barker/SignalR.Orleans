using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;
using Orleans.Runtime;
using Orleans.Streams;

namespace SignalR.Orleans;

// TODO: Is this thing called in a threadsafe manner by signalR? 
public sealed class OrleansHubLifetimeManager<THub> : HubLifetimeManager<THub>, ILifecycleParticipant<ISiloLifecycle>,
    IDisposable where THub : Hub
{
    private readonly Guid _serverId;
    private readonly ILogger _logger;
    private readonly string _hubName;
    private readonly IClusterClient _clusterClient;
    private readonly SemaphoreSlim _streamSetupLock = new(1);
    private readonly HubConnectionStore _connections = new();

    private IStreamProvider? _streamProvider;
    private IAsyncStream<ClientMessage> _serverStream = default!;
    private IAsyncStream<AllMessage> _allStream = default!;
    private Timer _timer = default!;

    // Self-heal. An Orleans *client* stream subscription does not survive a full cluster recycle — e.g.
    // scaling the silo from 1 -> N restarts every silo, so the client connection drops and reconnects to
    // a brand-new cluster generation. The subscription's in-memory observer is orphaned and nothing
    // re-establishes it, so this hub server silently stops receiving SERVER_STREAM / ALL_STREAM messages
    // until the process restarts. (Producer-side grains recover automatically because they rehydrate
    // their state from storage on reactivation.)
    //
    // Primary recovery is event-driven: an OrleansSignalRConnectionMonitor (an IClientConnectionRetryFilter)
    // raises ConnectionLost the instant the client can't reach the cluster, flipping us to "disconnected"
    // and starting a single-flight recovery loop (RecoverAsync) that re-subscribes as soon as the cluster
    // is reachable again. A low-frequency fallback probe heals the cases where no monitor is registered
    // (e.g. on a silo) or a signal was missed.
    private static readonly TimeSpan RecoveryProbeInterval = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan FallbackProbeInterval = TimeSpan.FromSeconds(60);
    private readonly OrleansSignalRConnectionMonitor? _connectionMonitor;
    private Timer _fallbackTimer = default!;
    private volatile bool _connected = true;
    private int _recovering;

    public OrleansHubLifetimeManager(
        ILogger<OrleansHubLifetimeManager<THub>> logger,
        IClusterClient clusterClient,
        OrleansSignalRConnectionMonitor? connectionMonitor = null
    )
    {
        var hubType = typeof(THub).BaseType?.GenericTypeArguments.FirstOrDefault() ?? typeof(THub);
        _hubName = hubType.IsInterface && hubType.Name[0] == 'I'
            ? hubType.Name[1..]
            : hubType.Name;
        _serverId = Guid.NewGuid();
        _logger = logger;
        _clusterClient = clusterClient;
        _connectionMonitor = connectionMonitor;
    }

    private Task HeartbeatCheck()
      => _clusterClient.GetServerDirectoryGrain().Heartbeat(_serverId);

    private async Task EnsureStreamSetup()
    {
        if (_streamProvider is not null)
            return;

        await _streamSetupLock.WaitAsync();

        try
        {
            if (_streamProvider is not null)
                return;

            _logger.LogInformation(
                "Initializing: Orleans HubLifetimeManager {hubName} (serverId: {serverId})...",
                _hubName, _serverId);

            _streamProvider = _clusterClient.GetOrleansSignalRStreamProvider();
            _serverStream = _streamProvider.GetServerStream(_serverId);
            _allStream = _streamProvider.GetAllStream(_hubName);

            _timer = new Timer(
                _ => Task.Run(HeartbeatCheck), null, TimeSpan.FromSeconds(0),
                TimeSpan.FromMinutes(SignalROrleansConstants.SERVER_HEARTBEAT_PULSE_IN_MINUTES));

            await SubscribeStreamsAsync();

            // Primary, event-driven recovery: the monitor raises ConnectionLost the instant the Orleans
            // client loses the cluster, which starts a single-flight recovery loop. No steady-state polling.
            if (_connectionMonitor is not null)
            {
                _connectionMonitor.ConnectionLost += OnConnectionLost;
            }

            // Fallback safety net: a low-frequency read-only probe that heals even when no monitor is
            // registered (e.g. on a silo) or a ConnectionLost signal was missed.
            _fallbackTimer = new Timer(
                _ => Task.Run(FallbackProbeAsync), null, FallbackProbeInterval, FallbackProbeInterval);

            _logger.LogInformation(
                "Initialized complete: Orleans HubLifetimeManager {hubName} (serverId: {serverId})",
                _hubName, _serverId);
        }
        finally
        {
            _streamSetupLock.Release();
        }
    }

    /// <summary>
    /// (Re)subscribes the server and broadcast streams. Resumes the existing subscription when one is
    /// already recorded in PubSub (re-wiring the live observer) rather than stacking a duplicate, so it
    /// is safe to call both for the initial setup and for re-establishing delivery after a reconnect.
    /// </summary>
    private async Task SubscribeStreamsAsync()
    {
        await ResumeOrSubscribeAsync(_serverStream, (msg, _) => ProcessServerMessage(msg));
        await ResumeOrSubscribeAsync(_allStream, (msg, _) => ProcessAllMessage(msg));
    }

    private static async Task ResumeOrSubscribeAsync<T>(IAsyncStream<T> stream, Func<T, StreamSequenceToken, Task> onNext)
    {
        var handles = await stream.GetAllSubscriptionHandles();
        if (handles.Count > 0)
        {
            // Re-attach the observer to the existing subscription; drop any duplicates so repeated
            // reconnects can't accumulate subscriptions.
            await handles[0].ResumeAsync(onNext);
            for (var i = 1; i < handles.Count; i++)
            {
                await handles[i].UnsubscribeAsync();
            }
        }
        else
        {
            await stream.SubscribeAsync(onNext);
        }
    }

    /// <summary>
    /// <see cref="OrleansSignalRConnectionMonitor.ConnectionLost"/> handler. Marks the backplane as
    /// disconnected and kicks the single-flight recovery loop. Runs on the Orleans client's reconnect
    /// path, so it must be cheap and non-blocking.
    /// </summary>
    private void OnConnectionLost()
    {
        _connected = false;
        _ = Task.Run(RecoverAsync);
    }

    /// <summary>
    /// Fallback safety net invoked on a low-frequency timer. Detects a dropped connection with a
    /// read-only probe and triggers recovery. Covers the cases where no
    /// <see cref="OrleansSignalRConnectionMonitor"/> is registered (e.g. on a silo) or a ConnectionLost
    /// signal was missed. A no-op while the connection is healthy beyond a single cheap probe.
    /// </summary>
    private async Task FallbackProbeAsync()
    {
        if (_streamProvider is null)
        {
            return;
        }

        if (_connected)
        {
            try
            {
                // Read-only probe (no storage write, unlike the ServerDirectory heartbeat).
                await _serverStream.GetAllSubscriptionHandles();
                return;
            }
            catch
            {
                _connected = false;
            }
        }

        await RecoverAsync();
    }

    /// <summary>
    /// Single-flight recovery loop. Waits (polling a read-only probe) until the cluster is reachable, then
    /// re-establishes the stream subscriptions so message delivery resumes without a process restart.
    /// Coalesces the many ConnectionLost signals that arrive during an outage into one running loop, and
    /// re-arms itself if a fresh loss lands while it is finishing.
    /// </summary>
    private async Task RecoverAsync()
    {
        if (_streamProvider is null || _connected)
        {
            return;
        }

        if (Interlocked.Exchange(ref _recovering, 1) == 1)
        {
            return;
        }

        try
        {
            _logger.LogWarning(
                "Backplane connection lost for {hubName} (serverId: {serverId}); waiting for the cluster and re-subscribing.",
                _hubName, _serverId);

            while (!_connected)
            {
                try
                {
                    // Read-only probe: throws while the client is disconnected, succeeds once the cluster
                    // is reachable again.
                    await _serverStream.GetAllSubscriptionHandles();
                }
                catch
                {
                    await Task.Delay(RecoveryProbeInterval);
                    continue;
                }

                await ResubscribeAsync();
                _connected = true;
                _connectionMonitor?.NotifyReconnected();

                _logger.LogInformation(
                    "Backplane subscriptions re-established for {hubName} (serverId: {serverId}).",
                    _hubName, _serverId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Backplane recovery failed for {hubName} (serverId: {serverId}); will retry on the next signal or probe.",
                _hubName, _serverId);
        }
        finally
        {
            Interlocked.Exchange(ref _recovering, 0);
        }

        // A loss that arrived while recovery was running (and was coalesced away by the guard) is picked
        // up here so we don't wait for the next fallback probe.
        if (!_connected)
        {
            _ = Task.Run(RecoverAsync);
        }
    }

    private async Task ResubscribeAsync()
    {
        await _streamSetupLock.WaitAsync();
        try
        {
            await SubscribeStreamsAsync();

            // Re-assert this server's liveness in the directory after the outage (one cheap write).
            await HeartbeatCheck();
        }
        finally
        {
            _streamSetupLock.Release();
        }
    }

    private Task ProcessAllMessage(AllMessage allMessage)
    {
        var allTasks = new List<Task>(_connections.Count);
        var payload = allMessage.Message!;

        foreach (var connection in _connections)
        {
            if (connection.ConnectionAborted.IsCancellationRequested)
                continue;

            if (allMessage.ExcludedIds == null || !allMessage.ExcludedIds.Contains(connection.ConnectionId))
                allTasks.Add(SendLocal(connection, payload));
        }

        return Task.WhenAll(allTasks);
    }

    private Task ProcessServerMessage(ClientMessage clientMessage)
    {
        var connection = _connections[clientMessage.ConnectionId];
        return connection == null ? Task.CompletedTask : SendLocal(connection, clientMessage.Message);
    }

    public override async Task OnConnectedAsync(HubConnectionContext connection)
    {
        await EnsureStreamSetup();

        try
        {
            _connections.Add(connection);

            var client = _clusterClient.GetClientGrain(_hubName, connection.ConnectionId);
            await client.OnConnect(_serverId);

            if (connection!.User!.Identity!.IsAuthenticated)
            {
                var user = _clusterClient.GetUserGrain(_hubName, connection.UserIdentifier!);
                await user.Add(connection.ConnectionId);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "An error has occurred 'OnConnectedAsync' while adding connection {connectionId} [hub: {hubName} (serverId: {serverId})]",
                connection?.ConnectionId, _hubName, _serverId);
            _connections.Remove(connection!);
            throw;
        }
    }

    public override async Task OnDisconnectedAsync(HubConnectionContext connection)
    {
        try
        {
            _logger.LogDebug("Handle disconnection {connectionId} on hub {hubName} (serverId: {serverId})",
                connection.ConnectionId, _hubName, _serverId);
            var client = _clusterClient.GetClientGrain(_hubName, connection.ConnectionId);
            await client.OnDisconnect("hub-disconnect");
        }
        finally
        {
            _connections.Remove(connection);
        }
    }

    public override Task SendAllAsync(string methodName, object?[] args,
        CancellationToken cancellationToken = default)
    {
        var message = new InvocationMessage(methodName, args);
        return _allStream.OnNextAsync(new AllMessage(message));
    }

    public override Task SendAllExceptAsync(string methodName, object?[] args,
        IReadOnlyList<string> excludedConnectionIds,
        CancellationToken cancellationToken = default)
    {
        var message = new InvocationMessage(methodName, args);
        return _allStream.OnNextAsync(new AllMessage(message, excludedConnectionIds));
    }

    public override Task SendConnectionAsync(string connectionId, string methodName, object?[] args,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(connectionId)) throw new ArgumentNullException(nameof(connectionId));
        if (string.IsNullOrWhiteSpace(methodName)) throw new ArgumentNullException(nameof(methodName));

        var message = new InvocationMessage(methodName, args);

        var connection = _connections[connectionId];
        if (connection != null) return SendLocal(connection, message);

        return SendExternal(connectionId, message);
    }

    public override Task SendConnectionsAsync(IReadOnlyList<string> connectionIds, string methodName, object?[] args,
        CancellationToken cancellationToken = default)
    {
        var tasks = connectionIds.Select(c => SendConnectionAsync(c, methodName, args, cancellationToken));
        return Task.WhenAll(tasks);
    }

    public override Task SendGroupAsync(string groupName, string methodName, object?[] args,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(groupName)) throw new ArgumentNullException(nameof(groupName));
        if (string.IsNullOrWhiteSpace(methodName)) throw new ArgumentNullException(nameof(methodName));

        var group = _clusterClient.GetGroupGrain(_hubName, groupName);
        return group.Send(methodName, args);
    }

    public override Task SendGroupsAsync(IReadOnlyList<string> groupNames, string methodName, object?[] args,
        CancellationToken cancellationToken = default)
    {
        var tasks = groupNames.Select(g => SendGroupAsync(g, methodName, args, cancellationToken));
        return Task.WhenAll(tasks);
    }

    public override Task SendGroupExceptAsync(string groupName, string methodName, object?[] args,
        IReadOnlyList<string> excludedConnectionIds,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(groupName)) throw new ArgumentNullException(nameof(groupName));
        if (string.IsNullOrWhiteSpace(methodName)) throw new ArgumentNullException(nameof(methodName));

        var group = _clusterClient.GetGroupGrain(_hubName, groupName);
        return group.SendExcept(methodName, args, excludedConnectionIds);
    }

    public override Task SendUserAsync(string userId, string methodName, object?[] args,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(userId)) throw new ArgumentNullException(nameof(userId));
        if (string.IsNullOrWhiteSpace(methodName)) throw new ArgumentNullException(nameof(methodName));

        var user = _clusterClient.GetUserGrain(_hubName, userId);
        return user.Send(methodName, args);
    }

    public override Task SendUsersAsync(IReadOnlyList<string> userIds, string methodName, object?[] args,
        CancellationToken cancellationToken = default)
    {
        var tasks = userIds.Select(u => SendGroupAsync(u, methodName, args, cancellationToken));
        return Task.WhenAll(tasks);
    }

    public override Task AddToGroupAsync(string connectionId, string groupName,
        CancellationToken cancellationToken = default)
    {
        var group = _clusterClient.GetGroupGrain(_hubName, groupName);
        return group.Add(connectionId);
    }

    public override Task RemoveFromGroupAsync(string connectionId, string groupName,
        CancellationToken cancellationToken = default)
    {
        var group = _clusterClient.GetGroupGrain(_hubName, groupName);
        return group.Remove(connectionId);
    }

    private Task SendLocal(HubConnectionContext connection, InvocationMessage hubMessage)
    {
        _logger.LogDebug(
            "Sending local message to connection {connectionId} on hub {hubName} (serverId: {serverId})",
            connection.ConnectionId, _hubName, _serverId);
        return connection.WriteAsync(hubMessage).AsTask();
    }

    private Task SendExternal(string connectionId, InvocationMessage hubMessage)
    {
        var client = _clusterClient.GetClientGrain(_hubName, connectionId);
        return client.Send(hubMessage);
    }

    public void Dispose()
    {
        _timer?.Dispose();
        _fallbackTimer?.Dispose();

        if (_connectionMonitor is not null)
        {
            _connectionMonitor.ConnectionLost -= OnConnectionLost;
        }

        var toUnsubscribe = new List<Task>();
        if (_serverStream is not null)
        {
            toUnsubscribe.Add(Task.Factory.StartNew(async () =>
            {
                var subscriptions = await _serverStream.GetAllSubscriptionHandles();
                var subs = new List<Task>();
                subs.AddRange(subscriptions.Select(s => s.UnsubscribeAsync()));
                await Task.WhenAll(subs);
            }));
        }

        if (_allStream is not null)
        {
            toUnsubscribe.Add(Task.Factory.StartNew(async () =>
            {
                var subscriptions = await _allStream.GetAllSubscriptionHandles();
                var subs = new List<Task>();
                subs.AddRange(subscriptions.Select(s => s.UnsubscribeAsync()));
                await Task.WhenAll(subs);
            }));
        }

        var serverDirectoryGrain = _clusterClient.GetServerDirectoryGrain();
        toUnsubscribe.Add(serverDirectoryGrain.Unregister(_serverId));

        Task.WhenAll(toUnsubscribe.ToArray()).GetAwaiter().GetResult();
    }

    public void Participate(ISiloLifecycle lifecycle)
    {
        lifecycle.Subscribe(
           observerName: nameof(OrleansHubLifetimeManager<THub>),
           stage: ServiceLifecycleStage.Active,
           onStart: async cts => await Task.Run(EnsureStreamSetup, cts));
    }
}
