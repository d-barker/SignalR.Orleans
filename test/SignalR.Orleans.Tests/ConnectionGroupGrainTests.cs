using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;
using Orleans.Storage;
using SignalR.Orleans;
using SignalR.Orleans.ConnectionGroups;
using Xunit;

namespace SignalR.Orleans.Tests;

public class ConnectionGroupGrainTests : IClassFixture<OrleansFixture>
{
    private const string HubName = "MyHub";

    private readonly OrleansFixture _fixture;

    public ConnectionGroupGrainTests(OrleansFixture fixture)
    {
        _fixture = fixture;
    }

    /// <summary>
    /// Reproduces the scenario where a group grain's state contains a connection id, but the
    /// client-disconnect stream has no persisted subscription handle (e.g. the handle was lost while
    /// the connection id survived in state). On activation the grain must re-subscribe to the
    /// disconnect stream, otherwise the stale connection id can never be cleaned up.
    /// </summary>
    [Fact]
    public async Task OnActivate_WhenNoSubscriptionHandleExists_ReSubscribesSoDisconnectCleansUp()
    {
        var groupName = $"resub-{Guid.NewGuid():N}";
        var connectionId = $"conn-{Guid.NewGuid():N}";

        var grain = _fixture.Client.GetGroupGrain(HubName, groupName);

        // Seed the grain's persisted state with a connection id WITHOUT ever subscribing to the
        // client-disconnect stream. This mimics state that outlived its stream subscription handle.
        var storage = _fixture.Silo.Services.GetRequiredKeyedService<IGrainStorage>(
            SignalROrleansConstants.SIGNALR_ORLEANS_STORAGE_PROVIDER);

        var seededState = new GrainState<ConnectionGroupGrainState>(
            new ConnectionGroupGrainState { ConnectionIds = { connectionId } });

        await storage.WriteStateAsync("ConnectionGroups", grain.GetGrainId(), seededState);

        // Activating the grain (any call) loads the seeded state and runs OnActivateAsync.
        Assert.Equal(1, await grain.Count());

        // Simulate the client disconnecting by publishing to the client-disconnect stream, exactly
        // as ClientGrain does on disconnect.
        await _fixture.Client.GetOrleansSignalRStreamProvider()
            .GetClientDisconnectionStream(connectionId)
            .OnNextAsync(connectionId);

        // If the grain re-subscribed on activation, the disconnect event removes the connection id.
        await WaitForCountAsync(grain, expected: 0);
    }

    private static async Task WaitForCountAsync(IConnectionGroupGrain grain, int expected)
    {
        var sw = Stopwatch.StartNew();
        while (await grain.Count() != expected)
        {
            if (sw.ElapsedMilliseconds > 5000)
            {
                throw new Exception(
                    $"Group count did not reach {expected} within the timeout; the grain did not re-subscribe to the client-disconnect stream on activation.");
            }

            await Task.Delay(25);
        }
    }
}
