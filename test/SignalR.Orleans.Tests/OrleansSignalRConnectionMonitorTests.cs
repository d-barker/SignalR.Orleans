using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans;
using Xunit;

namespace SignalR.Orleans.Tests
{
    public class OrleansSignalRConnectionMonitorTests
    {
        private static OrleansSignalRConnectionMonitor Create(OrleansSignalRConnectionMonitorOptions? options = null)
            => new(
                Options.Create(options ?? new OrleansSignalRConnectionMonitorOptions { RetryDelay = TimeSpan.Zero }),
                NullLogger<OrleansSignalRConnectionMonitor>.Instance);

        [Fact]
        public async Task ShouldRetryConnectionAttempt_RaisesConnectionLost()
        {
            var monitor = Create();
            var raised = 0;
            monitor.ConnectionLost += () => Interlocked.Increment(ref raised);

            await monitor.ShouldRetryConnectionAttempt(new Exception("boom"), CancellationToken.None);

            Assert.Equal(1, raised);
        }

        [Fact]
        public async Task ShouldRetryConnectionAttempt_RetriesIndefinitely_ByDefault()
        {
            var monitor = Create();

            for (var i = 0; i < 5; i++)
            {
                Assert.True(await monitor.ShouldRetryConnectionAttempt(new Exception(), CancellationToken.None));
            }
        }

        [Fact]
        public async Task ShouldRetryConnectionAttempt_StopsAfterMaxAttempts()
        {
            var monitor = Create(new OrleansSignalRConnectionMonitorOptions { RetryDelay = TimeSpan.Zero, MaxRetryAttempts = 3 });

            Assert.True(await monitor.ShouldRetryConnectionAttempt(new Exception(), CancellationToken.None));   // 1
            Assert.True(await monitor.ShouldRetryConnectionAttempt(new Exception(), CancellationToken.None));   // 2
            Assert.False(await monitor.ShouldRetryConnectionAttempt(new Exception(), CancellationToken.None));  // 3 -> give up
        }

        [Fact]
        public async Task NotifyReconnected_ResetsTheAttemptCounter()
        {
            var monitor = Create(new OrleansSignalRConnectionMonitorOptions { RetryDelay = TimeSpan.Zero, MaxRetryAttempts = 2 });

            Assert.True(await monitor.ShouldRetryConnectionAttempt(new Exception(), CancellationToken.None));   // 1
            Assert.False(await monitor.ShouldRetryConnectionAttempt(new Exception(), CancellationToken.None));  // 2 -> give up

            monitor.NotifyReconnected();

            // Counter reset, so the monitor keeps trying again for the next outage.
            Assert.True(await monitor.ShouldRetryConnectionAttempt(new Exception(), CancellationToken.None));
        }

        [Fact]
        public async Task ShouldRetryConnectionAttempt_DelegatesDecisionToInnerFilter_AndStillRaisesSignal()
        {
            var inner = new FakeRetryFilter(returnValue: false);
            var monitor = Create(new OrleansSignalRConnectionMonitorOptions { InnerRetryFilter = inner });
            var raised = 0;
            monitor.ConnectionLost += () => Interlocked.Increment(ref raised);

            var result = await monitor.ShouldRetryConnectionAttempt(new Exception(), CancellationToken.None);

            Assert.False(result);        // decision came from the inner policy
            Assert.Equal(1, inner.Calls);
            Assert.Equal(1, raised);     // the loss signal is still emitted
        }

        [Fact]
        public async Task ShouldRetryConnectionAttempt_SwallowsThrowingHandler()
        {
            var monitor = Create();
            monitor.ConnectionLost += () => throw new InvalidOperationException("handler blew up");

            // A throwing handler must not break the Orleans reconnect loop.
            var result = await monitor.ShouldRetryConnectionAttempt(new Exception(), CancellationToken.None);

            Assert.True(result);
        }

        private sealed class FakeRetryFilter : IClientConnectionRetryFilter
        {
            private readonly bool _returnValue;

            public FakeRetryFilter(bool returnValue) => _returnValue = returnValue;

            public int Calls { get; private set; }

            public Task<bool> ShouldRetryConnectionAttempt(Exception exception, CancellationToken cancellationToken)
            {
                Calls++;
                return Task.FromResult(_returnValue);
            }
        }
    }
}
