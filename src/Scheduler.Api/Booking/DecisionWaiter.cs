using System.Collections.Concurrent;
using Npgsql;
using Scheduler.Infrastructure;

namespace Scheduler.Api.Booking;

/// <summary>
/// Lets a POST wait for its own decision. Workers NOTIFY request ids when their batch commits; one LISTEN
/// connection per API instance wakes the matching waiters. A missed notification only costs the wait:
/// the endpoint reads the database once more before falling back to 202.
/// </summary>
public sealed class DecisionWaiter
{
    private readonly ConcurrentDictionary<Guid, TaskCompletionSource> _waiting = new();

    /// <summary>Register before publishing the command, so a fast decision can't be missed.</summary>
    public Registration Register(Guid requestId)
    {
        var signal = _waiting.GetOrAdd(requestId, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
        return new Registration(this, requestId, signal);
    }

    public void OnNotification(string payload)
    {
        foreach (var part in payload.Split(',', StringSplitOptions.RemoveEmptyEntries))
            if (Guid.TryParse(part, out var id) && _waiting.TryRemove(id, out var signal))
                signal.TrySetResult();
    }

    public readonly struct Registration(DecisionWaiter owner, Guid requestId, TaskCompletionSource signal) : IDisposable
    {
        /// <summary>True if the decision was signalled within <paramref name="timeout"/>.</summary>
        public async Task<bool> WaitAsync(TimeSpan timeout, CancellationToken ct)
        {
            if (timeout <= TimeSpan.Zero) return signal.Task.IsCompleted;
            try
            {
                await signal.Task.WaitAsync(timeout, ct);
                return true;
            }
            catch (TimeoutException)
            {
                return false;
            }
        }

        public void Dispose() => owner._waiting.TryRemove(new KeyValuePair<Guid, TaskCompletionSource>(requestId, signal));
    }
}

public sealed class DecisionListener(NpgsqlDataSource db, DecisionWaiter waiter, ILogger<DecisionListener> log) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken ct) =>
        PgListener.RunAsync(db, PgChannels.BookingDecided, waiter.OnNotification, log, ct);
}
