using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Options;
using SyncSphere.Contracts;

namespace SyncSphere.Server;

public interface ISyncFaults
{
    bool DropBatch(DeltaBatch batch);
}

public sealed class NoSyncFaults : ISyncFaults
{
    public bool DropBatch(DeltaBatch batch) => false;
}

public sealed class SyncStats
{
    public static readonly Meter Meter = new("SyncSphere");
    private static readonly Counter<long> DeltasMetric = Meter.CreateCounter<long>("syncsphere.deltas");
    private static readonly Counter<long> BatchesMetric = Meter.CreateCounter<long>("syncsphere.batches");

    private long _deltas;
    private long _batches;
    private long _sentDeltas;
    private long _connections;
    private long _replays;
    private long _dropped;

    public long Deltas => Interlocked.Read(ref _deltas);
    public long Batches => Interlocked.Read(ref _batches);
    public long SentDeltas => Interlocked.Read(ref _sentDeltas);
    public long Connections => Interlocked.Read(ref _connections);
    public long Replays => Interlocked.Read(ref _replays);
    public long Dropped => Interlocked.Read(ref _dropped);

    public void Applied()
    {
        Interlocked.Increment(ref _deltas);
        DeltasMetric.Add(1);
    }

    public void Sent(int deltas)
    {
        Interlocked.Increment(ref _batches);
        Interlocked.Add(ref _sentDeltas, deltas);
        BatchesMetric.Add(1);
    }

    public void ConnectionOpened() => Interlocked.Increment(ref _connections);

    public void ConnectionClosed() => Interlocked.Decrement(ref _connections);

    public void Replayed() => Interlocked.Increment(ref _replays);

    public void BatchDropped() => Interlocked.Increment(ref _dropped);
}

public sealed class DeltaBroadcaster(IHubContext<DashboardHub> hub, IOptions<SyncOptions> options, ISyncFaults faults, SyncStats stats)
{
    private sealed class Pending(string group, string dashboard)
    {
        public readonly object Gate = new();
        public readonly List<Delta> Deltas = [];
        public bool Scheduled;
        public string Group { get; } = group;
        public string Dashboard { get; } = dashboard;
    }

    private readonly ConcurrentDictionary<string, Pending> _pending = new();

    public static string GroupName(string tenant, string dashboard) => $"{tenant}/{dashboard}";

    public void Enqueue(string tenant, string dashboard, Delta delta)
    {
        var group = GroupName(tenant, dashboard);
        var window = options.Value.ConflationWindowMs;
        if (window <= 0)
        {
            _ = SendAsync(group, dashboard, [delta]);
            return;
        }

        var pending = _pending.GetOrAdd(group, g => new Pending(g, dashboard));
        bool schedule;
        lock (pending.Gate)
        {
            pending.Deltas.Add(delta);
            schedule = !pending.Scheduled;
            pending.Scheduled = true;
        }

        if (schedule)
        {
            _ = Task.Delay(window).ContinueWith(_ => Flush(pending), TaskScheduler.Default);
        }
    }

    private void Flush(Pending pending)
    {
        List<Delta> batch;
        lock (pending.Gate)
        {
            batch = [.. pending.Deltas];
            pending.Deltas.Clear();
            pending.Scheduled = false;
        }

        if (batch.Count > 0)
        {
            _ = SendAsync(pending.Group, pending.Dashboard, batch);
        }
    }

    private async Task SendAsync(string group, string dashboard, List<Delta> deltas)
    {
        var latest = new Dictionary<string, Delta>();
        foreach (var d in deltas)
        {
            if (!latest.TryGetValue(d.WidgetId, out var existing) || d.WidgetVersion > existing.WidgetVersion)
            {
                latest[d.WidgetId] = d;
            }
        }

        var batch = new DeltaBatch(dashboard, [.. deltas.Select(d => d.Version).Order()], [.. latest.Values]);
        if (faults.DropBatch(batch))
        {
            stats.BatchDropped();
            return;
        }

        try
        {
            await hub.Clients.Group(group).SendAsync(ClientMethods.Deltas, batch);
            stats.Sent(batch.Deltas.Length);
        }
        catch (Exception)
        {
            stats.BatchDropped();
        }
    }
}
