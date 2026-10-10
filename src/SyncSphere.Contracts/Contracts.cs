namespace SyncSphere.Contracts;

public sealed record WidgetState(string WidgetId, long Version, string Json);

public sealed record Delta(long Version, string WidgetId, long WidgetVersion, string Json, long PublishedAtUnixMs);

public sealed record DeltaBatch(string DashboardId, long[] Versions, Delta[] Deltas);

public sealed record Snapshot(string DashboardId, long Version, WidgetState[] Widgets);

public sealed record JoinResult(Snapshot? Snapshot, DeltaBatch? Replay, long CurrentVersion, string[] Viewers);

public sealed record ReplayResult(DeltaBatch? Batch, bool NeedSnapshot);

public sealed record UpdateResult(bool Applied, long Version, long WidgetVersion, string? ConflictJson);

public sealed record UpdateWidgetRequest(string Json, long ExpectedWidgetVersion = -1);

public static class HubMethods
{
    public const string Join = "Join";
    public const string Leave = "Leave";
    public const string Replay = "Replay";
    public const string UpdateWidget = "UpdateWidget";
    public const string Heartbeat = "Heartbeat";
    public const string SnapshotRequest = "GetSnapshot";
}

public static class ClientMethods
{
    public const string Deltas = "Deltas";
    public const string PresenceChanged = "PresenceChanged";
}
