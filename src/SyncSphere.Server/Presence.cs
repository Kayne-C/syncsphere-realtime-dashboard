using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace SyncSphere.Server;

public sealed class PresenceStore(IConnectionMultiplexer redis, IOptions<SyncOptions> options, TimeProvider time)
{
    private static RedisKey Key(string tenant, string dashboard) => $"ss:{DashboardStore.Tag(tenant, dashboard)}:presence";

    private double Expiry() => time.GetUtcNow().ToUnixTimeMilliseconds() + options.Value.PresenceTtlSeconds * 1000.0;

    public async Task<string[]> JoinAsync(string tenant, string dashboard, string connectionId, string user)
    {
        var db = redis.GetDatabase();
        await db.SortedSetAddAsync(Key(tenant, dashboard), $"{connectionId}|{user}", Expiry());
        return await ViewersAsync(tenant, dashboard);
    }

    public async Task<string[]> LeaveAsync(string tenant, string dashboard, string connectionId, string user)
    {
        var db = redis.GetDatabase();
        await db.SortedSetRemoveAsync(Key(tenant, dashboard), $"{connectionId}|{user}");
        return await ViewersAsync(tenant, dashboard);
    }

    public async Task RefreshAsync(string tenant, string dashboard, string connectionId, string user)
    {
        var db = redis.GetDatabase();
        await db.SortedSetAddAsync(Key(tenant, dashboard), $"{connectionId}|{user}", Expiry(), When.Exists);
    }

    public async Task<string[]> ViewersAsync(string tenant, string dashboard)
    {
        var db = redis.GetDatabase();
        var key = Key(tenant, dashboard);
        await db.SortedSetRemoveRangeByScoreAsync(key, double.NegativeInfinity, time.GetUtcNow().ToUnixTimeMilliseconds());
        var members = await db.SortedSetRangeByRankAsync(key);
        return [.. members.Select(m => { var text = (string)m!; return text[(text.IndexOf('|') + 1)..]; }).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal)];
    }
}
