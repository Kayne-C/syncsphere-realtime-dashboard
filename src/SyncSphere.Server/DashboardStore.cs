using Microsoft.Extensions.Options;
using StackExchange.Redis;
using SyncSphere.Contracts;

namespace SyncSphere.Server;

public enum ApplyStatus
{
    Conflict = 0,
    Applied = 1,
    TooManyWidgets = 2,
}

public sealed record ApplyResult(ApplyStatus Status, long Version, long WidgetVersion, string? CurrentJson);

public sealed class DashboardStore(IConnectionMultiplexer redis, IOptions<SyncOptions> options)
{
    private const string ApplyScript = """
        local cur = redis.call('HGET', @widgets, @widget)
        local cw = 0
        local cj = ''
        if cur then
          local sep = string.find(cur, '|', 1, true)
          cw = tonumber(string.sub(cur, 1, sep - 1))
          cj = string.sub(cur, sep + 1)
        end
        local expected = tonumber(@expected)
        if expected >= 0 and expected ~= cw then
          return {0, 0, cw, cj}
        end
        if (not cur) and redis.call('HLEN', @widgets) >= tonumber(@maxwidgets) then
          return {2, 0, 0, ''}
        end
        local ver = redis.call('INCR', @ver)
        local wv = cw + 1
        redis.call('HSET', @widgets, @widget, wv .. '|' .. @json)
        redis.call('XADD', @log, 'MAXLEN', @maxlen, ver .. '-0', 'w', @widget, 'wv', wv, 'j', @json, 't', @now)
        return {1, ver, wv, ''}
        """;

    private const string SnapshotScript = """
        local v = redis.call('GET', @ver)
        if not v then v = '0' end
        local all = redis.call('HGETALL', @widgets)
        return {v, all}
        """;

    private const string ReplayScript = """
        local cur = redis.call('GET', @ver)
        if not cur then cur = '0' end
        cur = tonumber(cur)
        local from = tonumber(@from)
        local max = tonumber(@max)
        if from >= cur then
          return {1, cur, {}}
        end
        local first = redis.call('XRANGE', @log, '-', '+', 'COUNT', 1)
        if #first == 0 then
          return {0, cur, {}}
        end
        local oldest = tonumber(string.match(first[1][1], '^(%d+)'))
        if from + 1 < oldest or cur - from > max then
          return {0, cur, {}}
        end
        local entries = redis.call('XRANGE', @log, (from + 1) .. '-0', '+', 'COUNT', max)
        return {1, cur, entries}
        """;

    private static readonly LuaScript Apply = LuaScript.Prepare(ApplyScript);
    private static readonly LuaScript SnapshotLua = LuaScript.Prepare(SnapshotScript);
    private static readonly LuaScript ReplayLua = LuaScript.Prepare(ReplayScript);

    public static string Tag(string tenant, string dashboard) => $"{{{tenant}:{dashboard}}}";

    private static RedisKey Widgets(string tenant, string dashboard) => $"ss:{Tag(tenant, dashboard)}:widgets";

    private static RedisKey Version(string tenant, string dashboard) => $"ss:{Tag(tenant, dashboard)}:ver";

    private static RedisKey Log(string tenant, string dashboard) => $"ss:{Tag(tenant, dashboard)}:log";

    public async Task<ApplyResult> ApplyAsync(string tenant, string dashboard, string widgetId, string json, long expectedWidgetVersion, long nowMs)
    {
        var db = redis.GetDatabase();
        var result = (RedisResult[])(await Apply.EvaluateAsync(db, new
        {
            widgets = Widgets(tenant, dashboard),
            ver = Version(tenant, dashboard),
            log = Log(tenant, dashboard),
            widget = widgetId,
            json,
            expected = expectedWidgetVersion,
            now = nowMs,
            maxlen = options.Value.ReplayRetention,
            maxwidgets = options.Value.MaxWidgetsPerDashboard,
        }))!;

        return new ApplyResult((ApplyStatus)(int)result[0], (long)result[1], (long)result[2], (string?)result[3]);
    }

    public async Task<long> HeadVersionAsync(string tenant, string dashboard)
    {
        var value = await redis.GetDatabase().StringGetAsync(Version(tenant, dashboard));
        return value.HasValue ? (long)value : 0;
    }

    public async Task<Snapshot> SnapshotAsync(string tenant, string dashboard)
    {
        var db = redis.GetDatabase();
        var result = (RedisResult[])(await SnapshotLua.EvaluateAsync(db, new { ver = Version(tenant, dashboard), widgets = Widgets(tenant, dashboard) }))!;
        var version = long.Parse((string)result[0]!);
        var flat = (RedisResult[])result[1]!;
        var widgets = new List<WidgetState>(flat.Length / 2);
        for (var i = 0; i + 1 < flat.Length; i += 2)
        {
            var value = (string)flat[i + 1]!;
            var sep = value.IndexOf('|');
            widgets.Add(new WidgetState((string)flat[i]!, long.Parse(value[..sep]), value[(sep + 1)..]));
        }

        return new Snapshot(dashboard, version, [.. widgets.OrderBy(w => w.WidgetId, StringComparer.Ordinal)]);
    }

    public async Task<ReplayResult> ReplayAsync(string tenant, string dashboard, long fromVersionExclusive)
    {
        var db = redis.GetDatabase();
        var result = (RedisResult[])(await ReplayLua.EvaluateAsync(db, new
        {
            ver = Version(tenant, dashboard),
            log = Log(tenant, dashboard),
            from = fromVersionExclusive,
            max = options.Value.MaxReplay,
        }))!;

        if ((long)result[0] == 0)
        {
            return new ReplayResult(null, NeedSnapshot: true);
        }

        var entries = (RedisResult[])result[2]!;
        var versions = new List<long>(entries.Length);
        var latest = new Dictionary<string, Delta>();
        foreach (var entryResult in entries)
        {
            var entry = (RedisResult[])entryResult!;
            var version = long.Parse(((string)entry[0]!).Split('-')[0]);
            var fields = (RedisResult[])entry[1]!;
            string widget = "", json = "";
            long wv = 0, at = 0;
            for (var i = 0; i + 1 < fields.Length; i += 2)
            {
                var name = (string)fields[i]!;
                var value = (string)fields[i + 1]!;
                switch (name)
                {
                    case "w": widget = value; break;
                    case "wv": wv = long.Parse(value); break;
                    case "j": json = value; break;
                    case "t": at = long.Parse(value); break;
                }
            }

            versions.Add(version);
            latest[widget] = new Delta(version, widget, wv, json, at);
        }

        return new ReplayResult(new DeltaBatch(dashboard, [.. versions], [.. latest.Values]), NeedSnapshot: false);
    }
}
