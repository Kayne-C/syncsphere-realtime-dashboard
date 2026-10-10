namespace SyncSphere.Server;

public sealed class SyncOptions
{
    public const string Section = "SyncSphere";

    public string RedisConnectionString { get; set; } = "";
    public int ConflationWindowMs { get; set; } = 25;
    public int ReplayRetention { get; set; } = 10_000;
    public int MaxReplay { get; set; } = 5_000;
    public int MaxWidgetBytes { get; set; } = 16 * 1024;
    public int MaxWidgetsPerDashboard { get; set; } = 500;
    public int PresenceTtlSeconds { get; set; } = 30;
    public int MaxDashboardsPerConnection { get; set; } = 20;
}

public sealed class JwtOptions
{
    public const string Section = "SyncSphere:Jwt";

    public string? Authority { get; set; }
    public string? Audience { get; set; }
    public string? Issuer { get; set; }
    public string? SigningKey { get; set; }
}
