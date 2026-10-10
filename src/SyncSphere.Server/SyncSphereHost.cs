using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;
using StackExchange.Redis;
using SyncSphere.Contracts;

namespace SyncSphere.Server;

public static class SyncSphereHost
{
    public const string HubPath = "/hubs/dashboard";
    public const string IngestPolicy = "ingest";

    public static WebApplication Build(string[] args, Action<WebApplicationBuilder>? configure = null, Action<IServiceCollection>? overrides = null)
    {
        var builder = WebApplication.CreateBuilder(args);
        configure?.Invoke(builder);

        var config = builder.Configuration;
        var sync = config.GetSection(SyncOptions.Section).Get<SyncOptions>() ?? new SyncOptions();
        if (string.IsNullOrWhiteSpace(sync.RedisConnectionString))
        {
            throw new InvalidOperationException($"{SyncOptions.Section}:RedisConnectionString is required.");
        }

        builder.Services.Configure<SyncOptions>(config.GetSection(SyncOptions.Section));
        builder.Services.AddSingleton(TimeProvider.System);
        var redisOptions = ConfigurationOptions.Parse(sync.RedisConnectionString);
        redisOptions.AbortOnConnectFail = false;
        builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisOptions));
        builder.Services.AddSingleton<DashboardStore>();
        builder.Services.AddSingleton<PresenceStore>();
        builder.Services.AddSingleton<DashboardService>();
        builder.Services.AddSingleton<DeltaBroadcaster>();
        builder.Services.AddSingleton<SyncStats>();
        builder.Services.AddSingleton<ISyncFaults, NoSyncFaults>();

        builder.Services.AddSignalR(o => o.MaximumReceiveMessageSize = 64 * 1024)
            .AddStackExchangeRedis(o =>
            {
                o.Configuration = ConfigurationOptions.Parse(sync.RedisConnectionString);
                o.Configuration.AbortOnConnectFail = false;
                o.Configuration.ChannelPrefix = RedisChannel.Literal("syncsphere");
            });

        AddSecurity(builder.Services, config, builder.Environment);
        builder.Services.AddHealthChecks();

        overrides?.Invoke(builder.Services);

        var app = builder.Build();
        _ = app.Services.GetRequiredService<IConnectionMultiplexer>();
        app.UseAuthentication();
        app.UseAuthorization();
        MapEndpoints(app);
        return app;
    }

    private static void AddSecurity(IServiceCollection services, IConfiguration config, IHostEnvironment env)
    {
        var jwt = config.GetSection(JwtOptions.Section).Get<JwtOptions>() ?? new JwtOptions();
        if (string.IsNullOrWhiteSpace(jwt.Authority) && string.IsNullOrWhiteSpace(jwt.SigningKey))
        {
            throw new InvalidOperationException($"Configure {JwtOptions.Section}:Authority or {JwtOptions.Section}:SigningKey (development only).");
        }

        if (jwt.SigningKey is { Length: < 32 })
        {
            throw new InvalidOperationException($"{JwtOptions.Section}:SigningKey must be at least 32 characters.");
        }

        if (!env.IsDevelopment() && string.IsNullOrWhiteSpace(jwt.Authority))
        {
            throw new InvalidOperationException("A symmetric SigningKey is only allowed in Development; configure an Authority.");
        }

        services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
            .AddJwtBearer(o =>
            {
                o.MapInboundClaims = false;
                if (!string.IsNullOrWhiteSpace(jwt.Authority))
                {
                    o.Authority = jwt.Authority;
                }

                o.TokenValidationParameters = new TokenValidationParameters
                {
                    NameClaimType = "sub",
                    RoleClaimType = "role",
                    ValidateIssuer = !string.IsNullOrWhiteSpace(jwt.Issuer) || !string.IsNullOrWhiteSpace(jwt.Authority),
                    ValidIssuer = jwt.Issuer,
                    ValidateAudience = !string.IsNullOrWhiteSpace(jwt.Audience),
                    ValidAudience = jwt.Audience,
                    ValidateLifetime = true,
                    ClockSkew = TimeSpan.FromSeconds(30),
                    ValidAlgorithms = string.IsNullOrWhiteSpace(jwt.SigningKey) ? null : [SecurityAlgorithms.HmacSha256],
                    IssuerSigningKey = string.IsNullOrWhiteSpace(jwt.SigningKey) ? null : new SymmetricSecurityKey(Encoding.UTF8.GetBytes(jwt.SigningKey)),
                };

                o.Events = new JwtBearerEvents
                {
                    OnMessageReceived = ctx =>
                    {
                        var token = ctx.Request.Query["access_token"];
                        if (!string.IsNullOrEmpty(token) && ctx.HttpContext.Request.Path.StartsWithSegments(HubPath))
                        {
                            ctx.Token = token;
                        }

                        return Task.CompletedTask;
                    },
                };
            });

        services.AddAuthorization(o =>
        {
            o.FallbackPolicy = new AuthorizationPolicyBuilder().RequireAuthenticatedUser().RequireClaim("tenant").Build();
            o.AddPolicy(IngestPolicy, p => p.RequireAuthenticatedUser().RequireClaim("tenant").RequireRole("ingest"));
        });
    }

    private static void MapEndpoints(WebApplication app)
    {
        app.MapHealthChecks("/healthz").AllowAnonymous();
        app.MapHub<DashboardHub>(HubPath);

        app.MapPut("/api/dashboards/{dashboard}/widgets/{widget}", async (
            string dashboard,
            string widget,
            UpdateWidgetRequest request,
            HttpContext ctx,
            DashboardService service) =>
        {
            var tenant = ctx.User.FindFirstValue("tenant")!;
            try
            {
                var result = await service.ApplyAsync(tenant, dashboard, widget, request);
                return result.Applied ? Results.Ok(result) : Results.Json(result, statusCode: StatusCodes.Status409Conflict);
            }
            catch (HubException ex)
            {
                return Results.Problem(ex.Message, statusCode: StatusCodes.Status400BadRequest);
            }
        }).RequireAuthorization(IngestPolicy);

        app.MapGet("/api/dashboards/{dashboard}", async (string dashboard, HttpContext ctx, DashboardStore store) =>
            DashboardService.ValidId(dashboard)
                ? Results.Ok(await store.SnapshotAsync(ctx.User.FindFirstValue("tenant")!, dashboard))
                : Results.BadRequest());

        app.MapGet("/stats", (SyncStats stats) => new
        {
            stats.Connections,
            stats.Deltas,
            stats.Batches,
            stats.SentDeltas,
            stats.Replays,
            stats.Dropped,
            conflation = stats.SentDeltas == 0 ? 1.0 : Math.Round((double)stats.Deltas / Math.Max(1, stats.SentDeltas), 2),
        }).AllowAnonymous();

        if (app.Environment.IsDevelopment())
        {
            app.MapGet("/dev/token", (string sub, string tenant, string? role, IConfiguration config) =>
            {
                var jwt = config.GetSection(JwtOptions.Section).Get<JwtOptions>();
                if (string.IsNullOrWhiteSpace(jwt?.SigningKey))
                {
                    return Results.Problem("No SigningKey configured.");
                }

                var claims = new List<Claim> { new("sub", sub), new("tenant", tenant) };
                foreach (var r in (role ?? string.Empty).Split(',', StringSplitOptions.RemoveEmptyEntries))
                {
                    claims.Add(new Claim("role", r));
                }

                var token = new JwtSecurityTokenHandler { SetDefaultTimesOnTokenCreation = false }.WriteToken(new JwtSecurityToken(
                    jwt.Issuer,
                    jwt.Audience,
                    claims,
                    expires: DateTime.UtcNow.AddHours(1),
                    signingCredentials: new SigningCredentials(new SymmetricSecurityKey(Encoding.UTF8.GetBytes(jwt.SigningKey)), SecurityAlgorithms.HmacSha256)));
                return Results.Ok(new { token });
            }).AllowAnonymous();
        }
    }
}
