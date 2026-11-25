using Npgsql;
using Testcontainers.PostgreSql;
using Xunit;

namespace Winche.Realtime.Tests;

/// <summary>One Postgres container for the run; each test takes a database of its own.</summary>
public sealed class PostgresFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:17-alpine").WithCommand("-c", "max_connections=500").Build();   // each test its own database and pool

    public async Task InitializeAsync() => await _container.StartAsync();
    public async Task DisposeAsync() => await _container.DisposeAsync();

    /// <summary>A new, empty database's connection string.</summary>
    public async Task<string> NewDatabaseAsync()
    {
        var name = "t" + Guid.NewGuid().ToString("N");
        await using (var conn = new NpgsqlConnection(_container.GetConnectionString()))
        {
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand($"CREATE DATABASE {name}", conn);
            await cmd.ExecuteNonQueryAsync();
        }
        return new NpgsqlConnectionStringBuilder(_container.GetConnectionString()) { Database = name }.ConnectionString;
    }
}

[CollectionDefinition("postgres")]
public sealed class PostgresCollection : ICollectionFixture<PostgresFixture>;
