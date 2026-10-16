
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using RabbitMQ.Client;
using System.Text;

namespace Mango.MessageBus;

public sealed class RabbitMQMessageProducer : IMessageProducer, IAsyncDisposable
{
    private readonly RabbitMqSettings _settings;
    private readonly ConnectionFactory _connectionFactory;

    // Shared connection reused by all messages sent by this producer.
    private IConnection? _connection;

    // Prevents concurrent calls from creating multiple connections.
    private readonly SemaphoreSlim _connectionLock = new(1, 1);

    private bool _disposed;


    public RabbitMQMessageProducer(IOptions<RabbitMqSettings> options)
    {
        ArgumentNullException.ThrowIfNull(options);

        _settings = options.Value
           ?? throw new InvalidOperationException(
               "RabbitMQ settings are not configured.");

        // Keep the configuration validation in the constructor.

        if (string.IsNullOrWhiteSpace(_settings.HostName))
            throw new InvalidOperationException(
                "RabbitMQ HostName is not configured.");

        if (string.IsNullOrWhiteSpace(_settings.UserName))
            throw new InvalidOperationException(
                "RabbitMQ UserName is not configured.");

        if (string.IsNullOrWhiteSpace(_settings.Password))
            throw new InvalidOperationException(
                "RabbitMQ Password is not configured.");

        if (_settings.Port is < 1 or > 65535)
            throw new InvalidOperationException(
                "RabbitMQ Port must be between 1 and 65535.");

        if (string.IsNullOrWhiteSpace(_settings.VirtualHost))
            throw new InvalidOperationException(
                "RabbitMQ VirtualHost is not configured.");

        // Create the factory once instead of creating it for every message.
        _connectionFactory = new ConnectionFactory
        {
            HostName = _settings.HostName,
            Port = _settings.Port,
            UserName = _settings.UserName,
            Password = _settings.Password,
            VirtualHost = _settings.VirtualHost
        };
    }

    public async Task SendMessage(object message, string queueName)
    {
        ArgumentNullException.ThrowIfNull(message);

        if (string.IsNullOrWhiteSpace(queueName))
            throw new ArgumentException(
                "Queue name cannot be empty.", nameof(queueName));

        // Reuse the shared connection.
        var connection = await GetConnectionAsync();

        // Use a separate channel for this publishing operation.
        // Do not share one channel across concurrent SendMessage calls.

        await using var channel =
            await connection.CreateChannelAsync();

        await channel.QueueDeclareAsync(
            queue: queueName,
            durable: false,
            exclusive: false,
            autoDelete: false,
            arguments: null);

        var json = JsonConvert.SerializeObject(message);
        var body = Encoding.UTF8.GetBytes(json);

        await channel.BasicPublishAsync(
            exchange: "",
            routingKey: queueName,
            mandatory: false,
            body: body);
    }
    private async Task<IConnection> GetConnectionAsync()
    {
        await _connectionLock.WaitAsync();

        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            // Return the existing connection if it is still open.
            if (_connection is { IsOpen: true })
                return _connection;

            // Dispose the old connection if it exists but is no longer open.
            if (_connection is not null)
            {
                await _connection.DisposeAsync();
                _connection = null;
            }

            // Create and retain a replacement connection.
            _connection = await _connectionFactory.CreateConnectionAsync();

            return _connection;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _connectionLock.WaitAsync();

        try
        {
            if (_disposed)
                return;

            _disposed = true;

            if (_connection is not null)
            {
                await _connection.DisposeAsync();
                _connection = null;
            }
        }
        finally
        {
            _connectionLock.Release();
        }

        // Intentionally leave the semaphore undisposed so a concurrent
        // waiter cannot encounter a disposed semaphore during shutdown.
    }
}
