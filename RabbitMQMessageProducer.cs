
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using RabbitMQ.Client;
using System.Text;

namespace Mango.MessageBus;

public sealed class RabbitMQMessageProducer : IMessageProducer, IAsyncDisposable
{
    private readonly ConnectionFactory _connectionFactory;

    // Reuse one connection for this producer instance instead of opening
    // a new TCP connection for every message.
    private IConnection? _connection;

    // Coordinates connection creation, publishing, and shutdown.
    // This prevents the shared connection from being disposed during a send.
    //
    // Trade-off: sends are serialized. A future throughput improvement could
    // allow concurrent sends while separately tracking active operations.
    private readonly SemaphoreSlim _lifecycleLock = new(1, 1);

    private bool _disposed;

    public RabbitMQMessageProducer(IOptions<RabbitMqSettings> options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var settings = options.Value
            ?? throw new InvalidOperationException(
                "RabbitMQ settings are not configured.");

        // Validate configuration early to provide clear startup errors.
        if (string.IsNullOrWhiteSpace(settings.HostName))
            throw new InvalidOperationException(
                "RabbitMQ HostName is not configured.");

        if (string.IsNullOrWhiteSpace(settings.UserName))
            throw new InvalidOperationException(
                "RabbitMQ UserName is not configured.");

        if (string.IsNullOrWhiteSpace(settings.Password))
            throw new InvalidOperationException(
                "RabbitMQ Password is not configured.");

        if (settings.Port is < 1 or > 65535)
            throw new InvalidOperationException(
                "RabbitMQ Port must be between 1 and 65535.");

        if (string.IsNullOrWhiteSpace(settings.VirtualHost))
            throw new InvalidOperationException(
                "RabbitMQ VirtualHost is not configured.");

        // Keep one factory and use it whenever a connection is needed.
        _connectionFactory = new ConnectionFactory
        {
            HostName = settings.HostName,
            Port = settings.Port,
            UserName = settings.UserName,
            Password = settings.Password,
            VirtualHost = settings.VirtualHost
        };
    }

    public async Task SendMessage(object message, string queueName)
    {
        ArgumentNullException.ThrowIfNull(message);

        if (string.IsNullOrWhiteSpace(queueName))
        {
            throw new ArgumentException(
                "Queue name cannot be empty.",
                nameof(queueName));
        }

        await _lifecycleLock.WaitAsync();

        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            // Reuse the connection or create a replacement if it has closed.
            var connection = await GetConnectionAsync();

            // Enable publisher confirms and confirmation tracking.
            // With RabbitMQ.Client 7.x, tracked publishing reports broker
            // nacks and mandatory returns through the publish operation.
            //
            // Confirm that your installed RabbitMQ.Client version supports
            // these CreateChannelOptions settings.
            var channelOptions = new CreateChannelOptions(
                publisherConfirmationsEnabled: true,
                publisherConfirmationTrackingEnabled: true);

            // Channels are lightweight. Create and dispose one per send.
            await using var channel =
                await connection.CreateChannelAsync(channelOptions);

            // Durable queues survive broker restarts.
            //
            // Every producer and consumer declaring this queue must use
            // compatible properties. Existing non-durable queues must be
            // migrated or recreated safely before this declaration is used.

            if (queueName == "rewardordercreated")
            {
                const string deadLetterExchange = "reward.dlx";
                const string deadLetterQueue = "rewardordercreated.dlq";
                const string deadLetterRoutingKey = "rewardordercreated";

                await channel.ExchangeDeclareAsync(
                    exchange: deadLetterExchange,
                    type: ExchangeType.Direct,
                    durable: true,
                    autoDelete: false,
                    arguments: null);

                await channel.QueueDeclareAsync(
                    queue: deadLetterQueue,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);

                await channel.QueueBindAsync(
                    queue: deadLetterQueue,
                    exchange: deadLetterExchange,
                    routingKey: deadLetterRoutingKey);

                var queueArguments = new Dictionary<string, object?>
                {
                    ["x-dead-letter-exchange"] = deadLetterExchange,
                    ["x-dead-letter-routing-key"] = deadLetterRoutingKey
                };

                await channel.QueueDeclareAsync(
                    queue: queueName,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: queueArguments);
            }
            else
            {
                await channel.QueueDeclareAsync(
                    queue: queueName,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);
            }


            // Serialize the message and encode it as UTF-8.
            var json = JsonConvert.SerializeObject(message);
            var body = Encoding.UTF8.GetBytes(json);

            // Persistent delivery works together with a durable queue to
            // improve message survival across broker restarts.
            var properties = new BasicProperties
            {
                Persistent = true,
                MessageId = Guid.NewGuid().ToString()
            };

            try
            {
                // Use the default exchange to route directly to the queue
                // whose name matches the routing key.
                //
                // mandatory: true requests that unroutable messages be
                // returned instead of silently discarded.
                //
                // Publisher confirmation tracking makes broker nacks and
                // returned messages observable through the publish task.
                // A successful confirmation means broker acceptance, not
                // successful processing by EmailAPI or RewardAPI.
                await channel.BasicPublishAsync(
                    exchange: "",
                    routingKey: queueName,
                    mandatory: true,
                    basicProperties: properties,
                    body: body);
            }
            catch (Exception ex)
            {
                // Preserve the original exception so the caller can log it
                // and decide how to handle the failed publishing operation.
                //
                // The message may have reached the broker before a connection
                // failure was observed. Avoid blind retries unless consumers
                // can safely handle duplicate deliveries.
                throw new InvalidOperationException(
                    $"Publishing to RabbitMQ queue '{queueName}' failed. " +
                    "The message may or may not have been accepted by the broker.",
                    ex);
            }
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }

    // The caller must hold _lifecycleLock before calling this method.
    private async Task<IConnection> GetConnectionAsync()
    {
        if (_connection is { IsOpen: true })
            return _connection;

        // Dispose a previous connection that is no longer usable.
        if (_connection is not null)
        {
            await _connection.DisposeAsync();
            _connection = null;
        }

        // Connect lazily on the first send, not in the constructor.
        // A subsequent send can create a replacement after a detected closure.
        _connection = await _connectionFactory.CreateConnectionAsync();

        return _connection;
    }

    public async ValueTask DisposeAsync()
    {
        await _lifecycleLock.WaitAsync();

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
            _lifecycleLock.Release();
        }

        // Leave the semaphore undisposed so callers already waiting on it
        // can enter and observe the disposed state.
    }
}
