using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;

using RabbitMQ.Client;

using System.Text;
using System.Text.Json.Serialization;

namespace Mango.MessageBus
{
    public class RabbitMQMessageProducer : IMessageProducer
    {
        private readonly string _hostName;
        private readonly string _userName;
        private readonly string _password;
        private readonly int _port;
        private IConnection _connection;

        public RabbitMQMessageProducer(IConfiguration configuration)
        {
            // Falls back to the old values if nothing is configured,
            // so local runs still work.
            _hostName = Read(configuration, "HostName") ?? "localhost";
            _userName = Read(configuration, "UserName") ?? "guest";
            _password = Read(configuration, "Password") ?? "guest";
            _port = int.TryParse(Read(configuration, "Port"), out var p) ? p : 5672;
        }

        // Checks Docker:/Http: prefixed key first, then the plain key.
        private static string? Read(IConfiguration config, string name)
        {
            var inContainer =
                Environment.GetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER") == "true";
            var profile = inContainer ? "Docker" : "Http";
            var key = $"MessageQueue:RabbitMQ:{name}";

            var value = config[$"{profile}:{key}"];
            if (string.IsNullOrWhiteSpace(value)) value = config[key];
            return string.IsNullOrWhiteSpace(value) ? null : value;
        }
        public async Task SendMessage(object message, string queueName)
        {
            var factory = new ConnectionFactory()
            {
                HostName = _hostName,
                Port = _port,
                Password = _password,
                UserName = _userName,
            };

            //TODO: Tutor has used CreateConnection(). But intellisense is not suggesting it.
            _connection = await factory.CreateConnectionAsync();

            var channel = await _connection.CreateChannelAsync();

            await channel.QueueDeclareAsync(
            queue: queueName,
            durable: false,
            exclusive: false,
            autoDelete: false,
            arguments: null);

            // serialize message
            var json = JsonConvert.SerializeObject(message);
            var body = Encoding.UTF8.GetBytes(json);

            // properties are optional but recommended (e.g., content-type)
            //var props = channel.CreateOptions();
            //props.ContentType = "application/json";

            // publish to the default exchange ("") using the queue name as routing key
            await channel.BasicPublishAsync(
                exchange: "",
                routingKey: queueName,
                mandatory: false,
                //basicProperties: props,
                body: body // byte[] is accepted; will be wrapped as ReadOnlyMemory<byte>
            );

            await channel.DisposeAsync();
            await _connection.DisposeAsync();
        }
    }
}
