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
        private IConnection _connection;

        public RabbitMQMessageProducer()
        {
            // TODO: Make it DI specific code that can be passed via args as well
            _hostName = "localhost";
            _userName = "guest";
            _password = "guest";
        }
        public async Task SendMessage(object message, string queueName)
        {
            var factory = new ConnectionFactory()
            {
                HostName = _hostName,
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
