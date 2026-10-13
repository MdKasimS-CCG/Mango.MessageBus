using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace Mango.MessageBus;

public sealed class RabbitMqSettings
{
    public string HostName { get; set; } = string.Empty;

    public int Port { get; set; } = 5672;

    public string UserName { get; set; } = string.Empty;

    public string Password { get; set; } = string.Empty;

    public string VirtualHost { get; set; } = "/";
}

