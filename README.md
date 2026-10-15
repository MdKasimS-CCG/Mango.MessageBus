This package is required by Mango application to have Event Bus. It enables event-driven architecture for online system - Mango.

<h2>GitHub Packages Flow</h2>

        Developer
            │
            │Push
            ▼
        GitHub
            │
            ▼
        GitHub Action
            │
            ▼
        dotnet pack
            │
            ▼
        dotnet nuget push
            │
            ▼
        GitHub Packages
            │
            ▼
        AuthApi
        OrderApi
        ProductApi
        RewardApi
        EmailApi

<h2>Usage</h2>

> **Version 2.0.0 – breaking change.** `RabbitMQMessageProducer` no longer has a parameterless constructor and RabbitMQ configuration is now mandatory. See [Migrating from 1.x](#migrating-from-1x).

### 1. Add configuration

Add a `RabbitMq` section to `appsettings.json`. **Do not commit real credentials** – supply `UserName` and `Password` through user-secrets (local development) or environment variables / a secret store (deployed environments).

```json
{
  "RabbitMq": {
    "HostName": "localhost",
    "Port": 5672,
    "UserName": "",
    "Password": "",
    "VirtualHost": "/"
  }
}
```

Environment-variable equivalents: `RabbitMq__HostName`, `RabbitMq__UserName`, `RabbitMq__Password`, etc.

| Setting       | Required | Default | Notes                                  |
|---------------|----------|---------|----------------------------------------|
| `HostName`    | Yes      | –       | RabbitMQ broker host                   |
| `Port`        | No       | `5672`  | Must be between 1 and 65535            |
| `UserName`    | Yes      | –       |                                        |
| `Password`    | Yes      | –       |                                        |
| `VirtualHost` | No       | `/`     |                                        |

If a required setting is missing, the producer throws an `InvalidOperationException` with a clear message the first time it is resolved.

### 2. Register in `Program.cs`

```csharp
using Mango.MessageBus;

builder.Services.Configure<RabbitMqSettings>(
    builder.Configuration.GetSection("RabbitMq"));

builder.Services.AddScoped<IMessageProducer, RabbitMQMessageProducer>();
```

### 3. Send a message

```csharp
public class OrderController : ControllerBase
{
    private readonly IMessageProducer _producer;

    public OrderController(IMessageProducer producer) => _producer = producer;

    [HttpPost]
    public async Task<IActionResult> Create(OrderDto order)
    {
        // ... save the order ...
        await _producer.SendMessage(order, "order-created");
        return Ok();
    }
}
```

The message is serialized to JSON (Newtonsoft.Json) and published to the named queue.

### Migrating from 1.x

- `new RabbitMQMessageProducer()` no longer exists – resolve `IMessageProducer` through dependency injection.
- `RabbitMQMessageProducer` is now `sealed`; use composition instead of inheriting from it.
- Add the `RabbitMq` configuration section and the `Configure<RabbitMqSettings>(...)` registration shown above, otherwise the service will fail when the producer is first resolved.