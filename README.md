# Mango.MessageBus

Mango.MessageBus is a shared messaging library for the Mango microservices. It provides a common `IMessageProducer` interface and a RabbitMQ-backed implementation to support asynchronous, event-driven communication between services.

**Current package version:** `2.0.1` (as specified in `Mango.MessageBus.csproj`)

**Breaking change:** Version 2.x requires RabbitMQ configuration through `IOptions<RabbitMqSettings>`. See [Migrating from 1.x](#migrating-from-1x).

> **Implementation status:** `RabbitMQMessageProducer` is implemented. `AzureServiceBusMessageProducer` is currently a stub and is not usable. Broker selection is not configurable through the current implementation.

## Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Dependency Injection](#dependency-injection)
- [Publishing a Message](#publishing-a-message)
- [Reliability and Delivery](#reliability-and-delivery)
- [Dead-Letter Queue](#dead-letter-queue)
- [Limitations](#limitations)
- [Migrating from 1.x](#migrating-from-1x)
- [Troubleshooting](#troubleshooting)

## Features

- Shared `IMessageProducer` abstraction.
- RabbitMQ-backed message publishing.
- JSON serialization using Newtonsoft.Json.
- Reusable RabbitMQ connection per producer instance.
- Separate channel creation and disposal for each publishing operation.
- Durable queue declarations and persistent messages.
- Publisher confirmations and confirmation tracking.
- Mandatory publishing to detect unroutable messages.
- Dead-letter exchange and queue configuration for `rewardordercreated`.
- Configuration validation for required RabbitMQ settings.

## Requirements

- .NET 10.0.
- A reachable RabbitMQ instance.
- Access to the Mango.MessageBus package hosted on GitHub Packages.
- A GitHub Packages token with permission to read packages.

## Installation

The package is hosted on GitHub Packages. Configure the package source in a `NuGet.Config` file in the consuming repository:

```xml
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <add key="github"
         value="https://nuget.pkg.github.com/MdKasimS-CCG/index.json" />
  </packageSources>
</configuration>
```

Authenticate with a token that has permission to read packages. Keep credentials out of source control and Docker images. Use user-level NuGet configuration, environment variables, or CI secrets as appropriate.

Add the package reference to the consuming service's project file:

```xml
<PackageReference Include="Mango.MessageBus" Version="2.0.1" />
```

Use this version only after `2.0.1` has been published and is available to the consuming environment. Otherwise, use the version that is actually published.

## Configuration

The producer uses `RabbitMqSettings` to configure the RabbitMQ connection.

| Property | Type | Required | Default |
|---|---|---|---|
| `HostName` | `string` | Yes | None |
| `Port` | `int` | No | `5672` |
| `UserName` | `string` | Yes | None |
| `Password` | `string` | Yes | None |
| `VirtualHost` | `string` | Yes | `/` |

Settings are read from the `MessageQueue:RabbitMQ` configuration section. When using environment variables, double underscores (`__`) represent configuration hierarchy separators.

Example environment variables:

```text
MessageQueue__RabbitMQ__HostName=rabbitmq
MessageQueue__RabbitMQ__Port=5672
MessageQueue__RabbitMQ__UserName=<configured username>
MessageQueue__RabbitMQ__Password=<configured password>
MessageQueue__RabbitMQ__VirtualHost=/
```

Replace the placeholder values with your environment's configuration. Do not commit real credentials.

Choose the hostname according to the deployment environment:

- **Docker Compose:** Use the RabbitMQ service name when both services share a Compose network.
- **Kubernetes:** Use the RabbitMQ Service DNS name.
- **Local execution:** Use a hostname or address reachable from the host.

The producer validates that the hostname, username, password, and virtual host are not empty and that the port is between `1` and `65535`. Invalid settings cause an exception when the producer is constructed.

## Dependency Injection

Register the RabbitMQ settings and producer in the consuming service's `Program.cs`:

```csharp
using Mango.MessageBus;

builder.Services.Configure<RabbitMqSettings>(
    builder.Configuration.GetSection("MessageQueue:RabbitMQ"));

builder.Services.AddScoped<IMessageProducer, RabbitMQMessageProducer>();
```

The producer is resolved through dependency injection. Its RabbitMQ connection is created lazily when the first message is sent, rather than during construction.

The registration above does not itself validate the configuration at application startup.

## Publishing a Message

Inject `IMessageProducer` into the class responsible for publishing events:

```csharp
using Mango.MessageBus;

public sealed class OrderPublisher
{
    private readonly IMessageProducer _messageProducer;

    public OrderPublisher(IMessageProducer messageProducer)
    {
        _messageProducer = messageProducer;
    }

    public Task PublishAsync(OrderCreated orderCreated)
    {
        return _messageProducer.SendMessage(
            orderCreated,
            "OrderCreatedQueue");
    }
}
```

`OrderCreated` and `OrderCreatedQueue` are illustrative names. Use the event contract and queue name agreed upon by the publishing and consuming services.

The producer serializes the message to JSON and publishes it to RabbitMQ's default exchange using the queue name as the routing key.

Consumers are implemented separately; this package does not provide a consumer API.

Publish an event only after the underlying operation—for example, saving an order—has succeeded.

## Reliability and Delivery

The current `RabbitMQMessageProducer` implements the following behavior for each `SendMessage` call:

1. Validates the message and queue name.
2. Acquires a lifecycle semaphore to coordinate publishing, connection creation, and disposal.
3. Reuses an open RabbitMQ connection or creates a new one if no usable connection exists.
4. Creates a channel for the publishing operation and disposes it afterward.
5. Declares the target queue with durable settings.
6. Serializes the message to UTF-8 JSON.
7. Publishes a persistent message with a generated `MessageId`.
8. Enables publisher confirmations and confirmation tracking, and publishes with `mandatory: true`.
9. Disposes the channel and releases the lifecycle semaphore.

### Connection lifecycle

The producer keeps a `ConnectionFactory` and reuses its connection across publishing operations for that producer instance.

If a subsequent send detects that the connection has closed, the producer disposes the previous connection and attempts to create a replacement. Connection recovery is therefore attempted when another send occurs; the producer does not independently monitor and reconnect an idle connection.

The lifecycle semaphore also serializes sends and coordinates disposal. This favors lifecycle safety over concurrent publishing throughput.

### Queue durability and message persistence

Queues are declared as durable, non-exclusive, and not auto-deleted. Messages are marked persistent.

These settings improve message survival across broker restarts when RabbitMQ is configured appropriately. They do not guarantee delivery in every failure scenario or guarantee that a consumer has processed a message.

All services declaring the same queue must use compatible queue properties. An existing queue declared with different properties may cause RabbitMQ to return `PRECONDITION_FAILED`.

### Publisher confirmations and unroutable messages

Publisher confirmations are enabled with confirmation tracking, and publishing uses `mandatory: true`.

The publishing operation awaits the RabbitMQ client's publish operation. A successful result does not mean that EmailAPI, RewardAPI, or another consumer has successfully processed the event.

Publishing failures are wrapped in an `InvalidOperationException` that preserves the underlying exception.

A connection failure can occur after RabbitMQ has received a message. Blindly retrying a failed send may therefore produce duplicate deliveries. Consumers should be designed to handle duplicates where necessary.

## Dead-Letter Queue

The producer currently configures dead-letter handling specifically for the `rewardordercreated` queue.

| Component | Name |
|---|---|
| Main queue | `rewardordercreated` |
| Dead-letter exchange | `reward.dlx` |
| Exchange type | Direct |
| Dead-letter queue | `rewardordercreated.dlq` |
| Dead-letter routing key | `rewardordercreated` |

The producer declares the dead-letter exchange and queue as durable, binds the dead-letter queue to the exchange, and configures the main queue with the corresponding dead-letter exchange and routing key.

Messages rejected or negatively acknowledged without requeueing can be routed to the dead-letter queue when RabbitMQ's dead-letter conditions are met.

**Important:** The dead-letter configuration is currently hardcoded for `rewardordercreated`. Other queues receive durable declarations without these dead-letter arguments. This is not a general-purpose retry policy.

The consuming service must declare the main queue using compatible arguments. Coordinate queue declarations between the producer and consumer before deploying this change to an environment with existing queues.

## Limitations

- **No consumer implementation:** Consumers must be implemented in the consuming services.
- **No exactly-once guarantee:** Publisher confirmations do not guarantee successful processing by a consumer.
- **No general retry policy:** The package does not implement a configurable retry count, delay, or retry queue.
- **Queue-specific dead-lettering:** Dead-letter configuration is currently specific to `rewardordercreated`.
- **No automatic idle reconnection:** A closed connection is replaced when a later send attempts to use it.
- **Serialized publishing:** The lifecycle semaphore allows only one publishing operation at a time per producer instance.
- **No broker switching:** Azure Service Bus is not currently implemented as a usable alternative.
- **Deployment compatibility:** Existing queue declarations must be compatible with the durable and dead-letter settings used by the producer.

## Migrating from 1.x

Version 2.x introduces breaking API changes:

- The parameterless `RabbitMQMessageProducer` constructor has been removed.
- The producer is sealed; use composition instead of inheritance.
- RabbitMQ configuration through `IOptions<RabbitMqSettings>` is required.
- Consumers should resolve `IMessageProducer` through dependency injection.
- Existing services must configure the `MessageQueue:RabbitMQ` section and use a compatible package version.

Review queue declarations before upgrading services. Changing a queue from non-durable to durable, or changing its arguments, does not modify an existing RabbitMQ queue. Existing queues may need a planned migration or safe recreation.

## Troubleshooting

### RabbitMQ HostName is not configured

Check that `MessageQueue:RabbitMQ:HostName` is supplied by an active configuration provider and that the settings are bound to the correct section.

### RabbitMQ UserName or Password is not configured

Provide non-empty credentials through environment-specific configuration or a secret manager.

### PRECONDITION_FAILED when declaring a queue

The producer and consumer may be declaring the same queue with incompatible durability or arguments. Align their declarations. If the queue already exists, plan its migration before changing its properties.

### Package restore cannot find Mango.MessageBus

Verify that the referenced package version has been published, that the GitHub Packages source URL is correct, and that the environment has valid package-read credentials.

### A message is not received by a consumer

Verify that RabbitMQ is reachable, the target queue exists, the consumer is connected, and the event contract and queue name match. Check publisher errors and RabbitMQ logs for routing or declaration failures.

### Publishing fails after a connection interruption

Inspect the underlying exception and RabbitMQ availability. The producer attempts to create a replacement connection on a later send if it detects that the existing connection has closed. Before retrying a potentially ambiguous send, consider whether the consumer can safely handle duplicate messages.
