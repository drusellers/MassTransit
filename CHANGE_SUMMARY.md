# Kafka Rider Transactional Outbox Implementation Summary

## Overview

This implementation adds transactional outbox support for Kafka riders in MassTransit. Messages produced to Kafka topics are stored in the database outbox within the same transaction as business data, then delivered by a background service after the transaction commits.

## Problem Solved

Without the outbox pattern, producing to Kafka during a database transaction creates a distributed transaction problem:

```csharp
// BEFORE: Potential inconsistency
public async Task Consume(ConsumeContext<OrderPlaced> context)
{
    await _dbContext.Orders.AddAsync(order);
    await _dbContext.SaveChangesAsync();  // Transaction commits

    await _kafkaProducer.Produce(new OrderEvent { ... });  // Can fail after DB commit!
}
```

With the Kafka outbox:

```csharp
// AFTER: Atomic operation
public async Task Consume(ConsumeContext<OrderPlaced> context)
{
    await _dbContext.Orders.AddAsync(order);

    // Kafka message stored in OutboxMessage table atomically
    await _kafkaProducer.Produce(new OrderEvent { ... });

    await _dbContext.SaveChangesAsync();  // Both committed atomically
}
// Background service delivers to Kafka after transaction commits
```

## Files Created

### Kafka Integration Project

**`src/Transports/MassTransit.KafkaIntegration/KafkaIntegration/Middleware/`**

| File | Purpose |
|------|---------|
| `OutboxTopicProducer.cs` | Wraps `ITopicProducer<TKey, TValue>` to intercept Produce calls and store messages in the outbox |
| `OutboxKafkaSendContext.cs` | Kafka-specific send context that captures message data and Kafka properties (key, partition) for outbox storage |
| `OutboxTopicProducerProvider.cs` | Wraps `ITopicProducerProvider` to return outbox-wrapped producers |

### New Bridge Project

**`src/Persistence/MassTransit.KafkaIntegration.EntityFrameworkCoreIntegration/`**

| File | Purpose |
|------|---------|
| `MassTransit.KafkaIntegration.EntityFrameworkCoreIntegration.csproj` | New NuGet package (`MassTransit.Kafka.EntityFrameworkCore`) bridging Kafka and EF Core |
| `KafkaOutboxDeliveryService.cs` | Background service that polls outbox for `kafka://` messages and delivers to Kafka |
| `KafkaOutboxDeliveryServiceOptions.cs` | Configuration options (MessageDeliveryLimit, QueryDelay, Timeouts) |
| `EntityFrameworkKafkaScopedContext.cs` | Scoped context providing outbox-wrapped topic producers |
| `SerializedKafkaMessage.cs` | DTO for serialized messages delivered from outbox |

**`src/Persistence/MassTransit.KafkaIntegration.EntityFrameworkCoreIntegration/Configuration/`**

| File | Purpose |
|------|---------|
| `IKafkaOutboxConfigurator.cs` | Interface for configuring Kafka outbox options |
| `KafkaOutboxConfigurator.cs` | Implementation that registers services and options |
| `KafkaOutboxConfigurationExtensions.cs` | Extension methods for enabling Kafka outbox |

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Application Code                          │
│   await producer.Produce(key, message)                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              OutboxTopicProducer<TKey, TValue>                   │
│   - Intercepts Produce() calls                                  │
│   - Creates OutboxKafkaSendContext with serialization           │
│   - Stores Kafka key/partition in message properties            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              EntityFrameworkKafkaScopedContext                   │
│   - Implements OutboxSendContext                                │
│   - Manages OutboxState lifecycle                               │
│   - Notifies delivery service on commit                         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Database (Same Transaction)                   │
│   OutboxMessage:                                                │
│   - DestinationAddress: kafka://topic/my-topic                  │
│   - Body: Serialized JSON                                       │
│   - Properties: {"MT-Kafka-Key": "...", ...}                    │
│   - OutboxId: Links to OutboxState                              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              KafkaOutboxDeliveryService (Background)             │
│   - Polls for messages with kafka:// destination                │
│   - Acquires database lock for delivery                         │
│   - Delivers via IKafkaRider.GetProducer()                      │
│   - Removes message after successful delivery                   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         Kafka Topic                              │
└─────────────────────────────────────────────────────────────────┘
```

## Configuration API

### Basic Usage

```csharp
services.AddMassTransit(x =>
{
    // Configure EF Core outbox (required)
    x.AddEntityFrameworkOutbox<AppDbContext>(o =>
    {
        o.UseSqlServer();
    });

    x.AddRider(rider =>
    {
        rider.AddProducer<string, OrderEvent>("order-events");

        rider.UsingKafka((context, k) =>
        {
            k.Host("localhost:9092");

            // Enable Kafka transactional outbox
            k.UseEntityFrameworkOutbox<AppDbContext>(services, o =>
            {
                o.MessageDeliveryLimit = 100;
                o.MessageDeliveryTimeout = TimeSpan.FromSeconds(30);
                o.QueryDelay = TimeSpan.FromSeconds(5);
            });
        });
    });
});
```

### Using the Outbox Producer

```csharp
public class OrderService
{
    readonly EntityFrameworkKafkaScopedContext<AppDbContext> _kafkaContext;
    readonly AppDbContext _dbContext;

    public OrderService(
        EntityFrameworkKafkaScopedContext<AppDbContext> kafkaContext,
        AppDbContext dbContext)
    {
        _kafkaContext = kafkaContext;
        _dbContext = dbContext;
    }

    public async Task CreateOrder(Order order)
    {
        // Business logic
        await _dbContext.Orders.AddAsync(order);

        // Get outbox-wrapped producer
        var producer = _kafkaContext.GetProducer<string, OrderCreatedEvent>("order-events");

        // Message stored in outbox (same transaction)
        await producer.Produce(order.Id.ToString(), new OrderCreatedEvent
        {
            OrderId = order.Id,
            CustomerId = order.CustomerId
        });

        // Both saved atomically
        await _dbContext.SaveChangesAsync();
    }
}
```

## Key Design Decisions

1. **Reuses Existing Outbox Infrastructure**: Same `OutboxMessage`, `OutboxState`, and `InboxState` entities as the bus outbox.

2. **URI Scheme for Transport Identification**: Messages use `kafka://topic/topic-name` destination address to identify Kafka messages without schema changes.

3. **Separate Delivery Service**: `KafkaOutboxDeliveryService` runs independently from `BusOutboxDeliveryService`, querying only `kafka://` messages.

4. **Kafka Properties in Message Properties**: Key and partition stored in the `Properties` JSON field of `OutboxMessage`.

5. **New Bridge Package**: `MassTransit.Kafka.EntityFrameworkCore` package bridges the Kafka and EF Core integrations without creating circular dependencies.

## Database Requirements

No schema changes required. The existing outbox tables support Kafka messages:

- `OutboxMessage.DestinationAddress` stores `kafka://topic/topic-name`
- `OutboxMessage.Properties` stores Kafka-specific data (key, partition)
- `OutboxState` tracks delivery state per outbox batch

## Testing

The implementation should be tested with:

1. **Unit Tests**: OutboxTopicProducer message interception and serialization
2. **Integration Tests**: End-to-end outbox storage and Kafka delivery
3. **Transaction Rollback Tests**: Verify messages not delivered on rollback

## Future Enhancements

1. **EventHub Support**: Similar implementation for Azure EventHub rider
2. **Consumer Outbox Integration**: Wire up outbox for messages published during Kafka consumer execution
3. **MongoDB Support**: Add MongoDB outbox support for Kafka
4. **Metrics and Observability**: Add OpenTelemetry instrumentation for Kafka outbox delivery
