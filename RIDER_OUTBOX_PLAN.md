# Plan: Transactional Outbox Support for Riders

## Executive Summary

This plan outlines the implementation of transactional outbox support for MassTransit Riders (Kafka, EventHub). The goal is to provide the same atomicity guarantees for messages produced to external messaging systems (Kafka topics, EventHub) as currently exists for the primary bus transport.

---

## Problem Statement

### Current State

The transactional outbox currently supports:
- **Bus Outbox**: Messages published/sent via `IBus` or `ISendEndpointProvider` are captured in the outbox and delivered by `BusOutboxDeliveryService`
- **Consumer Outbox**: Messages published during consumer execution are stored atomically with inbox state

However, **Riders are not supported**:
- Messages produced to Kafka topics via `ITopicProducer<TKey, TValue>` bypass the outbox
- Messages produced to EventHub via `IEventHubProducerProvider` bypass the outbox
- No atomicity guarantee between database state and Kafka/EventHub message production

### The Gap

When a consumer processes a message and produces to Kafka:
```csharp
public async Task Consume(ConsumeContext<OrderPlaced> context)
{
    await _dbContext.Orders.AddAsync(order);
    await _dbContext.SaveChangesAsync();  // ← Transaction commits

    await _kafkaProducer.Produce(new OrderEvent { ... });  // ← Can fail after DB commit!
}
```

If the Kafka produce fails after the database commit, the system is in an inconsistent state.

### Desired State

```csharp
public async Task Consume(ConsumeContext<OrderPlaced> context)
{
    await _dbContext.Orders.AddAsync(order);

    // Kafka message stored in OutboxMessage table atomically
    await _kafkaProducer.Produce(new OrderEvent { ... });

    await _dbContext.SaveChangesAsync();  // ← Both committed atomically
}
// Background service delivers to Kafka after transaction commits
```

---

## Architecture Overview

### Design Principles

1. **Reuse existing outbox infrastructure** - Same `OutboxMessage`, `OutboxState`, `InboxState` entities
2. **Mirror the bus outbox pattern** - Use endpoint provider wrapping for interception
3. **Transport-agnostic delivery** - New delivery service that understands Kafka/EventHub destinations
4. **Backward compatible** - Opt-in configuration, existing code continues to work
5. **Consistent API** - Similar configuration experience to bus outbox

### High-Level Component Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Application Code                               │
├─────────────────────────────────────────────────────────────────────────┤
│  Consumer                                                                │
│  ├─ ITopicProducer<TKey, TValue>.Produce()                              │
│  └─ IEventHubProducerProvider.Produce()                                 │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    Outbox Producer Wrapper (NEW)                         │
├─────────────────────────────────────────────────────────────────────────┤
│  OutboxTopicProducer<TKey, TValue>                                       │
│  ├─ Intercepts Produce() calls                                          │
│  ├─ Serializes message                                                  │
│  └─ Stores in OutboxMessage with RiderName + TopicName                  │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         Database (Same Tables)                           │
├─────────────────────────────────────────────────────────────────────────┤
│  OutboxMessage                                                           │
│  ├─ DestinationAddress: "kafka://topic-name" or "eventhub://hub-name"   │
│  ├─ RiderName: "Kafka" or "EventHub" (NEW field or encoded in address) │
│  └─ ... existing fields ...                                            │
│                                                                          │
│  OutboxState (reused for rider outbox batches)                          │
│  InboxState (reused for consumer idempotency)                           │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                   Rider Outbox Delivery Service (NEW)                    │
├─────────────────────────────────────────────────────────────────────────┤
│  RiderOutboxDeliveryService<TRider>                                      │
│  ├─ Queries OutboxMessage by RiderName                                  │
│  ├─ Gets rider instance from DI                                         │
│  ├─ Reconstructs producer and delivers to topic                         │
│  └─ Removes message on success                                          │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        External Message Broker                           │
├─────────────────────────────────────────────────────────────────────────┤
│  Kafka Topic / EventHub                                                  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Implementation Plan

### Phase 1: Core Infrastructure

#### 1.1 Extend OutboxMessage Entity

**File**: `src/Persistence/MassTransit.EntityFrameworkCoreIntegration/OutboxMessage.cs`

Add fields to support rider-specific routing:

```csharp
public class OutboxMessage : OutboxMessageContext
{
    // ... existing fields ...

    // NEW: Rider identification
    public string? TransportType { get; set; }  // "Bus", "Kafka", "EventHub"

    // For Kafka: store key separately for proper partitioning
    public string? PartitionKey { get; set; }
    public byte[]? PartitionKeyBytes { get; set; }
}
```

**Alternative**: Encode transport type in `DestinationAddress` URI scheme:
- Bus: `rabbitmq://host/queue` or `sb://namespace/queue`
- Kafka: `kafka://cluster/topic`
- EventHub: `eventhub://namespace/hub`

The URI scheme approach is cleaner and requires no schema changes.

**Decision**: Use URI scheme approach - no entity changes needed.

#### 1.2 Create Rider Outbox Context Interfaces

**New File**: `src/MassTransit/Middleware/Riders/IRiderOutboxContext.cs`

```csharp
public interface IRiderOutboxContext
{
    /// <summary>
    /// Add a message to the outbox for later delivery to a rider topic
    /// </summary>
    Task AddProduce<TKey, TValue>(
        string topicName,
        TKey key,
        TValue value,
        Action<ITopicProduceContext<TKey, TValue>>? configure = null);
}

public interface IRiderOutboxContextFactory<TRider>
    where TRider : IRider
{
    IRiderOutboxContext CreateContext(ConsumeContext context);
}
```

#### 1.3 Create Outbox Topic Producer Wrapper

**New File**: `src/Transports/MassTransit.KafkaIntegration/Middleware/OutboxTopicProducer.cs`

```csharp
public class OutboxTopicProducer<TKey, TValue> : ITopicProducer<TKey, TValue>
{
    readonly ITopicProducer<TKey, TValue> _producer;
    readonly OutboxSendContext _outboxContext;
    readonly string _topicName;

    public async Task Produce(TKey key, TValue value, CancellationToken ct = default)
    {
        await Produce(key, value, Pipe.Empty<KafkaSendContext<TKey, TValue>>(), ct);
    }

    public async Task Produce(TKey key, TValue value,
        IPipe<KafkaSendContext<TKey, TValue>> pipe, CancellationToken ct = default)
    {
        // Create send context with Kafka-specific URI
        var destinationAddress = new Uri($"kafka://topic/{_topicName}");

        // Serialize and store in outbox
        await _outboxContext.AddSend(/* serialized message context */);
    }
}
```

### Phase 2: Kafka Integration

#### 2.1 Kafka Outbox Producer Provider

**New File**: `src/Transports/MassTransit.KafkaIntegration/Middleware/KafkaOutboxProducerProvider.cs`

```csharp
public class KafkaOutboxProducerProvider : ITopicProducerProvider
{
    readonly ITopicProducerProvider _innerProvider;
    readonly OutboxSendContext _outboxContext;

    public ITopicProducer<TKey, TValue> GetProducer<TKey, TValue>(Uri address)
    {
        var innerProducer = _innerProvider.GetProducer<TKey, TValue>(address);
        return new OutboxTopicProducer<TKey, TValue>(innerProducer, _outboxContext, address);
    }
}
```

#### 2.2 Kafka Scoped Bus Context for Outbox

**New File**: `src/Transports/MassTransit.KafkaIntegration/EntityFrameworkIntegration/EntityFrameworkKafkaScopedBusContext.cs`

Similar to `EntityFrameworkScopedBusContext` but for Kafka:

```csharp
public class EntityFrameworkKafkaScopedBusContext<TDbContext> :
    IKafkaScopedBusContext
    where TDbContext : DbContext
{
    readonly TDbContext _dbContext;
    readonly IKafkaRider _rider;
    readonly Guid _outboxId;
    readonly IBusOutboxNotification _notification;

    public ITopicProducerProvider TopicProducerProvider =>
        new KafkaOutboxProducerProvider(_rider, CreateOutboxContext());

    OutboxSendContext CreateOutboxContext()
    {
        return new EntityFrameworkOutboxSendContext(_dbContext, _outboxId);
    }

    public async ValueTask DisposeAsync()
    {
        if (WasCommitted())
            _notification.Delivered();
    }
}
```

#### 2.3 Kafka Outbox Delivery Service

**New File**: `src/Transports/MassTransit.KafkaIntegration/EntityFrameworkIntegration/KafkaOutboxDeliveryService.cs`

```csharp
public class KafkaOutboxDeliveryService<TDbContext> : BackgroundService
    where TDbContext : DbContext
{
    readonly IServiceScopeFactory _scopeFactory;
    readonly IKafkaRider _rider;
    readonly OutboxDeliveryServiceOptions _options;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await DeliverKafkaMessages(stoppingToken);
            await Task.Delay(_options.QueryDelay, stoppingToken);
        }
    }

    async Task DeliverKafkaMessages(CancellationToken ct)
    {
        using var scope = _scopeFactory.CreateScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<TDbContext>();

        // Query messages with kafka:// destination scheme
        var messages = await dbContext.Set<OutboxMessage>()
            .Where(m => m.DestinationAddress.StartsWith("kafka://"))
            .Where(m => m.OutboxId != null)
            .OrderBy(m => m.SequenceNumber)
            .Take(_options.MessageDeliveryLimit)
            .ToListAsync(ct);

        foreach (var message in messages)
        {
            try
            {
                await DeliverToKafka(message, ct);
                dbContext.Remove(message);
            }
            catch (Exception ex)
            {
                // Log and continue - message stays for retry
                break;
            }
        }

        await dbContext.SaveChangesAsync(ct);
    }

    async Task DeliverToKafka(OutboxMessage message, CancellationToken ct)
    {
        // Parse topic from destination address
        var topicName = ParseTopicName(message.DestinationAddress);

        // Get producer from rider
        var producer = _rider.GetProducer<byte[], byte[]>(
            new Uri($"topic:{topicName}"));

        // Reconstruct message and produce
        await producer.Produce(
            message.PartitionKeyBytes,
            Encoding.UTF8.GetBytes(message.Body),
            ct);
    }
}
```

#### 2.4 Kafka Outbox Configuration Extensions

**New File**: `src/Transports/MassTransit.KafkaIntegration/Configuration/KafkaOutboxConfigurationExtensions.cs`

```csharp
public static class KafkaOutboxConfigurationExtensions
{
    /// <summary>
    /// Configure Entity Framework outbox for Kafka rider
    /// </summary>
    public static void UseEntityFrameworkOutbox<TDbContext>(
        this IKafkaFactoryConfigurator configurator,
        Action<IKafkaOutboxConfigurator<TDbContext>>? configure = null)
        where TDbContext : DbContext
    {
        var outboxConfigurator = new KafkaOutboxConfigurator<TDbContext>();
        configure?.Invoke(outboxConfigurator);

        // Register scoped producer provider that uses outbox
        // Register delivery service
    }

    /// <summary>
    /// Configure outbox for a specific topic endpoint
    /// </summary>
    public static void UseEntityFrameworkOutbox<TDbContext>(
        this IKafkaTopicReceiveEndpointConfigurator configurator,
        IRegistrationContext context,
        Action<IOutboxConfigurator>? configure = null)
        where TDbContext : DbContext
    {
        // Similar to bus endpoint outbox configuration
    }
}
```

### Phase 3: EventHub Integration

#### 3.1 EventHub Outbox Producer Provider

**New File**: `src/Transports/MassTransit.EventHubIntegration/Middleware/EventHubOutboxProducerProvider.cs`

```csharp
public class EventHubOutboxProducerProvider : IEventHubProducerProvider
{
    readonly IEventHubProducerProvider _innerProvider;
    readonly OutboxSendContext _outboxContext;

    public IEventHubProducer GetProducer(string eventHubName)
    {
        var innerProducer = _innerProvider.GetProducer(eventHubName);
        return new OutboxEventHubProducer(innerProducer, _outboxContext, eventHubName);
    }
}
```

#### 3.2 EventHub Outbox Delivery Service

**New File**: `src/Transports/MassTransit.EventHubIntegration/EntityFrameworkIntegration/EventHubOutboxDeliveryService.cs`

Similar pattern to Kafka, but queries for `eventhub://` destination addresses.

#### 3.3 EventHub Outbox Configuration Extensions

**New File**: `src/Transports/MassTransit.EventHubIntegration/Configuration/EventHubOutboxConfigurationExtensions.cs`

```csharp
public static class EventHubOutboxConfigurationExtensions
{
    public static void UseEntityFrameworkOutbox<TDbContext>(
        this IEventHubFactoryConfigurator configurator,
        Action<IEventHubOutboxConfigurator<TDbContext>>? configure = null)
        where TDbContext : DbContext
    {
        // Register outbox components for EventHub
    }
}
```

### Phase 4: Consumer Outbox Integration

#### 4.1 Rider-Aware Outbox Consume Context

Extend the existing `OutboxConsumeContext` pattern to support rider producers:

**Modified File**: `src/MassTransit/Middleware/Outbox/OutboxConsumeContextProxy.cs`

```csharp
public abstract class OutboxConsumeContextProxy<TMessage> :
    ConsumeContextProxy<TMessage>,
    OutboxConsumeContext<TMessage>,
    IRiderOutboxContext  // NEW interface
{
    // Existing implementation...

    // NEW: Rider producer support
    public abstract Task AddRiderProduce(
        string riderName,
        Uri destinationAddress,
        byte[] key,
        byte[] body,
        IDictionary<string, object> headers);
}
```

#### 4.2 EF Core Implementation

**Modified File**: `src/Persistence/MassTransit.EntityFrameworkCoreIntegration/DbContextOutboxConsumeContext.cs`

```csharp
public class DbContextOutboxConsumeContext<TDbContext, TMessage> :
    OutboxConsumeContextProxy<TMessage>
{
    // Existing implementation...

    public override async Task AddRiderProduce(
        string riderName,
        Uri destinationAddress,
        byte[] key,
        byte[] body,
        IDictionary<string, object> headers)
    {
        var outboxMessage = new OutboxMessage
        {
            MessageId = NewId.NextGuid(),
            DestinationAddress = destinationAddress,
            Body = Encoding.UTF8.GetString(body),
            PartitionKeyBytes = key,
            ContentType = "application/json",
            SentTime = DateTime.UtcNow,
            InboxMessageId = _inboxState.MessageId,
            InboxConsumerId = _inboxState.ConsumerId,
            // ... other fields
        };

        await _dbContext.Set<OutboxMessage>().AddAsync(outboxMessage);
    }
}
```

### Phase 5: MongoDB Support

#### 5.1 MongoDB Kafka Outbox Context

**New File**: `src/Persistence/MassTransit.MongoDbIntegration/Outbox/MongoDbKafkaOutboxContext.cs`

Mirror the EF Core implementation for MongoDB.

#### 5.2 MongoDB Delivery Services

**New Files**:
- `src/Persistence/MassTransit.MongoDbIntegration/Outbox/MongoDbKafkaOutboxDeliveryService.cs`
- `src/Persistence/MassTransit.MongoDbIntegration/Outbox/MongoDbEventHubOutboxDeliveryService.cs`

---

## Configuration API Design

### Kafka Rider with Outbox

```csharp
services.AddMassTransit(x =>
{
    x.AddEntityFrameworkOutbox<AppDbContext>(o =>
    {
        o.UseSqlServer();
        o.UseBusOutbox();

        // NEW: Enable rider outbox
        o.UseRiderOutbox(ro =>
        {
            ro.MessageDeliveryLimit = 100;
            ro.QueryDelay = TimeSpan.FromSeconds(5);
        });
    });

    x.AddRider(rider =>
    {
        rider.AddProducer<string, OrderEvent>("order-events");

        rider.UsingKafka((context, k) =>
        {
            k.Host("localhost:9092");

            // NEW: Enable outbox for this rider
            k.UseEntityFrameworkOutbox<AppDbContext>(o =>
            {
                o.QueryDelay = TimeSpan.FromSeconds(1);
            });

            k.TopicEndpoint<string, OrderCommand>("order-commands", "my-group", e =>
            {
                // NEW: Enable consumer outbox for topic endpoint
                e.UseEntityFrameworkOutbox<AppDbContext>(context);

                e.ConfigureConsumer<OrderCommandConsumer>(context);
            });
        });
    });
});
```

### EventHub Rider with Outbox

```csharp
services.AddMassTransit(x =>
{
    x.AddEntityFrameworkOutbox<AppDbContext>(o =>
    {
        o.UseSqlServer();
        o.UseRiderOutbox();
    });

    x.AddRider(rider =>
    {
        rider.UsingEventHub((context, eh) =>
        {
            eh.Host("connection-string");

            // NEW: Enable outbox for EventHub
            eh.UseEntityFrameworkOutbox<AppDbContext>();

            eh.ReceiveEndpoint("my-hub", e =>
            {
                e.UseEntityFrameworkOutbox<AppDbContext>(context);
                e.ConfigureConsumer<MyConsumer>(context);
            });
        });
    });
});
```

---

## File Changes Summary

### New Files

| File | Purpose |
|------|---------|
| `MassTransit.KafkaIntegration/Middleware/OutboxTopicProducer.cs` | Wraps topic producer to capture messages |
| `MassTransit.KafkaIntegration/Middleware/KafkaOutboxProducerProvider.cs` | Provides outbox-wrapped producers |
| `MassTransit.KafkaIntegration/EntityFrameworkIntegration/EntityFrameworkKafkaScopedBusContext.cs` | Scoped context for Kafka outbox |
| `MassTransit.KafkaIntegration/EntityFrameworkIntegration/KafkaOutboxDeliveryService.cs` | Background delivery for Kafka |
| `MassTransit.KafkaIntegration/Configuration/KafkaOutboxConfigurationExtensions.cs` | Configuration API |
| `MassTransit.KafkaIntegration/Configuration/IKafkaOutboxConfigurator.cs` | Configurator interface |
| `MassTransit.KafkaIntegration/Configuration/KafkaOutboxConfigurator.cs` | Configurator implementation |
| `MassTransit.EventHubIntegration/Middleware/OutboxEventHubProducer.cs` | Wraps EventHub producer |
| `MassTransit.EventHubIntegration/Middleware/EventHubOutboxProducerProvider.cs` | Provides outbox-wrapped producers |
| `MassTransit.EventHubIntegration/EntityFrameworkIntegration/EntityFrameworkEventHubScopedBusContext.cs` | Scoped context for EventHub outbox |
| `MassTransit.EventHubIntegration/EntityFrameworkIntegration/EventHubOutboxDeliveryService.cs` | Background delivery for EventHub |
| `MassTransit.EventHubIntegration/Configuration/EventHubOutboxConfigurationExtensions.cs` | Configuration API |
| `MassTransit.MongoDbIntegration/Kafka/MongoDbKafkaOutboxDeliveryService.cs` | MongoDB Kafka delivery |
| `MassTransit.MongoDbIntegration/EventHub/MongoDbEventHubOutboxDeliveryService.cs` | MongoDB EventHub delivery |
| `MassTransit/Middleware/Riders/IRiderOutboxContext.cs` | Core rider outbox interface |

### Modified Files

| File | Changes |
|------|---------|
| `MassTransit/Middleware/Outbox/OutboxConsumeContextProxy.cs` | Add `IRiderOutboxContext` implementation |
| `MassTransit.EntityFrameworkCoreIntegration/DbContextOutboxConsumeContext.cs` | Add rider produce support |
| `MassTransit.EntityFrameworkCoreIntegration/EntityFrameworkOutboxConfigurator.cs` | Add `UseRiderOutbox()` method |
| `MassTransit.MongoDbIntegration/Outbox/MongoDbOutboxConsumeContext.cs` | Add rider produce support |
| `MassTransit.KafkaIntegration/KafkaIntegration/KafkaRider.cs` | Support outbox producer provider |
| `MassTransit.EventHubIntegration/EventHubIntegration/EventHubRider.cs` | Support outbox producer provider |

---

## Testing Strategy

### Unit Tests

1. **OutboxTopicProducer Tests**
   - Verify message serialization
   - Verify outbox context interaction
   - Verify key/partition handling

2. **Delivery Service Tests**
   - Verify message query filtering by URI scheme
   - Verify delivery retry behavior
   - Verify message removal on success

### Integration Tests

1. **Kafka Outbox Integration**
   - Produce message, verify in OutboxMessage table
   - Verify delivery to actual Kafka topic
   - Test transaction rollback (message not delivered)

2. **Consumer Outbox with Kafka Produce**
   - Consume bus message, produce to Kafka
   - Verify atomicity of inbox + outbox
   - Test idempotency on redelivery

3. **Multi-Rider Scenario**
   - Consumer produces to both Kafka and EventHub
   - Verify both delivered after commit

### Test Files

| File | Purpose |
|------|---------|
| `tests/MassTransit.KafkaIntegration.Tests/Outbox/KafkaOutbox_Specs.cs` | Kafka outbox integration tests |
| `tests/MassTransit.KafkaIntegration.Tests/Outbox/KafkaConsumerOutbox_Specs.cs` | Consumer outbox with Kafka produce |
| `tests/MassTransit.EventHubIntegration.Tests/Outbox/EventHubOutbox_Specs.cs` | EventHub outbox integration tests |

---

## Migration Considerations

### Database Schema

No schema changes required if using URI scheme approach for transport identification.

If `TransportType` column is added:
```sql
ALTER TABLE OutboxMessage ADD TransportType NVARCHAR(50) NULL;
CREATE INDEX IX_OutboxMessage_TransportType ON OutboxMessage(TransportType, SequenceNumber);
```

### Backward Compatibility

- Existing bus outbox continues to work unchanged
- Rider outbox is opt-in via `UseEntityFrameworkOutbox()` on rider configurator
- No breaking changes to existing APIs

---

## Implementation Order

### Sprint 1: Core Kafka Support
1. Create `IRiderOutboxContext` interface
2. Implement `OutboxTopicProducer<TKey, TValue>`
3. Implement `KafkaOutboxProducerProvider`
4. Implement `KafkaOutboxDeliveryService`
5. Add configuration extensions
6. Write integration tests

### Sprint 2: Consumer Outbox Integration
1. Extend `OutboxConsumeContextProxy` with rider support
2. Implement in `DbContextOutboxConsumeContext`
3. Wire up scoped producer provider in consume context
4. Test consumer → Kafka produce atomicity

### Sprint 3: EventHub Support
1. Implement `OutboxEventHubProducer`
2. Implement `EventHubOutboxProducerProvider`
3. Implement `EventHubOutboxDeliveryService`
4. Add configuration extensions
5. Write integration tests

### Sprint 4: MongoDB Support
1. Implement MongoDB Kafka outbox delivery
2. Implement MongoDB EventHub outbox delivery
3. Add MongoDB configuration extensions
4. Write integration tests

### Sprint 5: Polish & Documentation
1. Performance optimization
2. Metrics and observability
3. Documentation
4. Sample projects

---

## Open Questions

1. **Key Serialization**: How should Kafka message keys be stored in the outbox? As bytes, string, or JSON?
   - Recommendation: Store as bytes (`PartitionKeyBytes`) for maximum flexibility

2. **Headers**: Should Kafka/EventHub-specific headers be stored separately or merged with MassTransit headers?
   - Recommendation: Store in existing `Headers` JSON field with transport-specific prefix

3. **Partitioning**: Should the delivery service respect original partition assignments?
   - Recommendation: Yes, use stored key for consistent partitioning

4. **Ordering**: Should messages be delivered in strict sequence number order across all topics?
   - Recommendation: Order per-topic, not globally, for better parallelism

5. **Multiple Riders**: If multiple Kafka riders exist (e.g., different clusters), how to route?
   - Recommendation: Include rider name in destination URI: `kafka://rider-name/topic`

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Kafka producer context differs from bus send context | Medium | Create adapter that maps Kafka-specific context to outbox format |
| Delivery service contention with bus delivery | Low | Separate polling intervals, use rider-specific notification |
| Key/value serialization mismatch | High | Store original serialized bytes, deserialize only for logging |
| Performance regression in high-throughput scenarios | Medium | Batch deliveries, optimize queries, add metrics |

---

## Success Criteria

1. Messages produced to Kafka/EventHub are stored atomically with database state
2. Background delivery service reliably delivers messages
3. Consumer outbox works for consumers that produce to riders
4. No regression in bus outbox functionality
5. Configuration API is intuitive and consistent
6. Performance within 10% of non-outbox produce operations
