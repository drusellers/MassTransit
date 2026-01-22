# MassTransit Outbox Architecture

This document describes the architecture and implementation of the Outbox pattern in MassTransit.

## Overview

The Outbox pattern in MassTransit provides reliable messaging by ensuring that messages are only published after the associated business transaction has been committed. This prevents the "dual write" problem where a database transaction could succeed but message publishing fails (or vice versa).

MassTransit implements two complementary patterns:

1. **Inbox Pattern (Consumer-side)**: Ensures idempotent message consumption by tracking which messages have been processed
2. **Outbox Pattern (Producer-side)**: Stores outgoing messages in the same transaction as business logic, then delivers them asynchronously

## Core Abstractions

### OutboxConsumeContext

**Path:** `src/MassTransit/Middleware/OutboxConsumeContext.cs`

The central abstraction that combines consume and send capabilities for outbox processing:

- `CapturedContext` - Stores the original ConsumeContext
- `IsMessageConsumed` / `IsOutboxDelivered` - State tracking flags
- `ReceiveCount` / `LastSequenceNumber` - Delivery attempt tracking
- `SetConsumed()` / `SetDelivered()` - Mark processing milestones
- `LoadOutboxMessages()` / `RemoveOutboxMessages()` - Message management
- `AddSend<T>()` - Queue messages for later delivery

### OutboxMessageContext

**Path:** `src/MassTransit/Middleware/OutboxMessageContext.cs`

Represents a serialized message stored in the outbox:

- `SequenceNumber` - Order identifier for delivery
- `MessageId`, `ContentType`, `MessageType`, `Body` - Message payload
- `Properties` - Transport-specific metadata (routing keys, partition keys, session IDs)

### IOutboxContextFactory

**Path:** `src/MassTransit/Middleware/IOutboxContextFactory.cs`

Factory pattern for creating outbox context implementations. Each persistence provider implements this interface.

## Message Flow

### Consumer-Side Flow (Inbox + Outbox)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Message Received                                 │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  OutboxConsumeFilter                                                     │
│  - Resolves IOutboxContextFactory                                        │
│  - Creates OutboxMessagePipe wrapping the consumer                       │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  OutboxContextFactory.Send()                                             │
│  - Acquires lock on InboxState (MessageId + ConsumerId)                  │
│  - Begins transaction                                                    │
│  - Creates/retrieves InboxState record                                   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                        ┌───────────┴───────────┐
                        │                       │
                        ▼                       ▼
            ┌───────────────────┐   ┌───────────────────────┐
            │  First Delivery   │   │  Retry Delivery       │
            │  (!IsConsumed)    │   │  (IsConsumed)         │
            └───────────────────┘   └───────────────────────┘
                        │                       │
                        ▼                       │
┌─────────────────────────────────────────────────────────────────────────┐
│  Execute Consumer Logic                                                  │
│  - Business logic executes                                               │
│  - Send/Publish calls captured via AddSend()                             │
│  - Messages stored in OutboxMessage table (same transaction)             │
│  - SetConsumed() marks InboxState.Consumed                               │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Deliver Outbox Messages                                                 │
│  - LoadOutboxMessages() retrieves pending messages                       │
│  - Each message sent via endpoint.Send(SerializedMessageBody)            │
│  - LastSequenceNumber updated after each success                         │
│  - SetDelivered() marks InboxState.Delivered                             │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Cleanup                                                                 │
│  - RemoveOutboxMessages() clears delivered messages                      │
│  - Transaction commits                                                   │
│  - Lock released                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Bus Outbox Flow (Producer-Side, On-Ramp)

The Bus Outbox is used when publishing messages outside of a consumer context (e.g., from an API controller):

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Application Code (Controller, Service, etc.)                            │
│  - Begins database transaction                                           │
│  - Performs business logic                                               │
│  - Calls bus.Publish() or endpoint.Send()                                │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  EntityFrameworkScopedBusContext                                         │
│  - Creates OutboxState record with unique OutboxId                       │
│  - Messages stored in OutboxMessage table (OutboxId as foreign key)      │
│  - All within same database transaction                                  │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Transaction Commits                                                     │
│  - Business data and messages committed atomically                       │
│  - IBusOutboxNotification.Delivered() signals delivery service           │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  BusOutboxDeliveryService (Background)                                   │
│  - Queries for undelivered OutboxState records                           │
│  - Acquires distributed lock (updates LockId)                            │
│  - Loads and delivers OutboxMessage records                              │
│  - Updates LastSequenceNumber after each delivery                        │
│  - Marks OutboxState.Delivered when complete                             │
│  - Removes delivered messages                                            │
└─────────────────────────────────────────────────────────────────────────┘
```

## Implementation Variants

### In-Memory Outbox

**Use Case:** Testing, development, and scenarios where durability is not required.

**Key Files:**
- `src/MassTransit/Middleware/Outbox/InMemoryOutboxMessageRepository.cs`
- `src/MassTransit/Middleware/Outbox/InMemoryInboxMessage.cs`
- `src/MassTransit/Middleware/Outbox/InMemoryOutboxConsumeContext.cs`
- `src/MassTransit/Middleware/Outbox/InMemoryOutboxContextFactory.cs`

**Characteristics:**
- Uses `Dictionary<InMemoryInboxMessageKey, InMemoryInboxMessage>` for storage
- `SemaphoreSlim` for per-message concurrency control
- Messages delivered synchronously after consumer completes
- No background delivery service required
- Does not support Bus Outbox (producer-side)

**Locking Mechanism:**
```csharp
// InMemoryInboxMessage.cs
SemaphoreSlim _inUse = new SemaphoreSlim(1, 1);

public async Task Lock(CancellationToken cancellationToken)
{
    await _inUse.WaitAsync(cancellationToken);
}

public void Release()
{
    _inUse.Release();
}
```

### Entity Framework Core Outbox

**Use Case:** Production deployments with SQL databases.

**Key Files:**
- `src/Persistence/MassTransit.EntityFrameworkCoreIntegration/EntityFrameworkCoreIntegration/OutboxState.cs`
- `src/Persistence/MassTransit.EntityFrameworkCoreIntegration/EntityFrameworkCoreIntegration/OutboxMessage.cs`
- `src/Persistence/MassTransit.EntityFrameworkCoreIntegration/EntityFrameworkCoreIntegration/InboxState.cs`
- `src/Persistence/MassTransit.EntityFrameworkCoreIntegration/EntityFrameworkCoreIntegration/EntityFrameworkOutboxContextFactory.cs`
- `src/Persistence/MassTransit.EntityFrameworkCoreIntegration/EntityFrameworkCoreIntegration/BusOutboxDeliveryService.cs`

#### Database Schema

**InboxState** (Idempotency tracking for consumed messages):

| Column | Type | Purpose |
|--------|------|---------|
| Id | long | Clustered index primary key |
| MessageId | Guid | Part of composite unique key |
| ConsumerId | Guid | Part of composite unique key |
| LockId | Guid | Distributed lock token |
| RowVersion | byte[] | Optimistic concurrency |
| Received | DateTime | When message was first received |
| ReceiveCount | int | Delivery attempt counter |
| Consumed | DateTime? | When consumer completed |
| Delivered | DateTime? | When outbox messages delivered |
| LastSequenceNumber | long? | Last delivered message sequence |
| ExpirationTime | DateTime? | Message TTL |

**OutboxState** (Transaction-level outbox tracking for Bus Outbox):

| Column | Type | Purpose |
|--------|------|---------|
| OutboxId | Guid | Unique identifier per transaction |
| LockId | Guid | Distributed lock token |
| RowVersion | byte[] | Optimistic concurrency |
| Created | DateTime | Transaction timestamp |
| Delivered | DateTime? | When all messages delivered |
| LastSequenceNumber | long? | Last delivered message sequence |

**OutboxMessage** (Stored messages awaiting delivery):

| Column | Type | Purpose |
|--------|------|---------|
| SequenceNumber | long | Ordering identifier |
| InboxMessageId | Guid? | FK for consumer-side outbox |
| InboxConsumerId | Guid? | FK for consumer-side outbox |
| OutboxId | Guid? | FK for bus outbox |
| MessageId | Guid | Original message ID |
| ContentType | string | Serialization format |
| MessageType | string | CLR type name |
| Body | string | JSON serialized message |
| Headers | string | JSON serialized headers |
| Properties | string | Transport-specific metadata |
| DestinationAddress | string | Target endpoint |
| And more... | | Additional addressing and scheduling fields |

#### Lock Statement Providers

Database-specific row locking is implemented via `ILockStatementProvider`:

**Path:** `src/Persistence/MassTransit.EntityFrameworkCoreIntegration/EntityFrameworkCoreIntegration/ILockStatementProvider.cs`

Implementations for:
- SQL Server (`SqlServerLockStatementProvider`)
- PostgreSQL (`PostgresLockStatementProvider`)
- MySQL (`MySqlLockStatementProvider`)
- Oracle (`OracleLockStatementProvider`)
- SQLite (`SqliteLockStatementProvider`)

Each provider generates database-specific `FOR UPDATE` or `WITH (UPDLOCK)` clauses.

#### Background Delivery Service

**Path:** `src/Persistence/MassTransit.EntityFrameworkCoreIntegration/EntityFrameworkCoreIntegration/BusOutboxDeliveryService.cs`

The `BusOutboxDeliveryService<TDbContext>` is a hosted service that:

1. Waits for the bus to be healthy
2. Queries for undelivered `OutboxState` records
3. Acquires distributed lock by updating `LockId`
4. Loads and delivers messages in batches (configurable via `MessageDeliveryLimit`)
5. Updates `LastSequenceNumber` after each successful delivery
6. Marks `OutboxState.Delivered` when complete
7. Removes delivered messages
8. Uses adaptive query batching via `RequestRateAlgorithm`

### MongoDB Outbox

**Use Case:** Production deployments with MongoDB.

**Key Files:**
- `src/Persistence/MassTransit.MongoDbIntegration/MongoDbIntegration/Outbox/OutboxState.cs`
- `src/Persistence/MassTransit.MongoDbIntegration/MongoDbIntegration/Outbox/OutboxMessage.cs`
- `src/Persistence/MassTransit.MongoDbIntegration/MongoDbIntegration/BusOutboxDeliveryService.cs`

**Characteristics:**
- Uses optimistic locking with `Version` field instead of row locks
- `ObjectId.GenerateNewId()` as lock token
- Leverages MongoDB transactions (4.0+)
- Similar architecture to EF Core implementation

## Message Serialization

When a message is captured by the outbox, it is serialized for storage:

```csharp
// Captured during AddSend<T>()
var outboxMessage = new OutboxMessage
{
    MessageId = context.MessageId,
    ContentType = context.ContentType.ToString(),
    MessageType = string.Join(";", context.SupportedMessageTypes),
    Body = JsonSerializer.Serialize(context.Message),
    Headers = JsonSerializer.Serialize(headers),
    Properties = JsonSerializer.Serialize(transportProperties),
    DestinationAddress = context.DestinationAddress.ToString(),
    // ... additional fields
};
```

On delivery, the message is reconstructed via `OutboxMessageSendPipe`:

**Path:** `src/MassTransit/Middleware/OutboxMessageSendPipe.cs`

- Deserializes message body from JSON
- Reconstructs headers via `OutboxMessageHeaderProvider`
- Restores routing metadata
- Preserves message IDs and correlation data

## Endpoint Interception

The outbox intercepts send and publish operations through custom endpoint providers:

**OutboxSendEndpointProvider** (`src/MassTransit/Middleware/Outbox/OutboxSendEndpointProvider.cs`)
- Wraps the real send endpoint provider
- Routes messages through the outbox instead of sending directly

**OutboxPublishEndpointProvider** (`src/MassTransit/Middleware/Outbox/OutboxPublishEndpointProvider.cs`)
- Wraps the real publish endpoint provider
- Routes published messages through the outbox

**OutboxSendEndpoint** (`src/MassTransit/Middleware/Outbox/OutboxSendEndpoint.cs`)
- Intercepts all Send/Publish calls
- Calls `AddSend()` instead of directly sending
- Messages are batched for later delivery

## Configuration

### In-Memory Outbox

```csharp
services.AddMassTransit(x =>
{
    x.AddConsumer<MyConsumer>();

    x.UsingRabbitMq((context, cfg) =>
    {
        cfg.ReceiveEndpoint("my-queue", e =>
        {
            e.UseInMemoryOutbox();
            e.ConfigureConsumer<MyConsumer>(context);
        });
    });
});
```

**Extension Methods:** `src/MassTransit/Configuration/InMemoryOutboxConfigurationExtensions.cs`

### Entity Framework Core Outbox

```csharp
services.AddMassTransit(x =>
{
    x.AddEntityFrameworkOutbox<MyDbContext>(o =>
    {
        // Use SQL Server lock statements
        o.UseSqlServer();

        // Optionally configure bus outbox for producer-side
        o.UseBusOutbox();
    });

    x.AddConsumer<MyConsumer>();

    x.UsingRabbitMq((context, cfg) =>
    {
        cfg.ReceiveEndpoint("my-queue", e =>
        {
            e.UseEntityFrameworkOutbox<MyDbContext>(context);
            e.ConfigureConsumer<MyConsumer>(context);
        });
    });
});
```

### Outbox Delivery Options

**Path:** `src/MassTransit/Configuration/DependencyInjection/OutboxDeliveryServiceOptions.cs`

| Option | Default | Description |
|--------|---------|-------------|
| MessageDeliveryLimit | 100 | Messages per delivery batch |
| MessageDeliveryTimeout | 5 seconds | Send timeout per message |
| QueryDelay | 5 seconds | Polling interval between sweeps |
| QueryMessageLimit | 100 | Concurrent outboxes to process |
| QueryTimeout | 30 seconds | Database query timeout |

## Key Architectural Patterns

### Transactional Outbox Pattern

Messages are queued in the same database transaction as business logic:

```
┌─────────────────────────────────────────┐
│           Database Transaction          │
│  ┌──────────────────────────────────┐   │
│  │  Business Data Changes           │   │
│  └──────────────────────────────────┘   │
│  ┌──────────────────────────────────┐   │
│  │  OutboxMessage Records           │   │
│  └──────────────────────────────────┘   │
│  ┌──────────────────────────────────┐   │
│  │  InboxState/OutboxState Update   │   │
│  └──────────────────────────────────┘   │
└─────────────────────────────────────────┘
                    │
                    ▼
              COMMIT (atomic)
```

### Idempotency Pattern

The inbox tracks consumed messages to ensure exactly-once processing:

1. First delivery: Consumer executes, `InboxState.Consumed` set
2. Retry delivery: Consumer skipped, cached outbox messages delivered
3. Subsequent retries: Outbox messages already delivered, skip to cleanup

The composite key `(MessageId, ConsumerId)` ensures each consumer processes each message exactly once.

### Distributed Locking

Multi-instance safety is achieved through database locks:

- **EF Core**: Row locks via `ILockStatementProvider` (database-specific SQL)
- **MongoDB**: Optimistic locking with version checks
- **In-Memory**: `SemaphoreSlim` per inbox message

Lock tokens are updated to assert ownership and detect stale locks.

### Sequence Number Tracking

Messages are assigned sequence numbers for ordered delivery:

```csharp
// During capture
message.SequenceNumber = Interlocked.Increment(ref _sequenceNumber);

// During delivery
foreach (var message in messages.OrderBy(m => m.SequenceNumber))
{
    await Deliver(message);
    await UpdateLastSequenceNumber(message.SequenceNumber);
}
```

This enables recovery from partial delivery failures - on retry, only messages after `LastSequenceNumber` are delivered.

## Notification System

**Path:** `src/MassTransit/Middleware/Outbox/IBusOutboxNotification.cs`

The notification system prevents busy polling:

- `WaitForDelivery(CancellationToken)` - Blocks delivery service during idle periods
- `Delivered()` - Signals when new messages are available

```csharp
// In EntityFrameworkScopedBusContext, after transaction commit:
_notification.Delivered();

// In BusOutboxDeliveryService:
await _notification.WaitForDelivery(stoppingToken);
```

## Comparison Matrix

| Aspect | In-Memory | Entity Framework | MongoDB |
|--------|-----------|------------------|---------|
| Storage | Dictionary | SQL Database | MongoDB Collections |
| Locking | SemaphoreSlim | Row Locks (DB-specific) | Optimistic Version |
| Delivery | Synchronous | Async Background Service | Async Background Service |
| Inbox Pattern | Yes | Yes | Yes |
| Bus Outbox | No | Yes | Yes |
| Durability | Memory only | Durable | Durable |
| Use Case | Testing/Dev | Production | Production |

## Related Test Files

- `tests/MassTransit.Tests/Outbox_Specs.cs` - In-memory outbox behavior
- `tests/MassTransit.EntityFrameworkCoreIntegration.Tests/ReliableMessaging/Outbox_Specs.cs` - Consumer-side outbox
- `tests/MassTransit.EntityFrameworkCoreIntegration.Tests/ReliableMessaging/BusOutbox_Specs.cs` - Bus outbox with test harness
