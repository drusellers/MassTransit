# MassTransit Transactional Outbox Research

## 1. What is the Transactional Outbox Pattern and What Problem Does It Solve?

### Problem it Solves

The transactional outbox solves the distributed transaction problem when publishing messages while updating local state. Without the outbox pattern:
- A message could be published to a broker before the database transaction commits, causing message loss on transaction rollback
- A database transaction could commit without publishing messages if a network error occurs after the DB save but before the publish

### The Pattern

The outbox ensures atomicity by:
1. Messages are stored in the same database transaction as business state
2. A background delivery service picks up these messages and delivers them to the message broker
3. Delivery is tracked to prevent duplicates
4. If delivery fails, the message remains in the outbox for retry

---

## 2. Architectural Structure (Interfaces, Base Classes, Key Abstractions)

### Core Interfaces

| Interface | Location | Purpose |
|-----------|----------|---------|
| `IOutboxContextFactory<TContext>` | `Middleware/IOutboxContextFactory.cs` | Factory for creating outbox-aware consume contexts |
| `OutboxConsumeContext<TMessage>` | `Middleware/OutboxConsumeContext.cs` | Transactional outbox context combining ConsumeContext and OutboxSendContext |
| `OutboxSendContext` | `Middleware/OutboxSendContext.cs` | Interface for adding messages to the outbox |
| `OutboxMessageContext` | `Middleware/OutboxMessageContext.cs` | Interface for stored outbox messages with metadata |
| `IBusOutboxNotification` | `Middleware/Outbox/IBusOutboxNotification.cs` | Notifies delivery service of pending messages |
| `IOutboxConfigurator` | `Configuration/IOutboxConfigurator.cs` | Configures outbox behavior (concurrent delivery) |
| `IBusOutboxConfigurator` | `Configuration/DependencyInjection/IBusOutboxConfigurator.cs` | Configures bus-level outbox options |
| `ITransactionalOutboxConfigurator` | `Configuration/DependencyInjection/ITransactionalOutboxConfigurator.cs` | Marker interface for transactional outbox |

### Base Classes & Implementations

| Class | Location | Purpose |
|-------|----------|---------|
| `OutboxConsumeContextProxy<TMessage>` | `Middleware/Outbox/OutboxConsumeContextProxy.cs` | Abstract base for database-specific outbox contexts |
| `DbContextOutboxConsumeContext<TDbContext, TMessage>` | `EntityFrameworkCoreIntegration/DbContextOutboxConsumeContext.cs` | EF Core implementation |
| `MongoDbOutboxConsumeContext<TMessage>` | `MongoDbIntegration/Outbox/MongoDbOutboxConsumeContext.cs` | MongoDB implementation |
| `InMemoryOutboxConsumeContext` | `Middleware/InMemoryOutbox/InMemoryOutboxConsumeContext.cs` | In-memory implementation (testing) |
| `OutboxConsumeFilter<TContext, TMessage>` | `Middleware/OutboxConsumeFilter.cs` | Pipeline filter that invokes outbox context factory |
| `OutboxMessagePipe<TMessage>` | `Middleware/OutboxMessagePipe.cs` | Handles message delivery and outbox lifecycle (3 states) |

### Lifecycle States (in OutboxMessagePipe)

```
State 1: IsMessageConsumed = false → Execute consumer, set consumed
State 2: IsOutboxDelivered = false → Deliver outbox messages
State 3: IsOutboxDelivered = true → Remove outbox messages
```

---

## 3. Outbox Integration with Message Publishing/Sending

### Integration Pattern

The outbox intercepts publish and send operations through **endpoint providers**:

| Provider | Location | Implementation |
|----------|----------|-----------------|
| `OutboxSendEndpointProvider` | `Middleware/Outbox/OutboxSendEndpointProvider.cs` | Wraps ISendEndpointProvider to route sends through OutboxSendEndpoint |
| `OutboxPublishEndpointProvider` | `Middleware/Outbox/OutboxPublishEndpointProvider.cs` | Wraps IPublishEndpointProvider for publish operations |
| `OutboxSendEndpoint` | `Middleware/Outbox/OutboxSendEndpoint.cs` | Terminal send endpoint that captures messages to the outbox |

### Flow for Bus Outbox (on-ramp messages)

1. Application calls `await bus.Publish()` or `await endpoint.Send()`
2. `EntityFrameworkScopedBusContext` (or MongoDB equivalent) provides wrapped endpoints
3. `OutboxSendEndpointProvider` → `OutboxSendEndpoint` intercepts the call
4. `OutboxSendEndpoint.AddSend()` captures the SendContext
5. Message serialized and stored in `OutboxMessage` table with `OutboxId`
6. `BusOutboxDeliveryService` (background service) picks up messages by `OutboxId`

### Flow for Consumer Outbox (in-order, per-consumer)

1. Message arrives at consumer
2. `OutboxConsumeFilter` creates `DbContextOutboxConsumeContext` (or MongoDB equivalent)
3. Consumer publishes/sends during execution
4. Messages stored in `OutboxMessage` with `InboxMessageId + InboxConsumerId`
5. `OutboxMessagePipe` delivers messages after consumer completes

---

## 4. Message Storage in the Outbox (Entities/Tables)

### OutboxMessage Entity

Location: `EntityFrameworkCoreIntegration/OutboxMessage.cs`

```csharp
public class OutboxMessage : OutboxMessageContext
{
    // Routing & Identity
    public long SequenceNumber { get; set; }  // Primary Key
    public Guid MessageId { get; set; }

    // Message Addresses
    public Uri? SourceAddress { get; set; }
    public Uri? DestinationAddress { get; set; }
    public Uri? ResponseAddress { get; set; }
    public Uri? FaultAddress { get; set; }

    // Correlation IDs
    public Guid? ConversationId { get; set; }
    public Guid? CorrelationId { get; set; }
    public Guid? InitiatorId { get; set; }
    public Guid? RequestId { get; set; }

    // Message Content
    public string ContentType { get; set; }
    public string MessageType { get; set; }
    public string Body { get; set; }
    public string? Headers { get; set; }
    public string? Properties { get; set; }

    // Timing
    public DateTime SentTime { get; set; }
    public DateTime? EnqueueTime { get; set; }  // Delayed delivery
    public DateTime? ExpirationTime { get; set; }

    // Consumer Outbox (per-consumer tracking)
    public Guid? InboxMessageId { get; set; }
    public Guid? InboxConsumerId { get; set; }
    // Index: (InboxMessageId, InboxConsumerId, SequenceNumber) UNIQUE

    // Bus Outbox (on-ramp tracking)
    public Guid? OutboxId { get; set; }
    // Index: (OutboxId, SequenceNumber) UNIQUE
}
```

### OutboxState Entity

Location: `EntityFrameworkCoreIntegration/OutboxState.cs`

Tracks delivery state for bus outbox messages:

```csharp
public class OutboxState
{
    public Guid OutboxId { get; set; }  // Primary Key
    public Guid LockId { get; set; }    // Pessimistic lock during sweep
    public DateTime Created { get; set; }
    public DateTime? Delivered { get; set; }
    public long? LastSequenceNumber { get; set; }  // Last delivered sequence
    public string? BusName { get; set; }  // For MultiBus
    public byte[]? RowVersion { get; set; }  // Concurrency token
}
```

### InboxState Entity

Location: `EntityFrameworkCoreIntegration/InboxState.cs`

Tracks idempotency for consumer message processing:

```csharp
public class InboxState
{
    public long Id { get; set; }  // Primary Key (clustered)
    public Guid MessageId { get; set; }
    public Guid ConsumerId { get; set; }
    // Alternate Key: (MessageId, ConsumerId)

    public Guid LockId { get; set; }
    public DateTime Received { get; set; }
    public int ReceiveCount { get; set; }

    public DateTime? ExpirationTime { get; set; }
    public DateTime? Consumed { get; set; }
    public DateTime? Delivered { get; set; }
    public long? LastSequenceNumber { get; set; }  // Last delivered outbox msg
    public byte[]? RowVersion { get; set; }  // Concurrency token
}
```

### Entity Registration

Location: `EntityFrameworkOutboxConfigurationExtensions.cs:229-410`

- `AddTransactionalOutboxEntities()` - Registers all three entities
- `AddInboxStateEntity()`, `AddOutboxStateEntity()`, `AddOutboxMessageEntity()` - Individual registration

### Database Indexes

- `OutboxState`: (BusName, Created) - For sweeper to find pending outboxes
- `OutboxMessage`: (OutboxId, SequenceNumber) UNIQUE - For bus outbox delivery
- `OutboxMessage`: (InboxMessageId, InboxConsumerId, SequenceNumber) UNIQUE - For consumer outbox
- `InboxState`: (Delivered) - For cleanup service

---

## 5. Message Delivery from the Outbox

### Delivery Service: BusOutboxDeliveryService

Location: `EntityFrameworkCoreIntegration/BusOutboxDeliveryService.cs`

### Two-Tier Architecture

**Tier 1: Background Service (`BackgroundService`)**
- Runs as `IHostedService`
- Infinite loop with request rate algorithm
- Waits for health status = Healthy
- Implements exponential retry (1s, 3s, 30s)
- Uses `IBusOutboxNotification` to wait between sweeps

**Tier 2: DeliverOutbox() Method (Lines 106-195)**

Delivers messages from a single OutboxState:

1. **Lock Acquisition** (Lines 127-139):
   ```csharp
   // Get pending outbox (locked by BusName/Created index)
   List<OutboxState> outboxStateList = await dbContext.Set<OutboxState>()
       .FromSqlRaw(_getOutboxIdStatement)  // Database-specific lock
       .AsTracking()
       .ToListAsync();

   // Acquire lock with LockId
   outboxState.LockId = lockId;
   await dbContext.SaveChangesAsync();
   ```

2. **Message Loading** (Lines 229-231):
   ```csharp
   // Load messages for this outbox since last sequence number
   IList<OutboxMessage> messages = await _outboxMessagesQuery(
       dbContext, outboxState.OutboxId, lastSequenceNumber, messageLimit)
   ```

3. **Message Delivery** (Lines 239-298):
   - Loops through messages up to `MessageDeliveryLimit`
   - Creates `OutboxMessageSendPipe` to reconstruct message
   - Gets send endpoint for destination
   - Sends with timeout
   - Removes message on success
   - Breaks loop on failure (partial delivery)

4. **State Update** (Lines 300-323):
   ```csharp
   // Track last successful sequence
   outboxState.LastSequenceNumber = sentSequenceNumber;

   // Mark delivered when all messages sent
   if (messageIndex == messages.Count && messages.Count < messageLimit)
   {
       outboxState.Delivered = DateTime.UtcNow;
       // Remove state if no prior delivery tracking
       if (!hasLastSequenceNumber)
           dbContext.Remove(outboxState);
   }
   ```

5. **Transaction Commit** (Line 152):
   - All changes committed atomically
   - Concurrency exceptions handled gracefully

### Database-Specific Lock Statements

| Provider | Implementation |
|----------|-----------------|
| SQL Server | `SqlServerLockStatementProvider` - Row locks |
| PostgreSQL | `PostgresLockStatementProvider` - SELECT FOR UPDATE |
| MySQL | `MySqlLockStatementProvider` - Row locks |
| SQLite | `SqliteLockStatementProvider` - Simulated locks |
| Oracle | `OracleLockStatementProvider` - Row locks |

### Delivery Service Options

Location: `Configuration/DependencyInjection/OutboxDeliveryServiceOptions.cs`

```csharp
MessageDeliveryLimit = 100       // Messages per sweep iteration
MessageDeliveryTimeout = 5s      // Timeout for individual message send
QueryDelay = 5s                  // Delay between sweep iterations
QueryMessageLimit = 100          // Max concurrent outbox records
QueryTimeout = 30s               // DB query timeout
BusName = null                   // Bus identifier for MultiBus
```

---

## 6. Inbox Work (Idempotency/Deduplication)

### InboxCleanupService

Location: `EntityFrameworkCoreIntegration/InboxCleanupService.cs`

Purpose: Remove old `InboxState` records after consumer completes.

### Lifecycle

1. Message arrives → `InboxState` created (if first time)
2. Consumer execution → `InboxState.Consumed` set
3. Outbox messages delivered → `InboxState.Delivered` set
4. `InboxCleanupService` removes after `DuplicateDetectionWindow` (default 30 min)

### Cleanup Logic (Lines 68-96)

```csharp
// Find expired delivered messages
var removeTimestamp = DateTime.UtcNow - DuplicateDetectionWindow;

var count = await dbContext.Set<InboxState>()
    .Where(x => x.Delivered != null && x.Delivered.Value < removeTimestamp)
    .OrderBy(x => x.Delivered)
    .Take(QueryMessageLimit)
    .ExecuteDeleteAsync();
```

### Idempotency Enforcement

- `InboxState` alternate key: `(MessageId, ConsumerId)` ensures no duplicates
- First arrival: Creates new record, increments `ReceiveCount = 0`
- Retries: Finds existing record, increments `ReceiveCount`
- Consumer only executes if `Consumed` is null
- Redelivery prevented by checking `Delivered` timestamp

### ConsumerId Calculation

Generated per endpoint/consumer type using MD5 hash in `JobMetadataCache<TConsumer, TMessage>.GenerateJobTypeId()`

---

## 7. Database Providers Supported

### EF Core Providers

1. **SQL Server** - `Persistence/MassTransit.EntityFrameworkCoreIntegration`
   - Configuration: `UseSqlServer()` with optional schema caching

2. **PostgreSQL** - `Persistence/MassTransit.EntityFrameworkCoreIntegration`
   - Configuration: `UsePostgres()` with optional schema caching

3. **MySQL** - `Persistence/MassTransit.EntityFrameworkCoreIntegration`
   - Configuration: `UseMySql()` with optional schema caching

4. **SQLite** - `Persistence/MassTransit.EntityFrameworkCoreIntegration`
   - Configuration: `UseSqlite()` with optional schema caching

5. **Oracle** - `Persistence/MassTransit.EntityFrameworkCoreIntegration`
   - Configuration: `UseOracle()` with optional schema caching

### Non-EF Providers

1. **MongoDB** - `Persistence/MassTransit.MongoDbIntegration`
   - Outbox: `OutboxMessage` collection with sequence counter
   - Inbox: `InboxState` document
   - Configuration: `MongoDbOutboxConfigurator`

2. **In-Memory** - `Middleware/InMemoryOutbox` & `Middleware/Outbox`
   - `InMemoryOutboxMessageRepository` - Thread-safe dictionary
   - `InMemoryInboxMessage` - In-memory message tracking
   - Use: Testing, development (NOT for production)

### Configurator Pattern

```csharp
// Generic configurator
public class EntityFrameworkOutboxConfigurator<TBus, TDbContext>
public class MongoDbOutboxConfigurator<TBus, TDbContext>
public class EntityFrameworkOutboxConfigurator<TDbContext>  // Default IBus
```

---

## 8. Outbox Configuration

### Configuration Hierarchy

**Level 1: Bus Registration**

Location: `EntityFrameworkOutboxConfigurationExtensions.cs:22-48`

```csharp
services.AddMassTransitTestHarness(x =>
{
    x.AddEntityFrameworkOutbox<DbContext>(o =>
    {
        o.QueryDelay = TimeSpan.FromSeconds(1);
        o.QueryMessageLimit = 100;
        o.QueryTimeout = TimeSpan.FromSeconds(30);
        o.DuplicateDetectionWindow = TimeSpan.FromMinutes(30);
        o.IsolationLevel = IsolationLevel.RepeatableRead;
        o.LockStatementProvider = new SqlServerLockStatementProvider();
        o.DisableInboxCleanupService();

        o.UseBusOutbox(bo =>
        {
            bo.MessageDeliveryLimit = 100;
            bo.MessageDeliveryTimeout = TimeSpan.FromSeconds(10);
            bo.DisableDeliveryService();
        });
    });
});
```

**Level 2: Receive Endpoint**

Location: `EntityFrameworkOutboxConfigurationExtensions.cs:56-71`

```csharp
endpoint.UseEntityFrameworkOutbox<DbContext>(context, c =>
{
    c.MessageDeliveryLimit = 10;
    c.MessageDeliveryTimeout = TimeSpan.FromSeconds(30);
});
```

**Level 3: In-Memory Outbox**

Location: `InMemoryOutboxConfigurationExtensions.cs:22-34`

```csharp
configurator.UseInMemoryOutbox<TMessage>(context, cfg =>
{
    cfg.ConcurrentMessageDelivery = true;
});
```

### Configuration Classes

| Class | Location | Purpose |
|-------|----------|---------|
| `EntityFrameworkOutboxConfigurator<TDbContext>` | `Configuration/Configuration/EntityFrameworkOutboxConfigurator.cs` | Bus-level config |
| `EntityFrameworkBusOutboxConfigurator<TDbContext>` | `Configuration/Configuration/EntityFrameworkBusOutboxConfigurator.cs` | Bus outbox config |
| `EntityFrameworkOutboxOptions<TDbContext>` | `EntityFrameworkCoreIntegration/EntityFrameworkOutboxOptions.cs` | Options object |
| `OutboxDeliveryServiceOptions` | `Configuration/DependencyInjection/OutboxDeliveryServiceOptions.cs` | Delivery service options |
| `InboxCleanupServiceOptions<TDbContext>` | `Configuration/DependencyInjection/InboxCleanupServiceOptions.cs` | Cleanup service options |

### Entity Configuration (Lines 229-410)

```csharp
modelBuilder.AddTransactionalOutboxEntities();
// Or individually:
modelBuilder.AddInboxStateEntity();
modelBuilder.AddOutboxStateEntity();
modelBuilder.AddOutboxMessageEntity();
```

---

## 9. Message Lifecycle Through the Outbox

### Bus Outbox (on-ramp) Lifecycle

```
1. Application Context
   └─ await bus.Publish(msg)
   └─ EntityFrameworkScopedBusContext.PublishEndpoint
      └─ OutboxPublishEndpointProvider.GetPublishSendEndpoint()
         └─ OutboxSendEndpoint.Send()
            └─ OutboxSendEndpoint.AddSend(context)
               └─ EntityFrameworkScopedBusContext.AddSend()
                  └─ DbSet<OutboxMessage>.Add() [in same transaction]
                  └─ DbSet<OutboxState>.Add() [creates or reuses state]
   └─ await dbContext.SaveChangesAsync()  [ATOMIC]

2. Background Delivery Loop (BusOutboxDeliveryService)
   └─ Get lock on OutboxState (by BusName, Created)
   └─ Load OutboxMessages where SequenceNumber > LastSequenceNumber
   └─ For each message:
      ├─ Get send endpoint for DestinationAddress
      ├─ Create OutboxMessageSendPipe (deserializes)
      ├─ await endpoint.Send(SerializedMessageBody, pipe)
      ├─ Remove message from database
      └─ Update OutboxState.LastSequenceNumber
   └─ If all messages sent:
      └─ Set OutboxState.Delivered = UtcNow
      └─ Optionally remove OutboxState if no prior delivery
   └─ Commit transaction

3. Cleanup (implicit when OutboxState.Delivered != null)
   └─ OutboxState.RowVersion incremented (optimistic lock)
   └─ No explicit cleanup service for bus outbox
```

### Consumer Outbox (per-consumer) Lifecycle

```
1. Message Received
   └─ Create InboxState (MessageId, ConsumerId)
      └─ Or increment ReceiveCount if exists
   └─ Commit transaction (within session lock)

2. OutboxConsumeFilter<TContext, TMessage>
   └─ Create DbContextOutboxConsumeContext
   └─ Push into service scope
   └─ Call OutboxMessagePipe.Send()

3. OutboxMessagePipe - 3 States

   State 1: IsMessageConsumed = false (first execution)
   ├─ Call consumer handler
   ├─ await context.ConsumeCompleted
   ├─ await context.SetConsumed()  [InboxState.Consumed = UtcNow]
   └─ Return from pipeline

   State 2: IsOutboxDelivered = false (retry)
   ├─ Load OutboxMessages
   │  └─ WHERE InboxMessageId = messageId
   │      AND InboxConsumerId = consumerId
   │      AND SequenceNumber > LastSequenceNumber
   ├─ Deliver to destinations (batch up to MessageDeliveryLimit)
   ├─ Update InboxState.LastSequenceNumber
   ├─ On completion: await context.SetDelivered()
   │  └─ InboxState.Delivered = UtcNow
   └─ Return from pipeline

   State 3: IsOutboxDelivered = true (cleanup)
   ├─ Remove OutboxMessages
   │  └─ WHERE InboxMessageId = messageId
   │      AND InboxConsumerId = consumerId
   └─ Return from pipeline

4. InboxCleanupService
   └─ Periodically runs (QueryDelay = 10s default)
   └─ Find InboxState where Delivered < (UtcNow - DuplicateDetectionWindow)
   └─ Delete (default window: 30 minutes)
```

---

## 10. Key Code Patterns, Middleware/Filters, and Extension Points

### Middleware Pipeline Filters

| Filter | Location | Responsibility |
|--------|----------|-----------------|
| `OutboxConsumeFilter<TContext, TMessage>` | `Middleware/OutboxConsumeFilter.cs` | Entry point; creates outbox context via factory |
| `OutboxMessagePipe<TMessage>` | `Middleware/OutboxMessagePipe.cs` | Manages 3-state lifecycle (consume → deliver → cleanup) |
| `InMemoryOutboxFilter<TContext, TResult>` | `Middleware/InMemoryOutboxFilter.cs` | In-memory variant; executes pending actions on completion |

### Extension Methods Pattern

```csharp
// Receive Endpoint Configuration
endpoint.UseEntityFrameworkOutbox<DbContext>(context, cfg =>
{
    cfg.MessageDeliveryLimit = 10;
});

endpoint.UseInMemoryInboxOutbox(context);

// Bus Configuration
bus.AddEntityFrameworkOutbox<DbContext>(cfg =>
{
    cfg.UseBusOutbox(bo => bo.MessageDeliveryLimit = 100);
});

// Entity Configuration
modelBuilder.AddTransactionalOutboxEntities();
```

### Factory Pattern

`IOutboxContextFactory<TContext>` implementations are resolved per-request:

- **EF Core**: `EntityFrameworkOutboxContextFactory<TDbContext>` creates `DbContextOutboxConsumeContext<TDbContext, TMessage>`
- **MongoDB**: Factory creates `MongoDbOutboxConsumeContext<TMessage>`
- **In-Memory**: Factory creates `InMemoryOutboxConsumeContext<TMessage>`

### Endpoint Provider Wrapping

```csharp
// Original endpoint chain:
ISendEndpointProvider → GetSendEndpoint(address) → ISendEndpoint

// Outbox-wrapped chain:
OutboxSendEndpointProvider
  → GetSendEndpoint(address)
  → OutboxSendEndpoint(context, endpoint)
  → endpoint.Send()
  → context.AddSend(sendContext)  [stores in outbox]
```

### Observer Pattern

`OutboxConsumePipeSpecificationObserver<TContext>` implements:
- `IConsumerConfigurationObserver`
- `ISagaConfigurationObserver`
- `IActivityConfigurationObserver`

Listens to consumer/saga configuration and injects `OutboxConsumeFilter` into each message type's pipeline.

### Scoped Bus Context Pattern

`IScopedBusContextProvider<TBus>` determines which `ScopedBusContext` to use:
- Has active `ConsumeContext` → `ConsumeContextScopedBusContext`
- In consumer scope → `EntityFrameworkConsumeContextScopedBusContext`
- Background context → `EntityFrameworkScopedBusContext` (bus outbox)

Each provides wrapped `SendEndpointProvider` and `PublishEndpoint` that route through the outbox.

### Notification Pattern

`IBusOutboxNotification` allows background delivery service to signal when messages are available:

```csharp
// In EntityFrameworkScopedBusContext.Dispose():
if (WasCommitted())
    _notification.Delivered();  // Triggers delivery service sweep

// In BusOutboxDeliveryService.ExecuteAsync():
await _notification.WaitForDelivery(stoppingToken);  // Waits for signal
```

### Serialization/Deserialization

`OutboxMessage` stores serialized body + headers:
- During storage: `context.Serializer.GetMessageBody(context)` captures JSON
- During delivery: `OutboxMessageSendPipe` deserializes using stored ContentType
- Headers reconstructed from stored JSON via `IObjectDeserializer.DeserializeDictionary()`

### Concurrency Control

| Mechanism | Where | How |
|-----------|-------|-----|
| Pessimistic Lock | OutboxState delivery | Database-specific lock statements |
| Optimistic Lock | InboxState / OutboxState | `RowVersion` byte array concurrency token |
| Semaphore | InMemory repository | `SemaphoreSlim` for exclusive access |
| Transaction Isolation | All DB operations | `IsolationLevel.RepeatableRead` (default) |

---

## File Reference Summary

### Core Abstractions

- `Middleware/OutboxConsumeContext.cs` - Main outbox context interface
- `Middleware/IOutboxContextFactory.cs` - Factory interface
- `Middleware/OutboxSendContext.cs` - Send context interface
- `Middleware/OutboxMessageContext.cs` - Message context interface

### Middleware & Filters

- `Middleware/OutboxConsumeFilter.cs` - Entry point filter
- `Middleware/OutboxMessagePipe.cs` - Lifecycle state machine (lines 29-144)
- `Middleware/InMemoryOutboxFilter.cs` - In-memory variant
- `Middleware/Outbox/OutboxSendEndpoint.cs` - Terminal send endpoint
- `Middleware/Outbox/OutboxPublishEndpointProvider.cs` - Publish routing
- `Middleware/Outbox/OutboxSendEndpointProvider.cs` - Send routing

### EF Core Implementation

- `EntityFrameworkCoreIntegration/OutboxMessage.cs` - Message entity
- `EntityFrameworkCoreIntegration/OutboxState.cs` - Delivery state entity
- `EntityFrameworkCoreIntegration/InboxState.cs` - Idempotency state entity
- `EntityFrameworkCoreIntegration/DbContextOutboxConsumeContext.cs` - Outbox context
- `EntityFrameworkCoreIntegration/EntityFrameworkOutboxContextFactory.cs` - Factory (lines 34-139)
- `EntityFrameworkCoreIntegration/BusOutboxDeliveryService.cs` - Background service (lines 106-195, 221-329)
- `EntityFrameworkCoreIntegration/InboxCleanupService.cs` - Cleanup service
- `EntityFrameworkCoreIntegration/EntityFrameworkScopedBusContext.cs` - Bus context (lines 59-83)

### MongoDB Implementation

- `MongoDbIntegration/Outbox/OutboxMessage.cs` - Message document
- `MongoDbIntegration/Outbox/InboxState.cs` - Inbox state document
- `MongoDbIntegration/Outbox/MongoDbOutboxConsumeContext.cs` - Outbox context
- `MongoDbIntegration/Outbox/MongoDbOutboxExtensions.cs` - AddSend extension
- `MongoDbIntegration/MongoDbScopedBusContext.cs` - Bus context

### Configuration

- `EntityFrameworkOutboxConfigurationExtensions.cs` - Extension methods + entity config (lines 229-410)
- `Configuration/EntityFrameworkOutboxConfigurator.cs` - Bus-level configurator
- `Configuration/EntityFrameworkBusOutboxConfigurator.cs` - Bus outbox configurator
- `Configuration/InMemoryOutbox/OutboxConsumePipeSpecificationObserver.cs` - Observer pattern
- `Configuration/InMemoryOutboxConfigurationExtensions.cs` - Usage extensions
- `Configuration/DependencyInjection/OutboxDeliveryServiceOptions.cs` - Service options
- `Configuration/DependencyInjection/InboxCleanupServiceOptions.cs` - Cleanup options

### In-Memory

- `Middleware/InMemoryOutbox/InMemoryOutboxConsumeContext.cs` - In-memory context
- `Middleware/Outbox/InMemoryOutboxMessage.cs` - In-memory message
- `Middleware/Outbox/InMemoryOutboxMessageRepository.cs` - In-memory storage
- `Middleware/Outbox/InMemoryInboxMessage.cs` - In-memory inbox message

### Helper Classes

- `Middleware/Outbox/IBusOutboxNotification.cs` - Notification interface
- `Middleware/Outbox/BusOutboxNotification.cs` - Notification implementation
- `Middleware/OutboxMessageSendPipe.cs` - Message serialization pipe
- `Middleware/Outbox/OutboxConsumeContextProxy.cs` - Base proxy
- `Middleware/InternalOutboxExtensions.cs` - Utility extensions
- `EntityFrameworkCoreIntegration/EntityFrameworkOutboxExtensions.cs` - AddSend extension
- `EntityFrameworkCoreIntegration/EntityFrameworkScopedBusContextProvider.cs` - Bus context provider

### Test Examples

- `tests/MassTransit.EntityFrameworkCoreIntegration.Tests/ReliableMessaging/BusOutbox_Specs.cs` - Integration tests
