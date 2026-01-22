# MultiBus Outbox Implementation Plan

## Problem Statement

The Entity Framework Core outbox implementation does not support MultiBus scenarios. When multiple buses share the same `DbContext`:
- Only one `BusOutboxDeliveryService` instance runs
- Messages from all buses are delivered by a single bus instance (wrong transport)
- The configurators hardcode `IBus` instead of supporting generic `TBus`

## Solution Overview

1. Add `BusName` column to `OutboxState` to identify which bus owns messages
2. Make configurators generic over `TBus`
3. Use `Bind<TBus, T>` pattern for bus-specific DI registrations
4. Create bus-specific delivery services that filter by `BusName`

---

## Files to Modify

### 1. OutboxState Entity
**File:** `src/Persistence/MassTransit.EntityFrameworkCoreIntegration/EntityFrameworkCoreIntegration/OutboxState.cs`

Add property:
```csharp
public string? BusName { get; set; }
```

### 2. Entity Configuration
**File:** `src/Persistence/MassTransit.EntityFrameworkCoreIntegration/Configuration/EntityFrameworkOutboxConfigurationExtensions.cs`

- Add `BusName` property configuration with max length 256
- Add composite index on `(BusName, Created)` for efficient queries
- Add generic overload:
```csharp
public static void AddEntityFrameworkOutbox<TBus, TDbContext>(
    this IBusRegistrationConfigurator<TBus> configurator,
    Action<IEntityFrameworkOutboxConfigurator>? configure = null)
    where TBus : class, IBus
    where TDbContext : DbContext
```

### 3. Outbox Configurator
**File:** `src/Persistence/MassTransit.EntityFrameworkCoreIntegration/Configuration/Configuration/EntityFrameworkOutboxConfigurator.cs`

- Create `EntityFrameworkOutboxConfigurator<TBus, TDbContext>` with generic `TBus`
- Store `BusName` property: `typeof(TBus) == typeof(IBus) ? null : typeof(TBus).Name`
- Keep non-generic version as derived class for backward compatibility

### 4. Bus Outbox Configurator
**File:** `src/Persistence/MassTransit.EntityFrameworkCoreIntegration/Configuration/Configuration/EntityFrameworkBusOutboxConfigurator.cs`

- Create `EntityFrameworkBusOutboxConfigurator<TBus, TDbContext>` with generic `TBus`
- Change registration from `IScopedBusContextProvider<IBus>` to `IScopedBusContextProvider<TBus>`
- Register `Bind<TBus, IBusOutboxNotification>` singleton per bus
- Register `BusOutboxDeliveryService<TBus, TDbContext>` hosted service per bus
- Register `OutboxDeliveryServiceOptions<TBus>` options per bus

### 5. Bus Outbox Delivery Service
**File:** `src/Persistence/MassTransit.EntityFrameworkCoreIntegration/EntityFrameworkCoreIntegration/BusOutboxDeliveryService.cs`

- Change to `BusOutboxDeliveryService<TBus, TDbContext>` with generic `TBus`
- Inject `Bind<TBus, IBusControl>` instead of `IBusControl`
- Inject `Bind<TBus, IBusOutboxNotification>` for bus-specific notification
- Inject `IOptions<OutboxDeliveryServiceOptions<TBus>>` for bus-specific options
- Filter outbox queries by `BusName` column

### 6. Lock Statement Provider
**File:** `src/Persistence/MassTransit.EntityFrameworkCoreIntegration/EntityFrameworkCoreIntegration/ILockStatementProvider.cs`

Add overload:
```csharp
string GetOutboxStatement(DbContext context, string? busName = null);
```

### 7. SQL Lock Statement Implementations
**Files:**
- `SqlServerLockStatementProvider.cs`
- `PostgresLockStatementProvider.cs`
- `MySqlLockStatementProvider.cs`
- Other providers...

Update `GetOutboxStatement` to include `WHERE BusName = @busName` (or `IS NULL` for default bus)

### 8. Scoped Bus Context
**File:** `src/Persistence/MassTransit.EntityFrameworkCoreIntegration/EntityFrameworkCoreIntegration/EntityFrameworkScopedBusContext.cs`

Set `BusName` when creating `OutboxState`:
```csharp
_outboxState ??= _dbContext.Add(new OutboxState
{
    OutboxId = _outboxId,
    Created = DateTime.UtcNow,
    BusName = typeof(TBus) == typeof(IBus) ? null : typeof(TBus).Name
});
```

### 9. Outbox Delivery Service Options
**File:** `src/MassTransit/Configuration/DependencyInjection/OutboxDeliveryServiceOptions.cs`

- Add `BusName` property
- Create generic `OutboxDeliveryServiceOptions<TBus>` for bus-specific options

---

## Backward Compatibility

- Non-generic configurators (`EntityFrameworkOutboxConfigurator<TDbContext>`) remain as derived classes
- `NULL` BusName = default `IBus` (existing data continues working)
- Same extension method name `AddEntityFrameworkOutbox` with overloads

---

## Usage After Implementation

```csharp
// Primary bus (default IBus)
services.AddMassTransit(x =>
{
    x.AddEntityFrameworkOutbox<MyDbContext>(o =>
    {
        o.UsePostgres();
        o.UseBusOutbox();
    });
    x.UsingRabbitMq(...);
});

// Secondary bus
services.AddMassTransit<ISecondaryBus>(x =>
{
    x.AddEntityFrameworkOutbox<ISecondaryBus, MyDbContext>(o =>
    {
        o.UsePostgres();
        o.UseBusOutbox();
    });
    x.UsingAzureServiceBus(...);
});
```

---

## Database Migration Required

Users upgrading will need a migration to add:
1. `BusName` column (nullable varchar(256)) to `OutboxState` table
2. Index on `(BusName, Created)`

---

## Verification Plan

1. **Unit Tests:** Create `MultiBusOutbox_Specs.cs` in `tests/MassTransit.EntityFrameworkCoreIntegration.Tests/ReliableMessaging/`
   - Test two buses publishing through outbox
   - Verify each bus's delivery service only delivers its own messages
   - Verify messages reach correct transport endpoints

2. **Integration Test Scenarios:**
   - Primary bus publishes, secondary bus publishes, both deliver correctly
   - Restart scenario: undelivered messages from each bus are delivered by correct service
   - Cross-bus scenario: consumer on bus A publishes to bus B via outbox

3. **Backward Compatibility Test:**
   - Single bus scenario still works with existing API
   - Existing outbox data with NULL BusName handled correctly
