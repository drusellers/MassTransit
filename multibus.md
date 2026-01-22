# MultiBus Architecture

This document describes the architecture and implementation of MultiBus support in MassTransit, which enables applications to configure and use multiple independent bus instances within a single process.

## Overview

MultiBus allows applications to connect to multiple message transports (or the same transport with different configurations) simultaneously. Each bus instance is isolated and has its own:

- Transport connection
- Consumers and sagas
- Endpoint configuration
- Riders (e.g., Kafka, Event Hub)

## Core Concepts

### Marker Interfaces

The foundation of MultiBus is the marker interface pattern. Each additional bus is identified by a custom interface that extends `IBus`:

```csharp
public interface ISecondBus : IBus { }
public interface IThirdBus : IBus { }
```

These interfaces serve as type-safe keys for dependency injection, allowing the container to resolve the correct bus instance based on the requested interface type.

### Registration Pattern

The primary bus is registered without a type parameter:

```csharp
services.AddMassTransit(x => {
    x.AddConsumer<OrderConsumer>();
    x.UsingRabbitMq((context, cfg) => {
        cfg.ConfigureEndpoints(context);
    });
});
```

Additional buses use the generic overload with the marker interface:

```csharp
services.AddMassTransit<ISecondBus>(x => {
    x.AddConsumer<InventoryConsumer>();
    x.UsingRabbitMq((context, cfg) => {
        cfg.Host("inventory-host");
        cfg.ConfigureEndpoints(context);
    });
});
```

## Key Components

### 1. Bus Instance Hierarchy

```
IBusInstance
    └── IBusInstance<TBus>
            └── MultiBusInstance<TBus>
```

**`IBusInstance`** (`src/MassTransit/Transports/IBusInstance.cs`)
- Base interface for all bus instances
- Provides access to `IBusControl`, hosted service, and rider connectivity

**`IBusInstance<TBus>`**
- Generic interface that associates a bus instance with its marker interface
- Exposes the strongly-typed `TBus Bus` property

**`MultiBusInstance<TBus>`** (`src/MassTransit/Transports/MultiBusInstance.cs`)
- Wraps the typed bus and underlying `IBusInstance`
- Derives the bus name from the interface (e.g., `ISecondBus` → `masstransit-second-bus`)
- Delegates rider management to the inner instance

### 2. BusInstance Base Class

**`BusInstance<TBus>`** (`src/MassTransit.Abstractions/BusInstance.cs`)

Abstract base class that wraps an `IBusControl` and implements the bus interface:

```csharp
public abstract class BusInstance<TBus> : IBusControl
    where TBus : IBus
{
    readonly IBusControl _busControl;

    protected BusInstance(IBusControl busControl)
    {
        _busControl = busControl;
    }

    // Delegates all IBusControl methods to _busControl
}
```

This class enables custom bus implementations to add additional dependencies or behaviors.

### 3. Dynamic Type Generation

**`BusInstanceBuilder`** (`src/MassTransit/DependencyInjection/DependencyInjection/BusInstanceBuilder.cs`)

When only a marker interface is provided (no explicit implementation class), MassTransit dynamically generates a concrete class at runtime:

1. Uses `System.Reflection.Emit` to create a new type
2. The generated class inherits from `BusInstance<TBus>` and implements `TBus`
3. For `ISecondBus`, generates `MassTransit.BusInstances.SecondBus`
4. Supports additional interface properties via IL generation
5. Results are cached in a `ConcurrentDictionary` for thread safety

Example of what gets generated for `ISecondBus`:

```csharp
// Auto-generated at runtime
namespace MassTransit.BusInstances
{
    class SecondBus : BusInstance<ISecondBus>, ISecondBus
    {
        public SecondBus(IBusControl busControl) : base(busControl) { }
    }
}
```

### 4. The Bind<TKey, TValue> Pattern

**`Bind<TKey, TValue>`** (`src/MassTransit/DependencyInjection/DependencyInjection/Bind.cs`)

A wrapper type that associates a service with a specific bus:

```csharp
public class Bind<TKey, TValue> where TValue : class
{
    public TValue Value { get; }
}
```

This pattern enables the DI container to differentiate services belonging to different bus instances:

| Service Registration | Purpose |
|---------------------|---------|
| `Bind<IBus, IPublishEndpoint>` | Default bus publish endpoint |
| `Bind<ISecondBus, IPublishEndpoint>` | Second bus publish endpoint |
| `Bind<TBus, ISendEndpointProvider>` | Bus-specific send endpoint |
| `Bind<TBus, IClientFactory>` | Bus-specific client factory |

### 5. Container Registrars

**`DependencyInjectionContainerRegistrar`** (`src/MassTransit/DependencyInjection/Configuration/DependencyInjectionContainerRegistrar.cs`)

Handles registration of bus-related services into the DI container.

**`DependencyInjectionContainerRegistrar<TBus>`**

Generic variant for MultiBus that:
- Wraps all registrations in `Bind<TBus, T>` for type safety
- Retrieves registrations by filtering service collection for `Bind<TBus, T>` entries

### 6. Bus Configurators

**`ServiceCollectionBusConfigurator`** (`src/MassTransit/DependencyInjection/Configuration/ServiceCollectionBusConfigurator.cs`)

Configures the primary bus, registering:
- Scoped services (publish endpoint, send endpoint, consume context)
- Singleton services (bus instance, client factory)
- Hosted service for bus lifecycle

**`ServiceCollectionBusConfigurator<TBus, TBusInstance>`**

Extended configurator for MultiBus that:
- Inherits from the base configurator
- Wraps all service registrations in `Bind<TBus, T>`
- Creates `MultiBusInstance<TBus>` wrapping the actual bus instance

## Dependency Injection Flow

### Service Registration (at startup)

```
AddMassTransit<ISecondBus>()
    └── ServiceCollectionBusConfigurator<ISecondBus>
            ├── Register Bind<ISecondBus, IPublishEndpoint> (scoped)
            ├── Register Bind<ISecondBus, ISendEndpointProvider> (scoped)
            ├── Register Bind<ISecondBus, IClientFactory> (singleton)
            ├── Register IBusInstance<ISecondBus> (singleton)
            └── Register ISecondBus (singleton, resolved from IBusInstance<ISecondBus>)
```

### Service Resolution (at runtime)

When a consumer registered with `ISecondBus` requests `IPublishEndpoint`:

```
Consumer constructor
    └── IPublishEndpoint (parameter)
            └── Resolved via Bind<ISecondBus, IPublishEndpoint>
                    └── Returns ISecondBus's publish endpoint
```

## Rider Support with MultiBus

Riders (Kafka, Event Hub, etc.) support MultiBus through triple binding:

**`Bind<TBus, TRider, T>`** - Associates a service with both a bus and a rider

```csharp
services.AddMassTransit<ISecondBus>(x =>
{
    x.AddRider(rider =>
    {
        rider.AddConsumer<KafkaMessageConsumer>();
        rider.AddProducer<KafkaMessage>(topicName);
        rider.UsingKafka((context, k) => {
            k.TopicEndpoint<KafkaMessage>(topicName, groupId, e => {
                e.ConfigureConsumer<KafkaMessageConsumer>(context);
            });
        });
    });
    x.UsingInMemory();
});
```

**`ServiceCollectionRiderConfigurator<TBus>`** handles rider registration with the appropriate triple binding.

## Consumer Resolution

Consumers registered within a MultiBus configuration automatically receive the correct bus instance:

```csharp
services.AddMassTransit<ISecondBus>(x =>
{
    x.AddConsumer<InventoryConsumer>();
    // ...
});

public class InventoryConsumer : IConsumer<AllocateInventory>
{
    readonly IPublishEndpoint _publishEndpoint; // Resolves to ISecondBus's endpoint

    public InventoryConsumer(IPublishEndpoint publishEndpoint)
    {
        _publishEndpoint = publishEndpoint;
    }
}
```

To explicitly inject a specific bus from a shared service:

```csharp
public class OrderService
{
    readonly IPublishEndpoint _defaultBus;
    readonly ISecondBus _secondBus;

    public OrderService(IPublishEndpoint defaultBus, ISecondBus secondBus)
    {
        _defaultBus = defaultBus;
        _secondBus = secondBus;
    }
}
```

Or using `Bind<>` for more granular access:

```csharp
public class OrderService
{
    public OrderService(Bind<ISecondBus, ISendEndpointProvider> sendEndpoint)
    {
        // Access second bus's send endpoint provider
    }
}
```

## Scoped Context Providers

**`ScopedBusContextProvider<TBus>`** (`src/MassTransit/DependencyInjection/DependencyInjection/ScopedBusContextProvider.cs`)

Captures bus context for the current scope, enabling runtime resolution of bus-specific services. Resolution priority:

1. Bus-specific consume context provider (`Bind<TBus, IScopedConsumeContextProvider>`)
2. Global consume context provider
3. Bus instance with client factory

## Custom Bus Instance Classes

For advanced scenarios requiring additional dependencies or behaviors:

```csharp
public interface IThirdBus : IBus
{
    IMetricsService Metrics { get; }
}

public class ThirdBus : BusInstance<IThirdBus>, IThirdBus
{
    public ThirdBus(IBusControl busControl, IMetricsService metrics)
        : base(busControl)
    {
        Metrics = metrics;
    }

    public IMetricsService Metrics { get; }
}

services.AddMassTransit<IThirdBus, ThirdBus>(x => { ... });
```

## Limitations

1. **No cross-bus message buffering**: The InMemoryOutbox does not buffer messages across bus instances
2. **Separate conversations**: Calling Send/Publish on different bus instances starts new message conversations
3. **Container requirement**: MultiBus currently requires `Microsoft.Extensions.DependencyInjection`
4. **Single primary bus**: Only one bus can be registered without a type parameter (the default `IBus`)

## File Locations

| Component | Location |
|-----------|----------|
| Registration Extensions | `src/MassTransit/Configuration/DependencyInjection/DependencyInjectionRegistrationExtensions.cs` |
| BusInstance Base | `src/MassTransit.Abstractions/BusInstance.cs` |
| MultiBusInstance | `src/MassTransit/Transports/MultiBusInstance.cs` |
| IBusInstance | `src/MassTransit/Transports/IBusInstance.cs` |
| BusInstanceBuilder | `src/MassTransit/DependencyInjection/DependencyInjection/BusInstanceBuilder.cs` |
| Bind<> | `src/MassTransit/DependencyInjection/DependencyInjection/Bind.cs` |
| Container Registrar | `src/MassTransit/DependencyInjection/Configuration/DependencyInjectionContainerRegistrar.cs` |
| Bus Configurator | `src/MassTransit/DependencyInjection/Configuration/ServiceCollectionBusConfigurator.cs` |
| Scoped Context Provider | `src/MassTransit/DependencyInjection/DependencyInjection/ScopedBusContextProvider.cs` |
| Rider Configurator | `src/MassTransit/DependencyInjection/Configuration/ServiceCollectionRiderConfigurator.cs` |

## Test References

- `tests/MassTransit.Tests/ContainerTests/Common_Tests/MultiBus_Specs.cs` - Basic MultiBus usage
- `tests/MassTransit.Tests/ContainerTests/Common_Tests/MultiBusPublishEndpoint_Specs.cs` - Bind<> pattern usage
- `tests/MassTransit.Tests/ContainerTests/Common_Tests/MultiBusEndpointName_Specs.cs` - Endpoint naming
- `tests/MassTransit.Tests/MultiBusRequest_Specs.cs` - Cross-bus request/response
- `tests/MassTransit.KafkaIntegration.Tests/MultiBus_Specs.cs` - Rider integration
