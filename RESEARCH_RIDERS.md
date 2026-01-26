# MassTransit Riders Research

## 1. What is a Rider and What Problem Does It Solve?

A **Rider** is a lightweight abstraction that allows MassTransit to connect to external messaging systems (like Apache Kafka, Azure Event Hubs, etc.) in addition to the primary bus transport. Riders solve several problems:

- **Multi-transport support**: Enable consuming from alternative message brokers without replacing the primary bus
- **Decoupled lifecycle**: Riders start and stop independently from the bus
- **Scope isolation**: Messages consumed by riders are processed separately from bus consumers
- **Producer support**: Riders can provide send/produce capabilities to external systems
- **Flexible configuration**: Riders can be added on-demand, even after the bus is started

**Key file**: `src/MassTransit.Abstractions/Transports/IRider.cs`

---

## 2. Architectural Structure: Interfaces, Base Classes, and Key Abstractions

### Core Rider Interfaces

#### `IRider` (IRider.cs:3)
- Base marker interface for all riders
- Location: `src/MassTransit.Abstractions/Transports/IRider.cs`

#### `IRiderControl` (IRiderControl.cs:7-14)
- Extends `IRider`
- Location: `src/MassTransit.Abstractions/Transports/IRiderControl.cs`
- Methods:
  - `RiderHandle Start(CancellationToken)` - Starts the rider and returns a handle
  - `IEnumerable<EndpointHealthResult> CheckEndpointHealth()` - Health checks for endpoints

#### `RiderHandle` (RiderHandle.cs:7-12)
- Location: `src/MassTransit.Abstractions/Transports/RiderHandle.cs`
- Properties:
  - `Task Ready` - Completes when rider is ready to receive messages
  - `Task StopAsync(CancellationToken)` - Stops the rider

#### `HostRiderHandle` (HostRiderHandle.cs:7-17)
- Extends `RiderHandle`
- Location: `src/MassTransit.Abstractions/Transports/HostRiderHandle.cs`
- Additional properties:
  - `IRider Rider { get; }` - Access to the rider instance
  - `Task<RiderReady> Ready { get; }` - Typed completion with `RiderReady` contract

### Rider Registration Interfaces

#### `IRiderRegistrationConfigurator` (IRiderRegistrationConfigurator.cs:1-24)
- Location: `src/MassTransit/Configuration/DependencyInjection/IRiderRegistrationConfigurator.cs`
- Methods:
  - `void SetRiderFactory<TRider>(IRegistrationRiderFactory<TRider>)` - Register the rider factory
  - `void TryAddScoped<TRider, TService>(...)` - Register scoped services

#### `IRiderRegistrationContext` (IRiderRegistrationContext.cs:1-12)
- Location: `src/MassTransit/Configuration/DependencyInjection/IRiderRegistrationContext.cs`
- Extends `IRegistrationContext`
- Methods:
  - `IEnumerable<T> GetRegistrations<T>()` - Get registered components for consumer configuration

#### `IRegistrationRiderFactory<TRider>` (IRegistrationRiderFactory.cs:1-11)
- Location: `src/MassTransit/DependencyInjection/DependencyInjection/IRegistrationRiderFactory.cs`
- Method:
  - `IBusInstanceSpecification CreateRider(IRiderRegistrationContext context)` - Creates the rider specification

### Collection Management

#### `IRiderCollection` (IRiderCollection.cs:1-20)
- Location: `src/MassTransit/Transports/IRiderCollection.cs`
- Extends `IAgent` (lifecycle management)
- Methods:
  - `IRider Get(string name)` - Get a started rider by name
  - `void Add(string name, IRiderControl rider)` - Add a rider before starting
  - `HostRiderHandle[] StartRiders(CancellationToken)` - Start all registered riders
  - `HostRiderHandle StartRider(string name, CancellationToken)` - Start a specific rider

#### `RiderCollection` (concrete implementation)
- Location: `src/MassTransit/Transports/RiderCollection.cs`
- Extends `Agent`
- Manages multiple riders with thread-safe operations
- Stores riders in `Dictionary<string, IRiderControl>` and handles in `Dictionary<string, Handle>`

---

## 3. Rider Integration with the MassTransit Bus

### Bus Integration Points

#### `IBusInstance` interface (IBusInstance.cs:1-23)
- Location: `src/MassTransit/Transports/IBusInstance.cs`
- Methods:
  - `void Connect<TRider>(IRiderControl riderControl)` - Register a rider with the bus
  - `TRider GetRider<TRider>()` - Retrieve a rider instance

#### `TransportBusInstance` (concrete implementation)
- Location: `src/MassTransit/Transports/TransportBusInstance.cs`
- Lines 33-45: `Connect<TRider>()` and `GetRider<TRider>()` delegate to the host

#### `BaseHost` (core integration)
- Location: `src/MassTransit/Transports/BaseHost.cs`
- Line 23: `Riders = new RiderCollection()` - Creates rider collection in constructor
- Lines 73-94 (`HostHandle Start()`): Starts riders alongside receive endpoints
- Lines 106-109: `AddRider()` and `GetRider()` methods delegate to `RiderCollection`
- Lines 152: Stops riders when bus stops
- Lines 113-114: Riders included in health check

---

## 4. Lifecycle of a Rider

### Phase 1: Configuration (Dependency Injection Setup)
1. User calls `AddRider(Action<IRiderRegistrationConfigurator>)` on `IBusRegistrationConfigurator`
   - Location: `src/MassTransit/Configuration/DependencyInjection/IBusRegistrationConfigurator.cs` (lines 31-35)
2. Creates `ServiceCollectionRiderConfigurator`
   - Location: `src/MassTransit/DependencyInjection/Configuration/ServiceCollectionRiderConfigurator.cs` (lines 11-18)
3. User configures via transport-specific method (e.g., `UsingKafka()`)
4. Factory is registered in DI container

### Phase 2: Bus Creation
1. When bus is built, `CreateBus()` retrieves all `IBusInstanceSpecification` objects from DI
2. `IBusInstanceSpecification` includes the rider specification created by the factory
3. Rider is instantiated but not yet started

### Phase 3: Bus Start
1. `BaseHost.Start()` (lines 73-94)
   - Calls `Riders.StartRiders(cancellationToken)`
2. `RiderCollection.StartRiders()` (lines 59-66)
   - Iterates through all registered riders
   - Calls `Start(CancellationToken)` on each
3. Returns `HostRiderHandle[]` array

### Phase 4: Rider Activation
1. Each rider implementation (e.g., `KafkaRider.Start()`, lines 61-70)
   - Starts all receive endpoints
   - Creates a `RiderAgent` (extends `Agent`) for lifecycle management
   - Returns a `Handle` wrapping the agent

### Phase 5: Runtime Operation
- Rider processes messages independently
- `RiderAgent` (inner class) manages:
  - Endpoint lifecycle
  - Supervisor (ClientContextSupervisor for Kafka, ConnectionContextSupervisor for EventHub)
  - Graceful shutdown

### Phase 6: Shutdown
1. `BaseHost.Stop()` (line 152)
   - Calls `Riders.Stop(cancellationToken)`
2. `RiderCollection.StopAgent()` (lines 132-147)
   - Calls `StopAsync(false, cancellationToken)` on each rider handle
3. Each rider's `RiderAgent` stops endpoints and supervisor

---

## 5. How Riders Handle Configuration

### Configuration Pattern

#### Factory Pattern (`IRegistrationRiderFactory<TRider>`)
- Location: `src/Transports/MassTransit.KafkaIntegration/KafkaIntegration/Configuration/KafkaRegistrationRiderFactory.cs`
- Lines 28-41: `CreateRider()` method
- Creates a `KafkaFactoryConfigurator`
- Invokes user's configure lambda
- Registers producers from DI
- Calls `Build()` to create the rider

#### Configurator Pattern (e.g., `IKafkaFactoryConfigurator`)
- Location: `src/Transports/MassTransit.KafkaIntegration/Configuration/IKafkaFactoryConfigurator.cs`
- Extends `IRiderFactoryConfigurator`
- Methods like:
  - `Host(IReadOnlyList<string> servers, Action<IKafkaHostConfigurator>)` - Configure broker
  - `TopicEndpoint<TKey, TValue>(...)` - Define consumer endpoints
  - Various property setters for Kafka options (Acks, ClientId, MessageMaxBytes, etc.)

#### Endpoint Configurators (e.g., `IKafkaTopicReceiveEndpointConfigurator`)
- Location: `src/Transports/MassTransit.KafkaIntegration/Configuration/IKafkaTopicReceiveEndpointConfigurator.cs`
- Extends `IReceiveEndpointConfigurator`
- Topic-specific options (AutoOffsetReset, CheckpointInterval, ConcurrentConsumerLimit, etc.)
- Methods:
  - `ConfigureFetch(Action<IKafkaFetchConfigurator>)`
  - `UseIsolationLevel(IsolationLevel)`
  - `CreateIfMissing(Action<KafkaTopicOptions>)`

### Build Process (KafkaFactoryConfigurator Example)
- Location: `src/Transports/MassTransit.KafkaIntegration/KafkaIntegration/Configuration/KafkaFactoryConfigurator.cs` (lines 332-341)

```
Build(IRiderRegistrationContext context, IBusInstance busInstance):
  1. Retrieve all topic specifications
  2. Create ReceiveEndpointCollection
  3. For each topic: create receive endpoint
  4. Instantiate KafkaRider with configuration
```

---

## 6. Existing Rider Implementations

### A. Kafka Rider

**Files**:
- Interface: `src/Transports/MassTransit.KafkaIntegration/IKafkaRider.cs` (lines 1-12)
- Implementation: `src/Transports/MassTransit.KafkaIntegration/KafkaIntegration/KafkaRider.cs` (lines 1-151)
- Factory: `src/Transports/MassTransit.KafkaIntegration/KafkaIntegration/Configuration/KafkaRegistrationRiderFactory.cs` (lines 1-43)

**Features**:
- `IKafkaRider` implements:
  - `IRiderControl` - Lifecycle management
  - `ITopicProducerProvider` - Producer support
  - `IKafkaTopicEndpointConnector` - Dynamic endpoint connection

**Methods** (KafkaRider, lines 22-91):
- Constructor: Takes host configuration, bus instance, endpoints collection, registration context
- `ConnectTopicEndpoint<TKey, TValue>()` - Connect to Kafka topic (with group ID)
- `ConnectTopicEndpoint<TKey, TValue>()` - Connect with explicit ConsumerConfig
- `Start()` - Starts all endpoints and supervisor
- `GetProducer<TKey, TValue>(Uri)` - Get producer for topic

**Inner RiderAgent** (lines 94-118):
- Extends `Agent`
- Manages `IClientContextSupervisor` for Kafka client lifecycle
- Manages endpoints collection
- OnStop: Calls `_endpoints.StopEndpoints()` and `_supervisor.Stop()`

**Inner Handle** (lines 121-150):
- Implements `RiderHandle`
- Aggregates all endpoint readiness tasks
- Delegates stop to agent

### B. EventHub Rider

**Files**:
- Interface: `src/Transports/MassTransit.EventHubIntegration/IEventHubRider.cs` (lines 1-12)
- Implementation: `src/Transports/MassTransit.EventHubIntegration/EventHubIntegration/EventHubRider.cs` (lines 1-134)
- Factory: `src/Transports/MassTransit.EventHubIntegration/EventHubIntegration/Configuration/EventHubRegistrationRiderFactory.cs` (lines 1-32)

**Features**:
- `IEventHubRider` implements:
  - `IRiderControl` - Lifecycle management
  - `IEventHubEndpointConnector` - Endpoint connection
  - `GetProducerProvider(ConsumeContext)` - Producer with consume context support

**Methods** (EventHubRider, lines 22-72):
- Constructor: Similar to KafkaRider
- `ConnectEventHubEndpoint(string, string, Action<...>)` - Connect to EventHub
- `Start()` - Starts endpoints and connection supervisor
- `GetProducerProvider()` - Get producer (with optional consume context)

**Architecture**: Similar to KafkaRider with `RiderAgent` and `Handle` inner classes

---

## 7. How Consumers Work Within Riders

### Endpoint Connector Pattern

Riders implement endpoint connector interfaces to allow dynamic consumer registration:
- **`IKafkaTopicEndpointConnector`** (lines 7-16)
  - Location: `src/Transports/MassTransit.KafkaIntegration/IKafkaTopicEndpointConnector.cs`

- **`IEventHubEndpointConnector`** (lines 6-10)
  - Location: `src/Transports/MassTransit.EventHubIntegration/IEventHubEndpointConnector.cs`

### Configuration Process

**During Initial Configuration** (via `IKafkaFactoryConfigurator.TopicEndpoint()`):
```
Location: KafkaFactoryConfigurator.cs, lines 80-95
1. TopicEndpoint<TKey, TValue>(topicName, groupId, configure)
2. CreateSpecification<TKey, TValue>() - Creates KafkaConsumerSpecification
3. Stores in _topics dictionary
4. User's configure lambda is called with KafkaTopicReceiveEndpointConfigurator
5. Inside lambda: c.ConfigureConsumer<ConsumerType>(context)
```

**During Runtime** (after bus/rider started):
```
Location: KafkaRider.cs, lines 33-45
1. Call rider.ConnectTopicEndpoint<TKey, TValue>(topicName, groupId, configure)
2. Creates new specification
3. Adds to endpoints collection
4. Starts the endpoint immediately
5. Returns HostReceiveEndpointHandle for lifecycle management
```

### Consumer Registration in Context

The `IRiderRegistrationContext` passed to configurators provides:
- Location: `src/MassTransit/DependencyInjection/Configuration/RiderRegistrationContext.cs`
- Methods:
  - `ConfigureConsumer<T>(IReceiveEndpointConfigurator, Action<IConsumerConfigurator<T>>)` - Register consumer
  - `ConfigureConsumers(IReceiveEndpointConfigurator)` - Register all consumers
  - `GetRegistrations<T>()` - Get registered components
  - `GetService(Type)` - DI service access

### Example Pattern (from test)

```csharp
// Location: tests/MassTransit.KafkaIntegration.Tests/Receive_Specs.cs, lines 39-46

k.TopicEndpoint<KafkaMessage>(Topic, nameof(Receive_Specs), c =>
{
    c.AutoOffsetReset = AutoOffsetReset.Earliest;
    c.ConfigureConsumer<KafkaMessageConsumer>(context);
});
```

This configures:
1. Topic name and consumer group
2. Consumer-specific Kafka options
3. Consumer registration via the context

---

## 8. Key Code Patterns and Extension Points

### A. Registration Extension Methods

**Kafka Integration** (lines 10-65)
- Location: `src/Transports/MassTransit.KafkaIntegration/Configuration/KafkaIntegrationExtensions.cs`

```csharp
// Basic usage
public static void UsingKafka(
    this IRiderRegistrationConfigurator configurator,
    Action<IRiderRegistrationContext, IKafkaFactoryConfigurator> configure)
{
    var factory = new KafkaRegistrationRiderFactory(configure);
    configurator.SetRiderFactory(factory);
    configurator.TryAddScoped<IKafkaRider, ITopicProducerProvider>(
        (rider, provider) => rider.GetScopedTopicProducerProvider(provider));
}
```

**EventHub Integration** (lines 9-50)
- Location: `src/Transports/MassTransit.EventHubIntegration/EventHubIntegrationExtensions.cs`

Similar pattern with EventHub-specific producers

### B. Configuration Delegation Pattern

All rider configurations follow a fluent builder pattern:
1. Factory method registers rider factory in DI
2. Configurator accepts user's lambda
3. Build method creates the actual rider instance
4. Rider instance connects to bus via `IBusInstance.Connect<TRider>()`

### C. Scoped Service Registration

Riders register producers as scoped services:
```csharp
// Location: KafkaIntegrationExtensions.cs, line 20
configurator.TryAddScoped<IKafkaRider, ITopicProducerProvider>(
    (rider, provider) => rider.GetScopedTopicProducerProvider(provider));
```

This allows injecting `ITopicProducerProvider` in consumers to send to Kafka topics.

### D. Dynamic Endpoint Connection

```csharp
// Location: Receive_Specs.cs, lines 155-163
var kafka = provider.GetRequiredService<IKafkaRider>();
var connected = kafka.ConnectTopicEndpoint<int, KafkaMessage>(
    Topic,
    nameof(ConcurrentKeysReceive_Specs),
    (context, configurator) =>
    {
        configurator.AutoOffsetReset = AutoOffsetReset.Earliest;
        configurator.ConfigureConsumer<KafkaMessageConsumer>(context);
    });

await connected.Ready.OrCanceled(harness.CancellationToken);
```

This allows adding consumers to riders at runtime, after the bus/rider has started.

### E. Producer Provider Pattern

**Kafka**:
- Location: `src/Transports/MassTransit.KafkaIntegration/KafkaIntegration/KafkaRider.cs`, lines 82-86
- `GetProducer<TKey, TValue>(Uri)` with lazy initialization

**EventHub**:
- Location: `src/Transports/MassTransit.EventHubIntegration/EventHubIntegration/EventHubRider.cs`, lines 33-38
- `GetProducerProvider(ConsumeContext)` - Supports contextual producers

### F. Health Check Integration

```csharp
// Location: BaseHost.cs, lines 111-135
// Riders contribute to bus health
EndpointHealthResult[] results = ReceiveEndpoints.CheckEndpointHealth()
    .Concat(Riders.CheckEndpointHealth())
    .ToArray();
```

Each rider implements `CheckEndpointHealth()` from `IRiderControl`.

### G. Observer Pattern

Riders support observers for send and receive:
```csharp
// Location: IKafkaFactoryConfigurator.cs, lines 9-12
public interface IKafkaFactoryConfigurator :
    IRiderFactoryConfigurator,
    ISendObserverConnector,
    ISendPipelineConfigurator
```

This allows:
- Observing messages being sent to Kafka
- Observing messages received from Kafka topics

---

## Summary: Rider Lifecycle Diagram

```
Configuration Phase
├─ AddRider(configure) on IBusRegistrationConfigurator
├─ UsingKafka/UsingEventHub registers factory
└─ ServiceCollectionRiderConfigurator stores rider factory

Bus Creation Phase
├─ IBusInstanceSpecification created by factory
├─ Rider added to bus instance
└─ Rider not yet started (lazy)

Bus Start Phase
├─ BaseHost.Start()
├─ RiderCollection.StartRiders()
└─ For each rider:
    ├─ IRiderControl.Start() called
    ├─ RiderAgent created (extends Agent)
    ├─ All endpoints started
    ├─ Supervisor initialized
    └─ RiderHandle returned

Runtime Phase
├─ Rider processes messages independently
├─ Consumers handle messages from topics
├─ Producers available for sending
└─ Health checks run periodically

Shutdown Phase
├─ BaseHost.Stop()
├─ RiderCollection.StopAgent()
└─ For each rider:
    ├─ Endpoints stopped
    ├─ Supervisor stopped
    └─ RiderAgent stopped
```

---

## Key File References Summary

| Component | File Path | Lines |
|-----------|-----------|-------|
| IRider | `MassTransit.Abstractions/Transports/IRider.cs` | 1-6 |
| IRiderControl | `MassTransit.Abstractions/Transports/IRiderControl.cs` | 7-14 |
| RiderHandle | `MassTransit.Abstractions/Transports/RiderHandle.cs` | 7-12 |
| HostRiderHandle | `MassTransit.Abstractions/Transports/HostRiderHandle.cs` | 7-17 |
| RiderCollection | `MassTransit/Transports/RiderCollection.cs` | 11-200 |
| IRiderCollection | `MassTransit/Transports/IRiderCollection.cs` | 1-20 |
| BaseHost | `MassTransit/Transports/BaseHost.cs` | 11-169 |
| IBusInstance | `MassTransit/Transports/IBusInstance.cs` | 1-37 |
| TransportBusInstance | `MassTransit/Transports/TransportBusInstance.cs` | 7-92 |
| IKafkaRider | `Transports/MassTransit.KafkaIntegration/IKafkaRider.cs` | 1-12 |
| KafkaRider | `Transports/MassTransit.KafkaIntegration/KafkaIntegration/KafkaRider.cs` | 13-151 |
| KafkaRegistrationRiderFactory | `Transports/MassTransit.KafkaIntegration/KafkaIntegration/Configuration/KafkaRegistrationRiderFactory.cs` | 11-42 |
| KafkaFactoryConfigurator | `Transports/MassTransit.KafkaIntegration/KafkaIntegration/Configuration/KafkaFactoryConfigurator.cs` | 14-349 |
| IKafkaFactoryConfigurator | `Transports/MassTransit.KafkaIntegration/Configuration/IKafkaFactoryConfigurator.cs` | 9-302 |
| IEventHubRider | `Transports/MassTransit.EventHubIntegration/IEventHubRider.cs` | 1-12 |
| EventHubRider | `Transports/MassTransit.EventHubIntegration/EventHubIntegration/EventHubRider.cs` | 13-134 |
| IRiderRegistrationConfigurator | `MassTransit/Configuration/DependencyInjection/IRiderRegistrationConfigurator.cs` | 1-32 |
| ServiceCollectionRiderConfigurator | `MassTransit/DependencyInjection/Configuration/ServiceCollectionRiderConfigurator.cs` | 11-113 |
| IBusRegistrationConfigurator | `MassTransit/Configuration/DependencyInjection/IBusRegistrationConfigurator.cs` | 1-73 |
| KafkaIntegrationExtensions | `Transports/MassTransit.KafkaIntegration/Configuration/KafkaIntegrationExtensions.cs` | 10-66 |
| EventHubIntegrationExtensions | `Transports/MassTransit.EventHubIntegration/EventHubIntegrationExtensions.cs` | 9-50 |
