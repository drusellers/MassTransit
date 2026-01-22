---
name: masstransit-consumer
description: helps you write a consumer
---

# MassTransit Consumer Skill Guide

This guide helps you write effective Consumers in MassTransit based on patterns from the official test suite.

## Core Concepts

A **Consumer** in MassTransit processes messages from a queue. Consumers implement `IConsumer<TMessage>` and receive messages via a `ConsumeContext<TMessage>`.

---

## Basic Consumer Implementation

### Simple Consumer

```csharp
public class OrderSubmittedConsumer : IConsumer<OrderSubmitted>
{
    public async Task Consume(ConsumeContext<OrderSubmitted> context)
    {
        var orderId = context.Message.OrderId;

        // Process the order
        await ProcessOrder(orderId);
    }
}
```

### Consumer with Dependencies (Recommended)

```csharp
public class OrderSubmittedConsumer : IConsumer<OrderSubmitted>
{
    readonly IOrderRepository _orderRepository;
    readonly ILogger<OrderSubmittedConsumer> _logger;

    public OrderSubmittedConsumer(IOrderRepository orderRepository, ILogger<OrderSubmittedConsumer> logger)
    {
        _orderRepository = orderRepository;
        _logger = logger;
    }

    public async Task Consume(ConsumeContext<OrderSubmitted> context)
    {
        _logger.LogInformation("Processing order {OrderId}", context.Message.OrderId);

        await _orderRepository.SaveAsync(context.Message);
    }
}
```

**Key points:**
- Dependencies are constructor-injected
- MassTransit creates a new scope per message automatically
- `IDisposable` dependencies are disposed after consumption

---

## Consumer Registration

### Basic Registration

```csharp
services.AddMassTransit(x =>
{
    x.AddConsumer<OrderSubmittedConsumer>();

    x.UsingRabbitMq((context, cfg) =>
    {
        cfg.ConfigureEndpoints(context);
    });
});
```

### With Custom Endpoint Name

```csharp
x.AddConsumer<OrderSubmittedConsumer>()
    .Endpoint(e => e.Name = "order-processing");
```

### Bulk Registration

```csharp
x.AddConsumersFromNamespaceContaining<OrderSubmittedConsumer>();
```

---

## Consumer Definition (Advanced Configuration)

Use a `ConsumerDefinition` to configure retry, concurrency, and middleware:

```csharp
public class OrderSubmittedConsumerDefinition : ConsumerDefinition<OrderSubmittedConsumer>
{
    public OrderSubmittedConsumerDefinition()
    {
        EndpointName = "order-processing";
        ConcurrentMessageLimit = 10;
    }

    protected override void ConfigureConsumer(
        IReceiveEndpointConfigurator endpointConfigurator,
        IConsumerConfigurator<OrderSubmittedConsumer> consumerConfigurator,
        IRegistrationContext context)
    {
        endpointConfigurator.UseMessageRetry(r => r.Intervals(100, 500, 1000));
    }
}
```

Register with definition:

```csharp
x.AddConsumer<OrderSubmittedConsumer, OrderSubmittedConsumerDefinition>();
```

---

## Request/Response Pattern

### Consumer that Responds

```csharp
public class GetOrderStatusConsumer : IConsumer<GetOrderStatus>
{
    readonly IOrderRepository _repository;

    public GetOrderStatusConsumer(IOrderRepository repository)
    {
        _repository = repository;
    }

    public async Task Consume(ConsumeContext<GetOrderStatus> context)
    {
        var order = await _repository.GetAsync(context.Message.OrderId);

        if (order == null)
        {
            await context.RespondAsync<OrderNotFound>(new { context.Message.OrderId });
            return;
        }

        await context.RespondAsync<OrderStatus>(new
        {
            order.OrderId,
            order.Status
        });
    }
}
```

### Calling the Request

```csharp
public class OrderController
{
    readonly IRequestClient<GetOrderStatus> _client;

    public OrderController(IRequestClient<GetOrderStatus> client)
    {
        _client = client;
    }

    public async Task<OrderStatus> GetStatus(Guid orderId)
    {
        var response = await _client.GetResponse<OrderStatus>(new { OrderId = orderId });
        return response.Message;
    }
}
```

---

## Publishing Events from Consumers

```csharp
public class OrderSubmittedConsumer : IConsumer<OrderSubmitted>
{
    public async Task Consume(ConsumeContext<OrderSubmitted> context)
    {
        // Process order...

        // Publish an event for other consumers
        await context.Publish<OrderProcessed>(new
        {
            context.Message.OrderId,
            ProcessedAt = DateTime.UtcNow
        });
    }
}
```

---

## Multiple Message Types

A consumer can handle multiple message types:

```csharp
public class OrderConsumer :
    IConsumer<OrderSubmitted>,
    IConsumer<OrderCancelled>
{
    public async Task Consume(ConsumeContext<OrderSubmitted> context)
    {
        // Handle submission
    }

    public async Task Consume(ConsumeContext<OrderCancelled> context)
    {
        // Handle cancellation
    }
}
```

---

## Job Consumers (Long-Running Tasks)

For tasks that take longer than typical message processing:

```csharp
public class GenerateReportConsumer : IJobConsumer<GenerateReport>
{
    public async Task Run(JobContext<GenerateReport> context)
    {
        var job = context.Job;

        // Report progress
        await context.SetJobProgress(0, 100);

        for (int i = 0; i < 100; i++)
        {
            // Check for cancellation
            context.CancellationToken.ThrowIfCancellationRequested();

            await ProcessChunk(i);
            await context.SetJobProgress(i + 1, 100);
        }
    }
}
```

### Job Consumer with State Persistence

```csharp
public class LongProcessConsumer : IJobConsumer<LongProcess>
{
    public async Task Run(JobContext<LongProcess> context)
    {
        int startIndex = 0;

        // Resume from previous state if retrying
        if (context.TryGetJobState<ProcessState>(out var state))
        {
            startIndex = state.LastProcessedIndex;
        }

        try
        {
            for (int i = startIndex; i < 1000; i++)
            {
                context.CancellationToken.ThrowIfCancellationRequested();
                await ProcessItem(i);
            }
        }
        catch (OperationCanceledException)
        {
            // Save state before shutdown
            await context.SaveJobState(new ProcessState { LastProcessedIndex = i });
            throw;
        }
    }
}

public class ProcessState
{
    public int LastProcessedIndex { get; set; }
}
```

---

## Error Handling

### Automatic Fault Publishing

When a consumer throws an exception, MassTransit publishes a `Fault<TMessage>`:

```csharp
public class FaultConsumer : IConsumer<Fault<OrderSubmitted>>
{
    readonly ILogger<FaultConsumer> _logger;

    public FaultConsumer(ILogger<FaultConsumer> logger)
    {
        _logger = logger;
    }

    public Task Consume(ConsumeContext<Fault<OrderSubmitted>> context)
    {
        _logger.LogError("Order {OrderId} failed: {Exceptions}",
            context.Message.Message.OrderId,
            context.Message.Exceptions);

        return Task.CompletedTask;
    }
}
```

### Retry Configuration

```csharp
public class OrderConsumerDefinition : ConsumerDefinition<OrderSubmittedConsumer>
{
    protected override void ConfigureConsumer(
        IReceiveEndpointConfigurator endpointConfigurator,
        IConsumerConfigurator<OrderSubmittedConsumer> consumerConfigurator,
        IRegistrationContext context)
    {
        // Immediate retries
        endpointConfigurator.UseMessageRetry(r => r.Immediate(5));

        // Or with intervals
        endpointConfigurator.UseMessageRetry(r => r.Intervals(100, 500, 1000));

        // Or exponential backoff
        endpointConfigurator.UseMessageRetry(r => r.Exponential(5,
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromSeconds(10),
            TimeSpan.FromMilliseconds(100)));
    }
}
```

---

## Using Filters

### Transaction Filter Example

```csharp
public class TransactionFilter<TConsumer> : IFilter<ConsumerConsumeContext<TConsumer>>
    where TConsumer : class
{
    public async Task Send(ConsumerConsumeContext<TConsumer> context,
        IPipe<ConsumerConsumeContext<TConsumer>> next)
    {
        using var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);

        await next.Send(context);

        transaction.Complete();
    }

    public void Probe(ProbeContext context) => context.CreateFilterScope("transaction");
}

// Registration
x.AddConsumer<OrderConsumer>(cfg =>
{
    cfg.UseFilter(new TransactionFilter<OrderConsumer>());
});
```

---

## Testing Consumers

### Using Test Harness

```csharp
[Test]
public async Task Should_process_order()
{
    await using var provider = new ServiceCollection()
        .AddMassTransitTestHarness(x =>
        {
            x.AddConsumer<OrderSubmittedConsumer>();
        })
        .BuildServiceProvider(true);

    var harness = provider.GetRequiredService<ITestHarness>();
    await harness.Start();

    await harness.Bus.Publish(new OrderSubmitted { OrderId = Guid.NewGuid() });

    Assert.That(await harness.Consumed.Any<OrderSubmitted>(), Is.True);

    var consumerHarness = harness.GetConsumerHarness<OrderSubmittedConsumer>();
    Assert.That(await consumerHarness.Consumed.Any<OrderSubmitted>(), Is.True);
}
```

### Testing Request/Response

```csharp
[Test]
public async Task Should_respond_with_status()
{
    await using var provider = new ServiceCollection()
        .AddMassTransitTestHarness(x =>
        {
            x.AddConsumer<GetOrderStatusConsumer>();
        })
        .BuildServiceProvider(true);

    var harness = provider.GetRequiredService<ITestHarness>();
    await harness.Start();

    var client = harness.GetRequestClient<GetOrderStatus>();
    var response = await client.GetResponse<OrderStatus>(new { OrderId = Guid.NewGuid() });

    Assert.That(response.Message.Status, Is.EqualTo("Pending"));
}
```

---

## ConsumeContext Reference

Common operations available on `ConsumeContext<TMessage>`:

| Property/Method | Description |
|-----------------|-------------|
| `context.Message` | The deserialized message |
| `context.MessageId` | Unique message identifier |
| `context.CorrelationId` | Correlation ID for tracking |
| `context.ConversationId` | Conversation tracking |
| `context.Headers` | Message headers |
| `context.CancellationToken` | For graceful shutdown |
| `await context.Publish<T>(msg)` | Publish an event |
| `await context.Send(uri, msg)` | Send to specific endpoint |
| `await context.RespondAsync<T>(msg)` | Respond to request |
| `context.GetOrAddPayload<T>()` | Access/add middleware data |

---

## Best Practices Summary

1. **Use constructor injection** for dependencies - MassTransit manages scopes automatically
2. **Keep consumers focused** - one responsibility per consumer
3. **Use ConsumerDefinition** for configuration - keeps consumer code clean
4. **Configure retries** - transient failures are common in distributed systems
5. **Handle cancellation** - respect `CancellationToken` for graceful shutdown
6. **Test with harness** - MassTransit provides excellent testing support
7. **Publish events for side effects** - don't call other services directly in consumers
8. **Use Job Consumers** for long-running tasks - they support progress and state
9. **Log with context** - include message IDs and correlation IDs
10. **Avoid blocking calls** - always use async/await
