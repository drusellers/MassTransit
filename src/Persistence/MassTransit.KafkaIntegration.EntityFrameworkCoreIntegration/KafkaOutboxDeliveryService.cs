#nullable enable
namespace MassTransit.KafkaIntegration.EntityFrameworkCoreIntegration
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Text;
    using System.Text.Json;
    using System.Threading;
    using System.Threading.Tasks;
    using Confluent.Kafka;
    using EntityFrameworkCoreIntegration;
    using Logging;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.EntityFrameworkCore.Storage;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Hosting;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Options;
    using Middleware.Outbox;
    using RetryPolicies;
    using Serialization;
    using Util;


    /// <summary>
    /// Background service that delivers Kafka outbox messages.
    /// Polls the outbox for messages with kafka:// destination addresses and produces them to Kafka topics.
    /// </summary>
    public class KafkaOutboxDeliveryService<TDbContext> :
        BackgroundService
        where TDbContext : DbContext
    {
        readonly IKafkaRider _kafkaRider;
        readonly IsolationLevel _isolationLevel;
        readonly ILockStatementProvider _lockStatementProvider;
        readonly ILogger _logger;
        readonly IBusOutboxNotification _notification;
        readonly KafkaOutboxDeliveryServiceOptions _options;
        readonly Func<TDbContext, long, int, IAsyncEnumerable<OutboxMessage>> _outboxMessagesQuery;
        readonly IServiceProvider _provider;
        readonly IRetryPolicy _retryPolicy;

        public KafkaOutboxDeliveryService(
            IKafkaRider kafkaRider,
            IOptions<KafkaOutboxDeliveryServiceOptions> options,
            IOptions<EntityFrameworkOutboxOptions<TDbContext>> outboxOptions,
            IBusOutboxNotification notification,
            ILogger<KafkaOutboxDeliveryService<TDbContext>> logger,
            IServiceProvider provider)
        {
            _kafkaRider = kafkaRider;
            _notification = notification;
            _provider = provider;
            _logger = logger;

            _options = options.Value;

            _lockStatementProvider = outboxOptions.Value.LockStatementProvider;
            _isolationLevel = outboxOptions.Value.IsolationLevel;

            // Query for Kafka messages (destination starts with kafka://)
            _outboxMessagesQuery = EF.CompileAsyncQuery((TDbContext context, long lastSequenceNumber, int limit) =>
                context.Set<OutboxMessage>()
                    .Where(x => x.DestinationAddress != null
                        && x.DestinationAddress.StartsWith("kafka://")
                        && x.SequenceNumber > lastSequenceNumber
                        && x.OutboxId != null)
                    .OrderBy(x => x.SequenceNumber)
                    .Take(limit)
                    .AsNoTracking());

            _retryPolicy = Retry.Exponential(1000, TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(3));
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using var algorithm = new RequestRateAlgorithm(new RequestRateAlgorithmOptions
            {
                PrefetchCount = _options.QueryMessageLimit,
                RequestResultLimit = 10,
            });

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    // ReSharper disable once AccessToDisposedClosure
                    var count = await _retryPolicy.Retry(() => algorithm.Run(DeliverKafkaMessages, stoppingToken), stoppingToken)
                        .ConfigureAwait(false);
                    if (count > 0)
                        continue;

                    await _notification.WaitForDelivery(stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
                catch (DbUpdateConcurrencyException)
                {
                }
                catch (InvalidOperationException exception) when (IsTransientFailure(exception))
                {
                }
                catch (Exception exception)
                {
                    _logger.LogError(exception, "Kafka outbox delivery faulted");
                }
            }
        }

        async Task<int> DeliverKafkaMessages(int resultLimit, CancellationToken cancellationToken)
        {
            var scope = _provider.CreateAsyncScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<TDbContext>();

            try
            {
                async Task<int> Execute()
                {
                    using var timeoutToken = new CancellationTokenSource(_options.QueryTimeout);

                    await using var transaction = await dbContext.Database.BeginTransactionAsync(_isolationLevel, timeoutToken.Token)
                        .ConfigureAwait(false);

                    try
                    {
                        var messageCount = await DeliverOutboxMessages(dbContext, cancellationToken).ConfigureAwait(false);

                        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);

                        return messageCount;
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                    catch (InvalidOperationException exception) when (IsTransientFailure(exception))
                    {
                        throw;
                    }
                    catch (Exception)
                    {
                        await RollbackTransaction(transaction).ConfigureAwait(false);
                        throw;
                    }
                }

                var executionStrategy = dbContext.Database.CreateExecutionStrategy();

                var messageCount = 0;
                while (messageCount < resultLimit)
                {
                    var executeResult = executionStrategy is ExecutionStrategy
                        ? await executionStrategy.ExecuteAsync(() => Execute()).ConfigureAwait(false)
                        : await Execute().ConfigureAwait(false);

                    if (executeResult <= 0)
                        break;

                    messageCount += executeResult;
                }

                return messageCount;
            }
            finally
            {
                await dbContext.DisposeAsync().ConfigureAwait(false);
                await scope.DisposeAsync().ConfigureAwait(false);
            }
        }

        async Task<int> DeliverOutboxMessages(TDbContext dbContext, CancellationToken cancellationToken)
        {
            var messageLimit = _options.MessageDeliveryLimit;

            IList<OutboxMessage> messages = await _outboxMessagesQuery(dbContext, 0, messageLimit)
                .ToListAsync(cancellationToken)
                .ConfigureAwait(false);

            if (messages.Count == 0)
                return 0;

            var messageCount = 0;

            foreach (var message in messages)
            {
                message.Deserialize(SystemTextJsonMessageSerializer.Instance);

                if (message.DestinationAddress == null)
                    continue;

                try
                {
                    using var sendToken = new CancellationTokenSource(_options.MessageDeliveryTimeout);
                    using var token = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, sendToken.Token);

                    await DeliverToKafka(message, token.Token).ConfigureAwait(false);

                    // Remove the message after successful delivery
                    var trackedMessage = await dbContext.Set<OutboxMessage>()
                        .FirstOrDefaultAsync(x => x.SequenceNumber == message.SequenceNumber, cancellationToken)
                        .ConfigureAwait(false);

                    if (trackedMessage != null)
                        dbContext.Remove(trackedMessage);

                    await dbContext.SaveChangesAsync(cancellationToken).ConfigureAwait(false);

                    LogContext.Debug?.Log("Kafka Outbox Sent: {SequenceNumber} {MessageId} to {Topic}",
                        message.SequenceNumber, message.MessageId, message.DestinationAddress);

                    messageCount++;
                }
                catch (Exception ex)
                {
                    LogContext.Warning?.Log(ex, "Kafka Outbox Send Fault: {SequenceNumber} {MessageId}",
                        message.SequenceNumber, message.MessageId);

                    break;
                }
            }

            return messageCount;
        }

        async Task DeliverToKafka(OutboxMessage message, CancellationToken cancellationToken)
        {
            // Parse topic from destination address: kafka://topic/my-topic-name
            var topicName = ParseTopicName(message.DestinationAddress!);

            // Get the serialized key from properties
            var key = GetMessageKey(message);

            // For delivery, we use a generic object producer with the serialized message
            // The message body is already serialized JSON, so we deliver it as-is
            var producer = _kafkaRider.GetProducer<string, SerializedKafkaMessage>(new Uri($"topic:{topicName}"));

            var kafkaMessage = new SerializedKafkaMessage
            {
                Body = message.Body,
                ContentType = message.ContentType,
                MessageType = message.MessageType,
                Headers = message.Headers,
                MessageId = message.MessageId,
                CorrelationId = message.CorrelationId,
                ConversationId = message.ConversationId
            };

            await producer.Produce(key ?? string.Empty, kafkaMessage, cancellationToken).ConfigureAwait(false);
        }

        static string ParseTopicName(Uri destinationAddress)
        {
            // Format: kafka://topic/my-topic-name
            var path = destinationAddress.AbsolutePath.TrimStart('/');
            return path;
        }

        static string? GetMessageKey(OutboxMessage message)
        {
            if (message.Properties == null)
                return null;

            try
            {
                var properties = JsonSerializer.Deserialize<Dictionary<string, object>>(message.Properties);
                if (properties != null && properties.TryGetValue("MT-Kafka-Key", out var keyValue))
                    return keyValue?.ToString();
            }
            catch
            {
                // Ignore deserialization errors
            }

            return null;
        }

        static bool IsTransientFailure(Exception exception)
        {
            return exception.InnerException != null &&
                (exception.InnerException.Message.Contains("concurrent update")
                    || exception.InnerException.Message.Contains("transient failure"));
        }

        static async Task RollbackTransaction(IDbContextTransaction transaction)
        {
            try
            {
                await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Ignore rollback errors
            }
        }
    }
}
