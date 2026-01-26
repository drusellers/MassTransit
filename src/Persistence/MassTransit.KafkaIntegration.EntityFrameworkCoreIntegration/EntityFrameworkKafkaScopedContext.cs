#nullable enable
namespace MassTransit.KafkaIntegration.EntityFrameworkCoreIntegration
{
    using System;
    using System.Threading.Tasks;
    using EntityFrameworkCoreIntegration;
    using KafkaIntegration.Middleware;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.EntityFrameworkCore.ChangeTracking;
    using Middleware;
    using Middleware.Outbox;
    using Serialization;


    /// <summary>
    /// Scoped Kafka context that provides outbox-wrapped topic producers.
    /// Messages produced through this context are stored in the transactional outbox.
    /// </summary>
    public class EntityFrameworkKafkaScopedContext<TDbContext> :
        OutboxSendContext,
        IDisposable
        where TDbContext : DbContext
    {
        readonly TDbContext _dbContext;
        readonly IKafkaRider _kafkaRider;
        readonly IBusOutboxNotification _notification;
        readonly DbSet<OutboxMessage> _outboxMessageSet;
        readonly DbSet<OutboxState> _outboxStateSet;

        Guid _outboxId;
        EntityEntry<OutboxState>? _outboxState;
        ITopicProducerProvider? _outboxProducerProvider;

        public EntityFrameworkKafkaScopedContext(
            TDbContext dbContext,
            IKafkaRider kafkaRider,
            IBusOutboxNotification notification)
        {
            _dbContext = dbContext;
            _kafkaRider = kafkaRider;
            _notification = notification;

            _outboxId = NewId.NextGuid();

            _outboxMessageSet = dbContext.Set<OutboxMessage>();
            _outboxStateSet = dbContext.Set<OutboxState>();
        }

        /// <summary>
        /// Get a topic producer provider that stores messages in the outbox.
        /// </summary>
        public ITopicProducerProvider TopicProducerProvider =>
            _outboxProducerProvider ??= new OutboxTopicProducerProvider(_kafkaRider, this);

        /// <summary>
        /// Get a topic producer that stores messages in the outbox.
        /// </summary>
        public ITopicProducer<TKey, TValue> GetProducer<TKey, TValue>(Uri address)
            where TValue : class
        {
            return TopicProducerProvider.GetProducer<TKey, TValue>(address);
        }

        /// <summary>
        /// Get a topic producer that stores messages in the outbox.
        /// </summary>
        public ITopicProducer<TKey, TValue> GetProducer<TKey, TValue>(string topicName)
            where TValue : class
        {
            return GetProducer<TKey, TValue>(new Uri($"topic:{topicName}"));
        }

        public void Dispose()
        {
            if (WasCommitted())
                _notification.Delivered();

            _outboxState = null;
        }

        public Task AddSend<T>(SendContext<T> context)
            where T : class
        {
            lock (_outboxStateSet)
            {
                if (_outboxState == null || WasCommitted())
                {
                    if (WasCommitted())
                    {
                        _notification.Delivered();
                        _outboxId = NewId.NextGuid();
                        _outboxState = null;
                    }

                    _outboxState ??= _dbContext.Add(new OutboxState
                    {
                        OutboxId = _outboxId,
                        Created = DateTime.UtcNow,
                        BusName = "Kafka"  // Identify as Kafka outbox
                    });
                }
            }

            return _outboxMessageSet.AddSend(context, SystemTextJsonMessageSerializer.Instance, outboxId: _outboxId);
        }

        bool WasCommitted()
        {
            return _outboxState?.State == EntityState.Unchanged;
        }
    }
}
