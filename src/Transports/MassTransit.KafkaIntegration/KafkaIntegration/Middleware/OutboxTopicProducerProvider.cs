#nullable enable
namespace MassTransit.KafkaIntegration.Middleware
{
    using System;


    /// <summary>
    /// Wraps a topic producer provider to return outbox-wrapped producers.
    /// Messages produced through these producers are stored in the transactional outbox
    /// instead of being sent directly to Kafka.
    /// </summary>
    public class OutboxTopicProducerProvider :
        ITopicProducerProvider
    {
        readonly ITopicProducerProvider _producerProvider;
        readonly OutboxSendContext _outboxContext;

        public OutboxTopicProducerProvider(ITopicProducerProvider producerProvider, OutboxSendContext outboxContext)
        {
            _producerProvider = producerProvider;
            _outboxContext = outboxContext;
        }

        public ITopicProducer<TKey, TValue> GetProducer<TKey, TValue>(Uri address)
            where TValue : class
        {
            var producer = _producerProvider.GetProducer<TKey, TValue>(address);
            return new OutboxTopicProducer<TKey, TValue>(producer, _outboxContext, address);
        }

        public ConnectHandle ConnectSendObserver(ISendObserver observer)
        {
            return _producerProvider.ConnectSendObserver(observer);
        }
    }
}
