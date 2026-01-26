#nullable enable
namespace MassTransit.KafkaIntegration.Middleware
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Confluent.Kafka;
    using MassTransit.Middleware;
    using Serialization;


    /// <summary>
    /// Wraps a topic producer to intercept Produce calls and store messages in the outbox
    /// instead of directly producing to Kafka. Messages are delivered by the outbox delivery service.
    /// </summary>
    public class OutboxTopicProducer<TKey, TValue> :
        ITopicProducer<TKey, TValue>
        where TValue : class
    {
        readonly ITopicProducer<TKey, TValue> _producer;
        readonly OutboxSendContext _outboxContext;
        readonly Uri _topicAddress;

        public OutboxTopicProducer(ITopicProducer<TKey, TValue> producer, OutboxSendContext outboxContext, Uri topicAddress)
        {
            _producer = producer;
            _outboxContext = outboxContext;
            _topicAddress = topicAddress;
        }

        public Task Produce(TKey key, TValue value, CancellationToken cancellationToken = default)
        {
            return Produce(key, value, Pipe.Empty<KafkaSendContext<TKey, TValue>>(), cancellationToken);
        }

        public async Task Produce(TKey key, TValue value, IPipe<KafkaSendContext<TKey, TValue>> pipe, CancellationToken cancellationToken = default)
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));

            var sendContext = new OutboxKafkaSendContext<TKey, TValue>(key, value, _topicAddress, cancellationToken);

            // Set up serialization for outbox storage
            ConfigureSerialization(sendContext);

            if (pipe.IsNotEmpty())
                await pipe.Send(sendContext).ConfigureAwait(false);

            await _outboxContext.AddSend(sendContext).ConfigureAwait(false);
        }

        public Task Produce(TKey key, object values, CancellationToken cancellationToken = default)
        {
            return Produce(key, values, Pipe.Empty<KafkaSendContext<TKey, TValue>>(), cancellationToken);
        }

        public async Task Produce(TKey key, object values, IPipe<KafkaSendContext<TKey, TValue>> pipe, CancellationToken cancellationToken = default)
        {
            if (values == null)
                throw new ArgumentNullException(nameof(values));

            (var message, IPipe<SendContext<TValue>> initializerPipe) =
                await Initializers.MessageInitializerCache<TValue>.InitializeMessage(values, cancellationToken).ConfigureAwait(false);

            var sendContext = new OutboxKafkaSendContext<TKey, TValue>(key, message, _topicAddress, cancellationToken);

            // Set up serialization for outbox storage
            ConfigureSerialization(sendContext);

            if (initializerPipe.IsNotEmpty())
                await initializerPipe.Send(sendContext).ConfigureAwait(false);

            if (pipe.IsNotEmpty())
                await pipe.Send(sendContext).ConfigureAwait(false);

            await _outboxContext.AddSend(sendContext).ConfigureAwait(false);
        }

        public ConnectHandle ConnectSendObserver(ISendObserver observer)
        {
            return _producer.ConnectSendObserver(observer);
        }

        static void ConfigureSerialization(OutboxKafkaSendContext<TKey, TValue> context)
        {
            // Use the default JSON serializer for outbox storage
            context.Serializer = SystemTextJsonMessageSerializer.Instance;
            context.ContentType = SystemTextJsonMessageSerializer.JsonContentType;
        }
    }
}
