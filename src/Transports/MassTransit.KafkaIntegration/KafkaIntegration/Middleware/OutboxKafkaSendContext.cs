#nullable enable
namespace MassTransit.KafkaIntegration.Middleware
{
    using System;
    using System.Collections.Generic;
    using System.Text.Json;
    using System.Threading;
    using Confluent.Kafka;
    using Context;


    /// <summary>
    /// Send context used to capture Kafka message data for outbox storage.
    /// Stores Kafka-specific properties (key, partition) alongside standard message data.
    /// </summary>
    public class OutboxKafkaSendContext<TKey, TValue> :
        MessageSendContext<TValue>,
        KafkaSendContext<TKey, TValue>,
        TransportSendContext<TValue>
        where TValue : class
    {
        public OutboxKafkaSendContext(TKey key, TValue message, Uri destinationAddress, CancellationToken cancellationToken)
            : base(message, cancellationToken)
        {
            Key = key;
            Partition = Partition.Any;
            DestinationAddress = destinationAddress;
        }

        public Partition Partition { get; set; }
        public TKey Key { get; set; }

        public IAsyncSerializer<TKey> KeySerializer { get; set; } = null!;
        public IAsyncSerializer<TValue> ValueSerializer { get; set; } = null!;

        public override void WritePropertiesTo(IDictionary<string, object> properties)
        {
            base.WritePropertiesTo(properties);

            // Store Kafka-specific properties
            if (Key != null)
            {
                properties[KafkaPropertyNames.Key] = SerializeKey(Key);
                properties[KafkaPropertyNames.KeyType] = typeof(TKey).AssemblyQualifiedName!;
            }

            if (Partition != Partition.Any)
                properties[KafkaPropertyNames.Partition] = Partition.Value;
        }

        static string SerializeKey(TKey key)
        {
            if (key is string stringKey)
                return stringKey;

            if (key is byte[] bytes)
                return Convert.ToBase64String(bytes);

            return JsonSerializer.Serialize(key);
        }


        static class KafkaPropertyNames
        {
            public const string Key = "MT-Kafka-Key";
            public const string KeyType = "MT-Kafka-KeyType";
            public const string Partition = "MT-Kafka-Partition";
        }
    }
}
