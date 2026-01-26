#nullable enable
namespace MassTransit.KafkaIntegration.EntityFrameworkCoreIntegration
{
    using System;


    /// <summary>
    /// Represents a serialized message for delivery to Kafka from the outbox.
    /// Contains the already-serialized message body and metadata.
    /// </summary>
    public class SerializedKafkaMessage
    {
        /// <summary>
        /// The serialized message body (JSON).
        /// </summary>
        public string Body { get; set; } = null!;

        /// <summary>
        /// The content type of the message.
        /// </summary>
        public string ContentType { get; set; } = null!;

        /// <summary>
        /// The message types supported by this message.
        /// </summary>
        public string MessageType { get; set; } = null!;

        /// <summary>
        /// Serialized headers.
        /// </summary>
        public string? Headers { get; set; }

        /// <summary>
        /// The unique message ID.
        /// </summary>
        public Guid MessageId { get; set; }

        /// <summary>
        /// The correlation ID for tracing.
        /// </summary>
        public Guid? CorrelationId { get; set; }

        /// <summary>
        /// The conversation ID for message grouping.
        /// </summary>
        public Guid? ConversationId { get; set; }
    }
}
