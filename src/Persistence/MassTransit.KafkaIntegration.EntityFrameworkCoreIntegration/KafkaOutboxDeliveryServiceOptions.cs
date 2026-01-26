#nullable enable
namespace MassTransit.KafkaIntegration.EntityFrameworkCoreIntegration
{
    using System;


    /// <summary>
    /// Options for the Kafka outbox delivery service.
    /// </summary>
    public class KafkaOutboxDeliveryServiceOptions
    {
        /// <summary>
        /// The maximum number of messages to deliver in a single batch.
        /// Default: 100
        /// </summary>
        public int MessageDeliveryLimit { get; set; } = 100;

        /// <summary>
        /// The timeout for delivering a single message to Kafka.
        /// Default: 30 seconds
        /// </summary>
        public TimeSpan MessageDeliveryTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// The delay between delivery attempts when no messages are available.
        /// Default: 10 seconds
        /// </summary>
        public TimeSpan QueryDelay { get; set; } = TimeSpan.FromSeconds(10);

        /// <summary>
        /// The maximum number of messages to query from the outbox at once.
        /// Default: 100
        /// </summary>
        public int QueryMessageLimit { get; set; } = 100;

        /// <summary>
        /// The timeout for database queries.
        /// Default: 30 seconds
        /// </summary>
        public TimeSpan QueryTimeout { get; set; } = TimeSpan.FromSeconds(30);
    }
}
