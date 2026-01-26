#nullable enable
namespace MassTransit.KafkaIntegration.EntityFrameworkCoreIntegration
{
    using System;


    /// <summary>
    /// Configurator for Kafka transactional outbox options.
    /// </summary>
    public interface IKafkaOutboxConfigurator
    {
        /// <summary>
        /// The maximum number of messages to deliver in a single batch.
        /// </summary>
        int MessageDeliveryLimit { set; }

        /// <summary>
        /// The timeout for delivering a single message to Kafka.
        /// </summary>
        TimeSpan MessageDeliveryTimeout { set; }

        /// <summary>
        /// The delay between delivery attempts when no messages are available.
        /// </summary>
        TimeSpan QueryDelay { set; }

        /// <summary>
        /// The maximum number of messages to query from the outbox at once.
        /// </summary>
        int QueryMessageLimit { set; }

        /// <summary>
        /// The timeout for database queries.
        /// </summary>
        TimeSpan QueryTimeout { set; }

        /// <summary>
        /// Disable the background delivery service. Messages will remain in the outbox
        /// until delivered by an external process.
        /// </summary>
        void DisableDeliveryService();
    }
}
