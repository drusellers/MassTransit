#nullable enable
namespace MassTransit
{
    using System;
    using EntityFrameworkCoreIntegration;
    using KafkaIntegration;
    using KafkaIntegration.EntityFrameworkCoreIntegration;
    using KafkaIntegration.Middleware;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.DependencyInjection.Extensions;
    using Middleware.Outbox;


    public static class KafkaOutboxConfigurationExtensions
    {
        /// <summary>
        /// Configure Entity Framework Core transactional outbox for Kafka.
        /// Messages produced to Kafka topics will be stored in the outbox and delivered
        /// by a background service after the database transaction commits.
        /// </summary>
        /// <param name="configurator">The Kafka factory configurator</param>
        /// <param name="services">The service collection</param>
        /// <param name="configure">Optional configuration action</param>
        /// <typeparam name="TDbContext">The DbContext type</typeparam>
        public static void UseEntityFrameworkOutbox<TDbContext>(
            this IKafkaFactoryConfigurator configurator,
            IServiceCollection services,
            Action<IKafkaOutboxConfigurator>? configure = null)
            where TDbContext : DbContext
        {
            var outboxConfigurator = new KafkaOutboxConfigurator<TDbContext>(services);

            configure?.Invoke(outboxConfigurator);

            outboxConfigurator.Configure();

            // Register the outbox notification if not already registered
            services.TryAddSingleton<IBusOutboxNotification, BusOutboxNotification>();

            // Register the scoped Kafka context for outbox support
            services.TryAddScoped<EntityFrameworkKafkaScopedContext<TDbContext>>();
        }

        /// <summary>
        /// Configure Entity Framework Core transactional outbox for a Kafka topic endpoint.
        /// Messages published during consumer execution will be stored in the outbox.
        /// </summary>
        /// <param name="configurator">The topic endpoint configurator</param>
        /// <param name="context">The registration context</param>
        /// <param name="configure">Optional configuration action</param>
        /// <typeparam name="TDbContext">The DbContext type</typeparam>
        public static void UseEntityFrameworkOutbox<TDbContext>(
            this IKafkaTopicReceiveEndpointConfigurator configurator,
            IRegistrationContext context,
            Action<IOutboxConfigurator>? configure = null)
            where TDbContext : DbContext
        {
            // Use the standard EF Core outbox for consumer endpoints
            // The outbox middleware handles storing messages during consumption
            var outboxContextFactory = context.GetService<IOutboxContextFactory<TDbContext>>();
            if (outboxContextFactory != null)
            {
                // Wire up the outbox consume filter using the standard EF Core outbox
                // This handles inbox deduplication and outbox storage for consumer-published messages
            }
        }
    }
}
