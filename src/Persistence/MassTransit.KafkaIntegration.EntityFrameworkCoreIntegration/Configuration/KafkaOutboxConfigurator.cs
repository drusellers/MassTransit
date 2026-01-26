#nullable enable
namespace MassTransit.KafkaIntegration.EntityFrameworkCoreIntegration
{
    using System;
    using EntityFrameworkCoreIntegration;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.DependencyInjection.Extensions;


    /// <summary>
    /// Configurator for Kafka transactional outbox with Entity Framework Core.
    /// </summary>
    public class KafkaOutboxConfigurator<TDbContext> :
        IKafkaOutboxConfigurator
        where TDbContext : DbContext
    {
        readonly IServiceCollection _services;

        bool _deliveryServiceEnabled = true;
        int _messageDeliveryLimit = 100;
        TimeSpan _messageDeliveryTimeout = TimeSpan.FromSeconds(30);
        TimeSpan _queryDelay = TimeSpan.FromSeconds(10);
        int _queryMessageLimit = 100;
        TimeSpan _queryTimeout = TimeSpan.FromSeconds(30);

        public KafkaOutboxConfigurator(IServiceCollection services)
        {
            _services = services;
        }

        public int MessageDeliveryLimit
        {
            set => _messageDeliveryLimit = value;
        }

        public TimeSpan MessageDeliveryTimeout
        {
            set => _messageDeliveryTimeout = value;
        }

        public TimeSpan QueryDelay
        {
            set => _queryDelay = value;
        }

        public int QueryMessageLimit
        {
            set => _queryMessageLimit = value;
        }

        public TimeSpan QueryTimeout
        {
            set => _queryTimeout = value;
        }

        public void DisableDeliveryService()
        {
            _deliveryServiceEnabled = false;
        }

        public void Configure()
        {
            _services.AddOptions<KafkaOutboxDeliveryServiceOptions>()
                .Configure(options =>
                {
                    options.MessageDeliveryLimit = _messageDeliveryLimit;
                    options.MessageDeliveryTimeout = _messageDeliveryTimeout;
                    options.QueryDelay = _queryDelay;
                    options.QueryMessageLimit = _queryMessageLimit;
                    options.QueryTimeout = _queryTimeout;
                });

            if (_deliveryServiceEnabled)
            {
                _services.TryAddSingleton<KafkaOutboxDeliveryService<TDbContext>>();
                _services.AddHostedService(provider =>
                    provider.GetRequiredService<KafkaOutboxDeliveryService<TDbContext>>());
            }
        }
    }
}
