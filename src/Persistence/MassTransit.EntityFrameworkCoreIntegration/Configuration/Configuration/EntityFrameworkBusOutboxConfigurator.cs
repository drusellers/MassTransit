#nullable enable
namespace MassTransit.Configuration
{
    using System;
    using DependencyInjection;
    using EntityFrameworkCoreIntegration;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Options;
    using Middleware.Outbox;


    public class EntityFrameworkBusOutboxConfigurator<TBus, TDbContext> :
        IEntityFrameworkBusOutboxConfigurator
        where TBus : class, IBus
        where TDbContext : DbContext
    {
        protected readonly IBusRegistrationConfigurator Configurator;
        protected readonly EntityFrameworkOutboxConfigurator<TBus, TDbContext> OutboxConfigurator;
        bool _registerOutboxDeliveryService;

        public EntityFrameworkBusOutboxConfigurator(IBusRegistrationConfigurator configurator,
            EntityFrameworkOutboxConfigurator<TBus, TDbContext> outboxConfigurator)
        {
            OutboxConfigurator = outboxConfigurator;
            Configurator = configurator;

            _registerOutboxDeliveryService = true;
        }

        /// <summary>
        /// The number of message to deliver at a time from the outbox
        /// </summary>
        public int MessageDeliveryLimit { get; set; } = 100;

        /// <summary>
        /// Transport Send timeout when delivering messages to the transport
        /// </summary>
        public TimeSpan MessageDeliveryTimeout { get; set; } = TimeSpan.FromSeconds(10);

        /// <summary>
        /// The bus name used to identify this bus's outbox messages.
        /// Null for the default IBus, otherwise the type name of TBus.
        /// </summary>
        protected string? BusName => typeof(TBus) == typeof(IBus) ? null : typeof(TBus).Name;

        protected bool RegisterOutboxDeliveryService => _registerOutboxDeliveryService;

        public void DisableDeliveryService()
        {
            _registerOutboxDeliveryService = false;
        }

        public virtual void Configure(Action<IEntityFrameworkBusOutboxConfigurator>? configure)
        {
            configure?.Invoke(this);

            Configurator.ReplaceScoped<IScopedBusContextProvider<TBus>, EntityFrameworkScopedBusContextProvider<TBus, TDbContext>>();

            // Register bus-specific notification for MultiBus support
            Configurator.AddSingleton(provider =>
            {
                var options = provider.GetRequiredService<IOptions<OutboxDeliveryServiceOptions<TBus>>>();
                return Bind<TBus>.Create<IBusOutboxNotification>(new BusOutboxNotification(options));
            });

            if (_registerOutboxDeliveryService)
            {
                Configurator.AddHostedService<BusOutboxDeliveryService<TBus, TDbContext>>();
                Configurator.AddOptions<OutboxDeliveryServiceOptions<TBus>>()
                    .Configure(options =>
                    {
                        options.BusName = BusName;
                        options.QueryDelay = OutboxConfigurator.QueryDelay;
                        options.QueryMessageLimit = OutboxConfigurator.QueryMessageLimit;
                        options.QueryTimeout = OutboxConfigurator.QueryTimeout;
                        options.MessageDeliveryLimit = MessageDeliveryLimit;
                        options.MessageDeliveryTimeout = MessageDeliveryTimeout;
                    });
            }
        }
    }


    /// <summary>
    /// Non-generic configurator for backward compatibility with the default IBus.
    /// </summary>
    public class EntityFrameworkBusOutboxConfigurator<TDbContext> :
        EntityFrameworkBusOutboxConfigurator<IBus, TDbContext>
        where TDbContext : DbContext
    {
        public EntityFrameworkBusOutboxConfigurator(IBusRegistrationConfigurator configurator,
            EntityFrameworkOutboxConfigurator<TDbContext> outboxConfigurator)
            : base(configurator, outboxConfigurator)
        {
        }

        public override void Configure(Action<IEntityFrameworkBusOutboxConfigurator>? configure)
        {
            configure?.Invoke(this);

            Configurator.ReplaceScoped<IScopedBusContextProvider<IBus>, EntityFrameworkScopedBusContextProvider<IBus, TDbContext>>();

            // Register singleton notification for the default bus (backward compatible)
            Configurator.AddSingleton<IBusOutboxNotification, BusOutboxNotification>();

            if (RegisterOutboxDeliveryService)
            {
                // Use the non-generic delivery service for backward compatibility
                Configurator.AddHostedService<BusOutboxDeliveryService<TDbContext>>();
                Configurator.AddOptions<OutboxDeliveryServiceOptions>()
                    .Configure(options =>
                    {
                        options.BusName = BusName;
                        options.QueryDelay = OutboxConfigurator.QueryDelay;
                        options.QueryMessageLimit = OutboxConfigurator.QueryMessageLimit;
                        options.QueryTimeout = OutboxConfigurator.QueryTimeout;
                        options.MessageDeliveryLimit = MessageDeliveryLimit;
                        options.MessageDeliveryTimeout = MessageDeliveryTimeout;
                    });
            }
        }
    }
}
