namespace MassTransit.Registration
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Definition;
    using Monitoring.Health;


    public class BusRegistrationContext :
        Registration,
        IBusRegistrationContext
    {
        readonly BusHealth _busHealth;
        readonly IRegistrationCache<IEndpointRegistration> _endpoints;
        IConfigureReceiveEndpoint _configureReceiveEndpoints;

        public BusRegistrationContext(IConfigurationServiceProvider provider, BusHealth busHealth, IRegistrationCache<IEndpointRegistration> endpoints,
            IRegistrationCache<IConsumerRegistration> consumers, IRegistrationCache<ISagaRegistration> sagas,
            IRegistrationCache<IExecuteActivityRegistration> executeActivities, IRegistrationCache<IActivityRegistration> activities,
            IRegistrationCache<IFutureRegistration> futures)
            : base(provider, consumers, sagas, executeActivities, activities, futures)
        {
            _busHealth = busHealth;
            _endpoints = endpoints;
        }

        public void UseHealthCheck(IBusFactoryConfigurator configurator)
        {
            configurator.ConnectBusObserver(_busHealth);
            configurator.ConnectEndpointConfigurationObserver(_busHealth);
        }

        public void ConfigureEndpoints<T>(IReceiveConfigurator<T> configurator, IEndpointNameFormatter endpointNameFormatter = null)
            where T : IReceiveEndpointConfigurator
        {
            ConfigureEndpoints(configurator, endpointNameFormatter, NoFilter);
        }

        public void ConfigureEndpoints<T>(IReceiveConfigurator<T> configurator, IEndpointNameFormatter endpointNameFormatter,
            Action<IRegistrationFilterConfigurator> configureFilter)
            where T : IReceiveEndpointConfigurator
        {
            endpointNameFormatter ??= GetService<IEndpointNameFormatter>() ?? DefaultEndpointNameFormatter.Instance;

            var configureReceiveEndpoint = GetConfigureReceiveEndpoints();

            var builder = new RegistrationFilterConfigurator();
            configureFilter?.Invoke(builder);

            var registrationFilter = builder.Filter;

            List<IGrouping<string, IConsumerDefinition>> consumersByEndpoint = Consumers.Values
                .Where(x => registrationFilter.Matches(x) && !WasConfigured(x.ConsumerType))
                .Select(x => x.GetDefinition(this))
                .GroupBy(x => x.GetEndpointName(endpointNameFormatter))
                .ToList();

            List<IGrouping<string, ISagaDefinition>> sagasByEndpoint = Sagas.Values
                .Where(x => registrationFilter.Matches(x) && !WasConfigured(x.SagaType))
                .Select(x => x.GetDefinition(this))
                .GroupBy(x => x.GetEndpointName(endpointNameFormatter))
                .ToList();

            List<IActivityDefinition> activities = Activities.Values
                .Where(x => registrationFilter.Matches(x) && !WasConfigured(x.ActivityType))
                .Select(x => x.GetDefinition(this))
                .ToList();

            List<IGrouping<string, IActivityDefinition>> activitiesByExecuteEndpoint = activities
                .GroupBy(x => x.GetExecuteEndpointName(endpointNameFormatter))
                .ToList();

            List<IGrouping<string, IActivityDefinition>> activitiesByCompensateEndpoint = activities
                .GroupBy(x => x.GetCompensateEndpointName(endpointNameFormatter))
                .ToList();

            List<IGrouping<string, IExecuteActivityDefinition>> executeActivitiesByEndpoint = ExecuteActivities.Values
                .Where(x => registrationFilter.Matches(x) && !WasConfigured(x.ActivityType))
                .Select(x => x.GetDefinition(this))
                .GroupBy(x => x.GetExecuteEndpointName(endpointNameFormatter))
                .ToList();

            List<IGrouping<string, IFutureDefinition>> futuresByEndpoint = Futures.Values
                .Where(x => registrationFilter.Matches(x) && !WasConfigured(x.FutureType))
                .Select(x => x.GetDefinition(this))
                .GroupBy(x => x.GetEndpointName(endpointNameFormatter))
                .ToList();

            var endpointsWithName = _endpoints.Values
                .Select(x => x.GetDefinition(this))
                .Select(x => new
                {
                    Name = x.GetEndpointName(endpointNameFormatter),
                    Definition = x
                })
                .GroupBy(x => x.Name, (name, values) => new
                {
                    Name = name,
                    Definition = values.Select(x => x.Definition).Combine()
                })
                .ToList();

            IEnumerable<string> endpointNames = consumersByEndpoint.Select(x => x.Key)
                .Union(sagasByEndpoint.Select(x => x.Key))
                .Union(activitiesByExecuteEndpoint.Select(x => x.Key))
                .Union(executeActivitiesByEndpoint.Select(x => x.Key))
                .Union(futuresByEndpoint.Select(x => x.Key))
                .Union(endpointsWithName.Select(x => x.Name))
                .Except(activitiesByCompensateEndpoint.Select(x => x.Key));

            var endpoints =
                from e in endpointNames
                join c in consumersByEndpoint on e equals c.Key into cs
                from c in cs.DefaultIfEmpty()
                join s in sagasByEndpoint on e equals s.Key into ss
                from s in ss.DefaultIfEmpty()
                join a in activitiesByExecuteEndpoint on e equals a.Key into aes
                from a in aes.DefaultIfEmpty()
                join ea in executeActivitiesByEndpoint on e equals ea.Key into eas
                from ea in eas.DefaultIfEmpty()
                join f in futuresByEndpoint on e equals f.Key into fs
                from f in fs.DefaultIfEmpty()
                join ep in endpointsWithName on e equals ep.Name into eps
                from ep in eps.Select(x => x.Definition)
                    .DefaultIfEmpty(c?.Select(x => (IEndpointDefinition)new DelegateEndpointDefinition(e, x, x.EndpointDefinition)).Combine()
                        ?? s?.Select(x => (IEndpointDefinition)new DelegateEndpointDefinition(e, x, x.EndpointDefinition)).Combine()
                        ?? a?.Select(x => (IEndpointDefinition)new DelegateEndpointDefinition(e, x, x.ExecuteEndpointDefinition)).Combine()
                        ?? ea?.Select(x => (IEndpointDefinition)new DelegateEndpointDefinition(e, x, x.ExecuteEndpointDefinition)).Combine()
                        ?? f?.Select(x => (IEndpointDefinition)new DelegateEndpointDefinition(e, x, x.EndpointDefinition)).Combine()
                        ?? new NamedEndpointDefinition(e))
                select new
                {
                    Name = e,
                    Definition = ep,
                    Consumers = c,
                    Sagas = s,
                    Activities = a,
                    ExecuteActivities = ea,
                    Futures = f
                };

            foreach (var endpoint in endpoints)
            {
                configurator.ReceiveEndpoint(endpoint.Definition, endpointNameFormatter, cfg =>
                {
                    configureReceiveEndpoint.Configure(endpoint.Definition.GetEndpointName(endpointNameFormatter), cfg);

                    if (endpoint.Consumers != null)
                    {
                        foreach (var consumer in endpoint.Consumers)
                            ConfigureConsumer(consumer.ConsumerType, cfg);
                    }

                    if (endpoint.Sagas != null)
                    {
                        foreach (var saga in endpoint.Sagas)
                            ConfigureSaga(saga.SagaType, cfg);
                    }

                    if (endpoint.Activities != null)
                    {
                        foreach (var activity in endpoint.Activities)
                        {
                            var compensateEndpointName = activity.GetCompensateEndpointName(endpointNameFormatter);

                            var compensateDefinition = activity.CompensateEndpointDefinition ??
                                endpointsWithName.SingleOrDefault(x => x.Name == compensateEndpointName)?.Definition;

                            if (compensateDefinition != null)
                            {
                                configurator.ReceiveEndpoint(compensateDefinition, endpointNameFormatter, compensateEndpointConfigurator =>
                                {
                                    configureReceiveEndpoint.Configure(compensateDefinition.GetEndpointName(endpointNameFormatter), cfg);

                                    ConfigureActivity(activity.ActivityType, cfg, compensateEndpointConfigurator);
                                });
                            }
                            else
                            {
                                configurator.ReceiveEndpoint(compensateEndpointName, compensateEndpointConfigurator =>
                                {
                                    configureReceiveEndpoint.Configure(compensateEndpointName, cfg);

                                    ConfigureActivity(activity.ActivityType, cfg, compensateEndpointConfigurator);
                                });
                            }
                        }
                    }

                    if (endpoint.ExecuteActivities != null)
                    {
                        foreach (var activity in endpoint.ExecuteActivities)
                            ConfigureExecuteActivity(activity.ActivityType, cfg);
                    }

                    if (endpoint.Futures != null)
                    {
                        foreach (var future in endpoint.Futures)
                            ConfigureFuture(future.FutureType, cfg);
                    }
                });
            }
        }

        public IConfigureReceiveEndpoint GetConfigureReceiveEndpoints()
        {
            if (_configureReceiveEndpoints != null)
                return _configureReceiveEndpoints;

            var configureReceiveEndpoints = GetService<IEnumerable<IConfigureReceiveEndpoint>>();

            if (configureReceiveEndpoints == null)
            {
                var configureReceiveEndpoint = GetService<IConfigureReceiveEndpoint>();
                if (configureReceiveEndpoint != null)
                    configureReceiveEndpoints = new[] {configureReceiveEndpoint};
            }

            _configureReceiveEndpoints = configureReceiveEndpoints == null
                ? new ConfigureReceiveEndpoint(Array.Empty<IConfigureReceiveEndpoint>())
                : new ConfigureReceiveEndpoint(configureReceiveEndpoints.ToArray());

            return _configureReceiveEndpoints;
        }

        static void NoFilter(IRegistrationFilterConfigurator configurator)
        {
        }


        class ConfigureReceiveEndpoint :
            IConfigureReceiveEndpoint
        {
            readonly IConfigureReceiveEndpoint[] _configurators;

            public ConfigureReceiveEndpoint(IConfigureReceiveEndpoint[] configurators)
            {
                _configurators = configurators;
            }

            public void Configure(string name, IReceiveEndpointConfigurator configurator)
            {
                for (var i = 0; i < _configurators.Length; i++)
                    _configurators[i].Configure(name, configurator);
            }
        }
    }
}
