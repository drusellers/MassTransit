﻿namespace MassTransit.HttpTransport.Configuration.Builders
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net.Http;
    using Clients;
    using Configurators;
    using EndpointConfigurators;
    using Hosting;
    using MassTransit.Builders;
    using MassTransit.Pipeline;
    using Pipeline;
    using Transports;


    public class HttpReceiveEndpointConfigurator :
        ReceiveEndpointConfigurator,
        IHttpReceiveEndpointConfigurator,
        IBusFactorySpecification
    {
        readonly IHttpHost _host;
        readonly ReceiveSettings _settings;

        public HttpReceiveEndpointConfigurator(IHttpHost host, string path, IConsumePipe consumePipe = null) :
            base(consumePipe)
        {
            _host = host;
            _settings = new HttpReceiveSettings(host.Settings.Host, host.Settings.Port, path);
        }

        public HttpReceiveEndpointConfigurator(IHttpHost host, ReceiveSettings settings, IConsumePipe consumePipe = null) :
            base(consumePipe)
        {
            _host = host;
            _settings = settings;
        }

        public override IEnumerable<ValidationResult> Validate()
        {
            foreach (ValidationResult result in base.Validate())
                yield return result.WithParentKey($"{_settings.Path}");
        }

        public void Apply(IBusBuilder builder)
        {
            HttpReceiveEndpointBuilder endpointBuilder = null;
            var receivePipe = CreateReceivePipe(builder, consumePipe =>
            {
                endpointBuilder = new HttpReceiveEndpointBuilder(consumePipe);
            
                return endpointBuilder;
            });
            
            if (endpointBuilder == null)
                throw new InvalidOperationException("The endpoint builder was not initialized");
            
            var transport = new HttpReceiveTransport(_host, _settings, endpointBuilder.GetHttpRouteBindings().ToArray());

            builder.AddReceiveEndpoint(_settings.Path ?? NewId.Next().ToString(), new ReceiveEndpoint(transport, receivePipe));
        }

        protected override Uri GetInputAddress()
        {
            return _host.Settings.GetInputAddress(_settings);
        }

        protected override Uri GetErrorAddress()
        {
            string errorQueueName = _settings.Path + "_error";
            var sendSettings = new HttpSendSettingsImpl(HttpMethod.Get, errorQueueName);

            //sendSettings.BindToQueue(errorQueueName);

            return _host.Settings.GetSendAddress(sendSettings);
        }

        protected override Uri GetDeadLetterAddress()
        {
            string deadLetterQueueName = _settings.Path + "_skipped";
            var sendSettings = new HttpSendSettingsImpl(HttpMethod.Delete, deadLetterQueueName);

            //sendSettings.BindToQueue(errorQueueName);

            return _host.Settings.GetSendAddress(sendSettings);
        }

    }
}