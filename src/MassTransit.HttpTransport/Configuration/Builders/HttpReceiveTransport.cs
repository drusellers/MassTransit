namespace MassTransit.HttpTransport.Configuration.Builders
{
    using System;
    using System.Net.Http;
    using System.Threading;
    using System.Threading.Tasks;
    using Hosting;
    using Logging;
    using MassTransit.Pipeline;
    using Microsoft.Owin.Hosting;
    using Pipeline;
    using Policies;
    using Transports;
    using Util;


    public class HttpReceiveTransport :
        IReceiveTransport
    {
        static readonly ILog _log = Logger.Get<HttpReceiveTransport>();
        readonly IHttpHost _host;
        readonly ReceiveEndpointObservable _receiveEndpointObservable;
        readonly ReceiveObservable _receiveObservable;
        readonly ReceiveSettings _settings;

        public HttpReceiveTransport(IHttpHost host, ReceiveSettings settings, object[] routeBindings)
        {
            _host = host;
            _settings = settings;

            _receiveObservable = new ReceiveObservable();
            _receiveEndpointObservable = new ReceiveEndpointObservable();
        }

        public ConnectHandle ConnectReceiveObserver(IReceiveObserver observer)
        {
            return _receiveObservable.Connect(observer);
        }

        public ConnectHandle ConnectReceiveEndpointObserver(IReceiveEndpointObserver observer)
        {
            return _receiveEndpointObservable.Connect(observer);
        }

        public void Probe(ProbeContext context)
        {
            var scope = context.CreateScope("transport");
            scope.Add("type", "HTTP");
            scope.Set(_settings);
        }

        public ReceiveTransportHandle Start(IPipe<ReceiveContext> receivePipe)
        {
            var supervisor = new TaskSupervisor($"{TypeMetadataCache<HttpReceiveTransport>.ShortName} - {_host.Settings.GetInputAddress(_settings)}");

            IPipe<OwinHostContext> hostPipe = Pipe.Execute<OwinHostContext>(cxt =>
            {
                cxt.Instance.Start(receivePipe);
            });

            _host.OwinHostCache.Send(hostPipe, supervisor.StoppingToken).ConfigureAwait(false);

            return new Handle(supervisor);
        }

        class Faulted :
            ReceiveEndpointFaulted
        {
            public Faulted(Uri inputAddress, Exception exception)
            {
                InputAddress = inputAddress;
                Exception = exception;
            }

            public Uri InputAddress { get; }
            public Exception Exception { get; }
        }

        class Handle :
            ReceiveTransportHandle
        {
            readonly TaskSupervisor _supervisor;

            public Handle(TaskSupervisor supervisor)
            {
                _supervisor = supervisor;
            }

            Task ReceiveTransportHandle.Stop(CancellationToken cancellationToken)
            {
                return _supervisor.Stop("Stop Receive Transport", cancellationToken);
            }
        }
    }
}