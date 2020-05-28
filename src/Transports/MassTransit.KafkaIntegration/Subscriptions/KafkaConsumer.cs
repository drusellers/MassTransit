namespace MassTransit.KafkaIntegration.Subscriptions
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Confluent.Kafka;
    using Context;
    using Transport;
    using Util;


    public class KafkaConsumer<TKey, TValue> :
        IKafkaConsumer
        where TValue : class
    {
        readonly IConsumer<TKey, TValue> _consumer;
        readonly bool _isAutoCommitEnabled;
        readonly ILogContext _logContext;
        readonly IKafkaReceiver<TKey, TValue> _receiver;
        readonly string _topic;
        CancellationTokenSource _cancellationTokenSource;
        Task _consumerTask;

        public KafkaConsumer(string topic, IConsumer<TKey, TValue> consumer, IKafkaReceiver<TKey, TValue> receiver, ILogContext logContext,
            bool isAutoCommitEnabled)
        {
            _topic = topic;
            _consumer = consumer;
            _receiver = receiver;
            _logContext = logContext;
            _isAutoCommitEnabled = isAutoCommitEnabled;
        }

        public Task Subscribe(CancellationToken cancellationToken)
        {
            LogContext.SetCurrentIfNull(_logContext);

            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            _consumerTask = Task.Run(async () =>
            {
                await Task.Yield();
                while (!_cancellationTokenSource.IsCancellationRequested)
                {
                    LogContext.SetCurrentIfNull(_logContext);
                    try
                    {
                        ConsumeResult<TKey, TValue> message = _consumer.Consume(_cancellationTokenSource.Token);

                        await _receiver.Handle(message, cancellationToken).ConfigureAwait(false);

                        if (!_isAutoCommitEnabled)
                            _consumer.Commit(message);
                    }
                    catch (OperationCanceledException e) when (e.CancellationToken == _cancellationTokenSource.Token)
                    {
                    }
                    catch (Exception e)
                    {
                        LogContext.Error?.Log(e, "Kafka subscription: {topicName} exception", _topic);
                        throw;
                    }
                }
            }, cancellationToken);

            LogContext.Info?.Log("Kafka subscription: {topicName} starting", _topic);
            _consumer.Subscribe(_topic);

            return _consumerTask.IsCompleted ? _consumerTask : TaskUtil.Completed;
        }

        public async Task Unsubscribe(CancellationToken cancellationToken)
        {
            LogContext.SetCurrentIfNull(_logContext);
            try
            {
                LogContext.Info?.Log("Kafka subscription: {topicName} stopping", _topic);

                _cancellationTokenSource.Cancel();
                await _consumerTask.ConfigureAwait(false);

                _consumer.Close();
                _consumer.Dispose();

                _cancellationTokenSource.Dispose();
            }
            catch (Exception e)
            {
                LogContext.Error?.Log(e, "Error occured while stopping kafka consumer: {topicName}", _topic);
            }
        }
    }
}