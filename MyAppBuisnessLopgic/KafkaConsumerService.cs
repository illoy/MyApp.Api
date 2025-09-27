using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using System.Text.Json;
using Microsoft.Extensions.Logging;

public class KafkaConsumerService : BackgroundService
{
    private readonly IConsumer<Ignore, string> _consumer;
    private readonly string _topic;
    private readonly ILogger<KafkaConsumerService> _logger;

    public KafkaConsumerService(IConfiguration config, ILogger<KafkaConsumerService> logger)
    {
        _logger = logger;

        var bootstrapServers = config["Kafka:BootstrapServers"];
        _topic = config["Kafka:Topic"];

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = "myapp-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false, // Краще контролювати вручну
            EnableAutoOffsetStore = false
        };

        _consumer = new ConsumerBuilder<Ignore, string>(consumerConfig)
            .SetErrorHandler((_, e) => _logger.LogError($"Kafka Error: {e.Reason}"))
            .SetLogHandler((_, log) => _logger.LogInformation($"Kafka Log: {log.Message}"))
            .Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(_topic);
        _logger.LogInformation($"Kafka Consumer started. Listening to topic: {_topic}");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = _consumer.Consume(stoppingToken);

                if (consumeResult?.Message != null)
                {
                    await ProcessMessageAsync(consumeResult);
                    _consumer.StoreOffset(consumeResult);
                }
            }
            catch (ConsumeException e)
            {
                _logger.LogError($"Consume error: {e.Error.Reason}");
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Consumer operation cancelled");
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing Kafka message");
                await Task.Delay(1000, stoppingToken); // Пауза перед повторною спробою
            }
        }

        _logger.LogInformation("Closing Kafka consumer...");
        _consumer.Close();
    }

    private async Task ProcessMessageAsync(ConsumeResult<Ignore, string> consumeResult)
    {
        var message = consumeResult.Message;

        // Логуємо отримане повідомлення
        _logger.LogInformation($"Received message: {message.Value} | " +
                             $"Topic: {consumeResult.Topic} | " +
                             $"Partition: {consumeResult.Partition} | " +
                             $"Offset: {consumeResult.Offset}");

        // Тут ви можете додати логіку обробки повідомлень
        // Наприклад, збереження в базу даних, відправка email, тощо

        // Приклад обробки різних типів повідомлень
        var messageTypeHeader = message.Headers?.FirstOrDefault(h => h.Key == "messageType");
        if (messageTypeHeader != null)
        {
            var messageType = System.Text.Encoding.UTF8.GetString(messageTypeHeader.GetValueBytes());
            _logger.LogInformation($"Message type: {messageType}");
        }

        // Симулюємо обробку
        await Task.Delay(100);
    }

    public override void Dispose()
    {
        _consumer?.Close();
        _consumer?.Dispose();
        base.Dispose();
    }
}