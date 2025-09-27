using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Text.Json;

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
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false
        };

        _consumer = new ConsumerBuilder<Ignore, string>(consumerConfig)
            .SetErrorHandler((_, e) => _logger.LogError($"Kafka Error: {e.Reason}"))
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
                await Task.Delay(1000, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing Kafka message");
                await Task.Delay(1000, stoppingToken);
            }
        }

        _logger.LogInformation("Closing Kafka consumer...");
        _consumer.Close();
    }

    private async Task ProcessMessageAsync(ConsumeResult<Ignore, string> consumeResult)
    {
        var message = consumeResult.Message;

        // Логуємо базову інформацію
        _logger.LogInformation($"📨 Received message: {message.Value} | " +
                             $"Topic: {consumeResult.Topic} | " +
                             $"Partition: {consumeResult.Partition} | " +
                             $"Offset: {consumeResult.Offset}");

        // Аналізуємо заголовки
        var messageType = GetHeaderValue(message.Headers, "messageType");
        var timestamp = GetHeaderValue(message.Headers, "timestamp");
        var producer = GetHeaderValue(message.Headers, "producer");

        _logger.LogInformation($"📋 Message details - Type: {messageType}, Producer: {producer}, Time: {timestamp}");

        // Обробка різних типів повідомлень
        await ProcessBusinessLogic(message.Value, messageType);

        // Симулюємо корисну роботу
        await Task.Delay(50);
    }

    private async Task ProcessBusinessLogic(string message, string messageType)
    {
        try
        {
            // Аналіз повідомлення та виконання дій
            if (message.Contains("Transferred") && message.Contains("$"))
            {
                await ProcessTransferMessage(message);
            }
            else if (message.Contains("Create User"))
            {
                await ProcessUserCreationMessage(message);
            }
            else
            {
                _logger.LogInformation($"🔍 Unknown message pattern: {message}");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"❌ Error processing business logic for message: {message}");
        }
    }

    private async Task ProcessTransferMessage(string message)
    {
        // Парсимо інформацію про трансфер
        var amountMatch = System.Text.RegularExpressions.Regex.Match(message, @"Transferred (\d+)\$");
        var usersMatch = System.Text.RegularExpressions.Regex.Match(message, @"from (.+?) to (.+?)$");

        if (amountMatch.Success && usersMatch.Success)
        {
            var amount = amountMatch.Groups[1].Value;
            var fromUser = usersMatch.Groups[1].Value;
            var toUser = usersMatch.Groups[2].Value;

            _logger.LogInformation($"💸 Processing transfer: {amount}$ from {fromUser} to {toUser}");

            // Тут можна додати корисну логіку:
            // - Збереження в окрему таблицю транзакцій
            // - Відправка email сповіщення
            // - Оновлення статистики
            // - Інтеграція з зовнішніми системами

            // Приклад: логуємо в файл або базу даних
            await LogTransactionToFile(amount, fromUser, toUser);

            _logger.LogInformation($"✅ Transfer processed successfully");
        }
    }

    private async Task ProcessUserCreationMessage(string message)
    {
        var userMatch = System.Text.RegularExpressions.Regex.Match(message, @"Create User (.+?) with balance: (\d+)\$");

        if (userMatch.Success)
        {
            var userName = userMatch.Groups[1].Value;
            var balance = userMatch.Groups[2].Value;

            _logger.LogInformation($"👤 Processing user creation: {userName} with {balance}$");

            // Корисні дії при створенні користувача:
            // - Створення профілю в системі аналітики
            // - Відправка welcome email
            // - Ініціалізація додаткових сервісів

            await Task.Delay(50); // Симуляція роботи
            _logger.LogInformation($"✅ User creation processed");
        }
    }

    private async Task LogTransactionToFile(string amount, string fromUser, string toUser)
    {
        try
        {
            var logEntry = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - Transfer: {amount}$ from {fromUser} to {toUser}\n";
            await File.AppendAllTextAsync("transactions.log", logEntry);
        }
        catch (Exception ex)
        {
            _logger.LogWarning($"Could not write to transactions log: {ex.Message}");
        }
    }

    private string GetHeaderValue(Headers headers, string key)
    {
        var header = headers?.FirstOrDefault(h => h.Key == key);
        return header != null ? System.Text.Encoding.UTF8.GetString(header.GetValueBytes()) : "Unknown";
    }

    public override void Dispose()
    {
        _consumer?.Close();
        _consumer?.Dispose();
        base.Dispose();
    }
}