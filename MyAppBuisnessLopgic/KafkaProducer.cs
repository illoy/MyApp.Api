using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using System.Text.Json;

public class KafkaProducer
{
    private readonly IProducer<string, string> _producer;
    private readonly string _topic;

    public KafkaProducer(IConfiguration config)
    {
        var bootstrapServers = config["Kafka:BootstrapServers"];
        _topic = config["Kafka:Topic"];

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            // Додаткові налаштування для надійності
            Acks = Acks.All,
            MessageSendMaxRetries = 3,
            RetryBackoffMs = 1000,
            EnableIdempotence = true
        };

        _producer = new ProducerBuilder<string, string>(producerConfig).Build();
    }

    public async Task SendMessageAsync<T>(T message, string key = null)
    {
        try
        {
            var json = JsonSerializer.Serialize(message);

            var kafkaMessage = new Message<string, string>
            {
                Key = key ?? Guid.NewGuid().ToString(),
                Value = json,
                Headers = new Headers
                {
                    { "producer", System.Text.Encoding.UTF8.GetBytes("MyApp.Api") },
                    { "timestamp", System.Text.Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("o")) },
                    { "messageType", System.Text.Encoding.UTF8.GetBytes(typeof(T).Name) }
                }
            };

            var result = await _producer.ProduceAsync(_topic, kafkaMessage);

            Console.WriteLine($"Kafka: Sent message | Key={kafkaMessage.Key} | " +
                            $"Offset={result.Offset} | Partition={result.Partition}");
        }
        catch (ProduceException<string, string> e)
        {
            Console.WriteLine($"Kafka: Delivery failed: {e.Error.Reason}");
            throw;
        }
    }

    public void Dispose()
    {
        _producer?.Flush(TimeSpan.FromSeconds(5));
        _producer?.Dispose();
    }
}