using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using System.Text.Json;

public class KafkaProducer
{
    private readonly string _bootstrapServers;
    private readonly string _topic;

    public KafkaProducer(IConfiguration config)
    {
        _bootstrapServers = config["Kafka:BootstrapServers"];
        _topic = config["Kafka:Topic"];
    }
    public async Task SendMessageAsync<T>(T message, string key = null)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers
        };

        using var producer = new ProducerBuilder<string, string>(config).Build();

        var json = JsonSerializer.Serialize(message);

        var kafkaMessage = new Message<string, string>
        {
            Key = key ?? Guid.NewGuid().ToString(), // Унікальний ключ, якщо не переданий
            Value = json,
            Headers = new Headers
            {
                { "producer", System.Text.Encoding.UTF8.GetBytes("MyApp.Api") },
                { "timestamp", System.Text.Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("o")) },
                { "messageType", System.Text.Encoding.UTF8.GetBytes(typeof(T).Name) }
            }
        };

        var result = await producer.ProduceAsync(_topic, kafkaMessage);

        Console.WriteLine($"Kafka: Sent {json} | Key={kafkaMessage.Key} | Offset={result.Offset}");
    }
}