using Confluent.Kafka;
using System.Text.Json;

namespace MyApp.Api.Services

{
    public class KafkaProducer
    {
        private readonly string _bootstrapServers;
        private readonly string _topic;

        public KafkaProducer(IConfiguration config)
        {
            _bootstrapServers = config["Kafka:BootstrapServers"];
            _topic = config["Kafka:Topic"];
        }
        public async Task SendMessageAsync<T>(T message)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = _bootstrapServers
            };

            using var producer = new ProducerBuilder<Null, string>(config).Build();
            string json = JsonSerializer.Serialize(message);

            var result = await producer.ProduceAsync(_topic, new Message<Null, string>
            {
                Value = json
            });

            Console.WriteLine($"Kafka: Sent to {_topic} at offset {result.Offset}");
        }
    }
}
