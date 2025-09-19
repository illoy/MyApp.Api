using System;
using System.Threading;
using Confluent.Kafka;

class Program
{
    static void Main(string[] args)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9093",   // Kafka broker
            GroupId = "my-consumer-group",         // Consumer group id
            AutoOffsetReset = AutoOffsetReset.Earliest // Read from beginning if no offset is stored
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();

        consumer.Subscribe("test");  // your Kafka topic

        Console.WriteLine("Listening for messages...");

        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        try
        {
            while (true)
            {
                try
                {
                    var cr = consumer.Consume(cts.Token);
                    Console.WriteLine($"Message received: {cr.Message.Value} " +
                                      $"(Topic: {cr.Topic}, Partition: {cr.Partition}, Offset: {cr.Offset})");
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Consume error: {e.Error.Reason}");
                }
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Closing consumer...");
            consumer.Close();
        }
    }
}
