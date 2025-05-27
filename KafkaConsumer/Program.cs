using Confluent.Kafka;
using System;
using System.Threading;

class Program
{
    const string BootstrapServers = "localhost:19092";
    const string SinglePartTopic = "single-part-topic";

    static void Main()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = "Individual", // Individual consumer
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoOffsetStore = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();

        // Manually assign to partition 0
        consumer.Assign(new TopicPartition(SinglePartTopic, new Partition(0)));

        Console.WriteLine($"Individual consumer listening to {SinglePartTopic}, Partition 0...\n");

        try
        {
            while (true)
            {
                var result = consumer.Consume(CancellationToken.None);
                Console.WriteLine($"[INDIVIDUAL] Key: {result.Message.Key ?? "null"}" +
                                 $" | Value: {result.Message.Value}" +
                                 $" | Part: {result.Partition}" +
                                 $" | Offset: {result.Offset}");
                consumer.StoreOffset(result);
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Individual consumer stopped.");
        }
    }
}