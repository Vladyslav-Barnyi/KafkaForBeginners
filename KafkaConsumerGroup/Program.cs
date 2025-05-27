using Confluent.Kafka;

class Program
{
    const string BootstrapServers = "localhost:19092";
    const string ThreePartTopic = "three-partition-topic";
    const string GroupId = "three-part-group";

    static void Main()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoOffsetStore = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(ThreePartTopic);

        Console.WriteLine($"Group Consumer 1 started. Subscribed to: {ThreePartTopic}\n");

        try
        {
            while (true)
            {
                var result = consumer.Consume(CancellationToken.None);
                Console.WriteLine($"[GROUP-1] Key: {result.Message.Key ?? "null"}" +
                                 $" | Value: {result.Message.Value}" +
                                 $" | Part: {result.Partition}" +
                                 $" | Offset: {result.Offset}");
                consumer.StoreOffset(result);
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Group Consumer 1 stopped.");
        }
    }
}