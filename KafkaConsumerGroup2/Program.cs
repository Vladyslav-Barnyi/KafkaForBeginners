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
        using var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = BootstrapServers }).Build();
        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(ThreePartTopic);

        var metadata = adminClient.GetMetadata(ThreePartTopic, TimeSpan.FromSeconds(10));
        var partitions = metadata.Topics[0].Partitions;
/*
        var offsets = new List<TopicPartitionOffset>();
        foreach (var partition in partitions)
        {
            offsets.Add(new TopicPartitionOffset(
                new TopicPartition(ThreePartTopic, new Partition(partition.PartitionId)),
                Offset.Beginning
            ));
        }

        consumer.Commit(offsets);
*/


        Console.WriteLine($"Group Consumer 2 started. Subscribed to: {ThreePartTopic}\n");

        try
        {
            while (true)
            {
                var result = consumer.Consume(CancellationToken.None);
                Console.WriteLine($"[GROUP-2] Key: {result.Message.Key ?? "null"}" +
                                 $" | Value: {result.Message.Value}" +
                                 $" | Part: {result.Partition}" +
                                 $" | Offset: {result.Offset}");
                consumer.StoreOffset(result);
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Group Consumer 2 stopped.");
        }
    }
}