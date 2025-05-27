using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System;
using System.Threading.Tasks;

const string BootstrapServers = "localhost:19092";  // <<< Use this!
const string TopicName = "single-part-topic";     // Topic to auto-create
const string MultiPartitionTopic = "three-partition-topic"; // 3-partition topic

var adminConfig = new AdminClientConfig { BootstrapServers = BootstrapServers };
var producerConfig = new ProducerConfig { BootstrapServers = BootstrapServers, Partitioner = Partitioner.Random };

using var adminClient = new AdminClientBuilder(adminConfig).Build();

try
{
    // Check if topic exists
    var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
    var topicExists = metadata.Topics.Exists(t => t.Topic == TopicName);

    if (!topicExists)
    {
        await adminClient.CreateTopicsAsync(new[]
        {
            new TopicSpecification
            {
                Name = TopicName,
                NumPartitions = 1, // Single partition
                ReplicationFactor = 1
            },
            new TopicSpecification
            {
                Name = MultiPartitionTopic,
                NumPartitions = 3, // Three partitions
                ReplicationFactor = 1,
                
            }
        });
        Console.WriteLine($"Topics created: {TopicName}, {MultiPartitionTopic}");
    }
    else
    {
        Console.WriteLine($"Topic {TopicName} already exists. Skipping creation.");
    }
}
catch (CreateTopicsException e)
{
    Console.WriteLine($"Topic creation failed: {e.Results[0].Error.Reason}");
}

// ====== Produce Messages ======
using var producer = new ProducerBuilder<string, string>(producerConfig).Build();
var rnd = new Random();

while (true)
{
    string key = $"value-{rnd.Next(1, 100000)}";
    var message = new Message<string, string>
    {
        Key = $"key-{rnd.Next(1, 100)}",
        Value = key
    };    /*       await producer.ProduceAsync(
              TopicName,
              new Message<string, string>
              {
                  Key = key,
                  Value = $"Order created at {DateTime.UtcNow}"
              }
          );
           Console.WriteLine($"Produced: {key}");*/

    // Send to 3-partition topic (keys distribute across partitions)
    var deliveryReport = await producer.ProduceAsync(MultiPartitionTopic, message);

    // Log partition and offset
    Console.WriteLine(
        $"Produced: '{deliveryReport.Value}' (Key: {deliveryReport.Key}) " +
        $"to Partition: {deliveryReport.Partition}, " +
        $"Offset: {deliveryReport.Offset}"
    );

    await Task.Delay(2000); // Send every 2 seconds
}