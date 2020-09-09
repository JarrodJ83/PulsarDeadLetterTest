using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Flurl.Http;
using Pulsar.Client.Api;
using Pulsar.Client.Common;

namespace PulsarDeadLetterTest
{
    class Program
    {
        static int maxRetry = 2;

        static async Task Main(string[] args)
        {
            var subscription = "pulsar-dead-letter-tester";
            var topic = $"public/default/{Guid.NewGuid()}";
            var deadLetterTopic = $"{topic}-{subscription}-DLQ";

            await WaitForPulsar(TimeSpan.FromSeconds(30));

            // SUCCESSFUL
            // await CreateTopic(topic);
            // UNSUCCESSFUL
            await CreateTopic(topic, 2);

            string serviceUrl = "pulsar://pulsar:6650";
            PulsarClient client = new PulsarClientBuilder().ServiceUrl(serviceUrl)
                .Build();

            var producer = await client.NewProducer().Topic(topic)                
                .CreateAsync();



            var consumer = await client.NewConsumer().Topic(topic).ConsumerName("test").SubscriptionName(subscription)
                .NegativeAckRedeliveryDelay(TimeSpan.FromSeconds(2))
                .DeadLetterPolicy(new DeadLetterPolicy(maxRetry))
                .SubscriptionType(SubscriptionType.Shared)
                .SubscribeAsync();

            var deadLetterConsumer = await client.NewConsumer().Topic(deadLetterTopic).ConsumerName("test").SubscriptionName(subscription)
                .SubscriptionType(SubscriptionType.Shared)
                .SubscribeAsync();

            await producer.SendAsync(System.Text.Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()));

            var consumerTask = ConsumeTopic(consumer);
            var deadLetterConsumerTask = ConsumeDeadLetterTopic(deadLetterConsumer);

            Task.WaitAny(new[]
            {
                consumerTask, deadLetterConsumerTask
            });

            if (consumerTask.IsFaulted) throw consumerTask.Exception;

            if (deadLetterConsumerTask.IsCompletedSuccessfully)
            {
                Console.WriteLine("Successfully moved to dead letter queue!");
                return;
            }

            if(!consumerTask.IsCompleted) throw new Exception("No new messages but no dead letter messages either.");
        }

        static async Task ConsumeTopic(IConsumer<byte[]> consumer)
        {
            var nackCount = 0;

            while (true)
            {
                var message = await consumer.ReceiveAsync();

                Console.WriteLine($"Received Message {message.MessageId}");
                await consumer.NegativeAcknowledge(message.MessageId);

                nackCount++;

                if (nackCount > maxRetry + 1) throw new InvalidOperationException($"Message has been received {nackCount} times but should have only been {maxRetry + 1}");

                Console.WriteLine($"Nacked Cound: {nackCount}");
            }
        }

        static async Task ConsumeDeadLetterTopic(IConsumer<byte[]> consumer)
        {
            var message = await consumer.ReceiveAsync();

            Console.WriteLine($"Received Dead Letter Message {message.MessageId}");
        }

        static async Task CreateTopic(string topic, int partitions = 0)
        {
            if(topic.Split('/').Length == 1)
            { 
                topic = $"public/default/{topic}";
            }

            var endpoint = $"http://pulsar:8080/admin/v2/persistent/{topic}";

            if (partitions > 0) endpoint += "/partitions";

            HttpResponseMessage response = await endpoint
                        .AllowAnyHttpStatus()
                        .PutAsync(new StringContent($"{partitions}", Encoding.UTF8));

            if (response.StatusCode == HttpStatusCode.Conflict)
            {
                return;
            }

            response.EnsureSuccessStatusCode();
        }

        static async Task WaitForPulsar(TimeSpan timeout)
        {
            var sw = Stopwatch.StartNew();
            Exception timeoutEx = new System.TimeoutException("Timeout waiting for Pulsar");

            while (sw.Elapsed < timeout)
            {
                try
                {
                    var response = await $"http://pulsar:8080/admin/v2/namespaces/public/default"
                                .AllowAnyHttpStatus()
                                .GetAsync();

                    response.EnsureSuccessStatusCode();

                    Console.WriteLine(await response.Content.ReadAsStringAsync());

                    return;
                }
                catch (Exception ex)
                {
                    timeoutEx = new System.TimeoutException("Timeout waiting for Pulsar", ex);
                    await Task.Delay(TimeSpan.FromSeconds(5));                    
                }
            }

            throw timeoutEx;
        }
    }
}
