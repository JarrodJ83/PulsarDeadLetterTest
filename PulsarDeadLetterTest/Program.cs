using System;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Flurl.Http;
using Pulsar.Client.Api;

namespace PulsarDeadLetterTest
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var topic = "test";
            var deadLetterTopic = $"{topic}-{subscription}-DLQ";
            var subscription = "pulsar-dead-letter-tester";

            await CreateTopic(topic);
            await CreateTopic(deadLetterTopic);

            string serviceUrl = "pulsar://pulsar:6650";
            PulsarClient client = new PulsarClientBuilder().ServiceUrl(serviceUrl).Build();

            var producer = await client.NewProducer().CreateAsync();

            var consumer = await client.NewConsumer().ConsumerName("test").SubscriptionName(subscription).DeadLetterPolicy(new DeadLetterPolicy(1, deadLetterTopic)).SubscribeAsync();

            await producer.SendAsync(System.Text.Encoding.UTF8.GetBytes(Guid.NewGuid().ToString());

            var nackCount = 0;
            while(true)
            {
                var message = await consumer.ReceiveAsync();

                Console.WriteLine($"Received Message {message.MessageId}");

                await consumer.NegativeAcknowledge(message);

                nackCount++;
                Console.WriteLine($"Nacked Cound: {nackCount}");
            }
        }

        static async Task CreateTopic(string topic)
        {
            var sw = Stopwatch.StartNew();
            try
            {
                HttpResponseMessage response = await $"http://pulsar:8080/admin/v2/persistent/{topic}/partitions"
                            .AllowAnyHttpStatus()
                            .PutAsync(new StringContent("3", Encoding.UTF8));

                if (response.StatusCode == HttpStatusCode.Conflict)
                {
                    return;
                }

                response.EnsureSuccessStatusCode();
            }
            catch(Exception)
            {
                if (sw.Elapsed > TimeSpan.FromMinutes(1)) throw;

                await Task.Delay(TimeSpan.FromSeconds(5));
                CreateTopic(topic);
            }
        }
    }
}
