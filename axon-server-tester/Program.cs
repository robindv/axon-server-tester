using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using UvA.AxonServer.Client;

namespace axon_server_tester
{
    public class Program
    {
        private const int NUM_ACTORS = 50;
        private const int NUM_EVENTS = 5;
        private const int NUM_ITERATIONS = 10000;

        public static void Main(string[] args)
        {
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
            var channel = GrpcChannel.ForAddress(new Uri("http://localhost:8124"));


            var perstids = Enumerable.Range(0, NUM_ACTORS).Select(a => $"id-{a}").ToList();

            Task.WaitAll(perstids.Select(i => TryForPersistenceId(channel, i)).ToArray());
        }

        private static async Task TryForPersistenceId(GrpcChannel channel, string persid)
        {
            var firstSequenceNumber = await ReadHighestSequenceNrAsync(persid, 0, channel);

            foreach (int n in Enumerable.Range(0, NUM_ITERATIONS))
            {
                var latestSequenceNumber = await ReadHighestSequenceNrAsync(persid, 0, channel);

                if (latestSequenceNumber != (n * NUM_EVENTS) + firstSequenceNumber)
                {
                    Console.WriteLine("Uhhh!");
                    return;
                }

                foreach (int i in Enumerable.Range(0, NUM_EVENTS))
                {
                    await WriteMessagesAsync(persid, "inhoud", latestSequenceNumber + i, channel);
                }
            }
            Console.WriteLine($"Persid: {persid} finished");
        }


        public static async Task<long> ReadHighestSequenceNrAsync(string persistenceId,
            long fromSequenceNr,
            ChannelBase channel)
        {
            var Client = new EventStore.EventStoreClient(channel);
            var _metadata = new Metadata();

            using (var call = Client.ReadHighestSequenceNrAsync(new ReadHighestSequenceNrRequest
                       {
                           AggregateId = persistenceId,
                           FromSequenceNr = fromSequenceNr - 1
                       },
                       _metadata))
            {
                var result = await call.ResponseAsync;
                return result.ToSequenceNr + 1;
            }
        }

        protected static async Task<bool> WriteMessagesAsync(string persid, string content, long seq, ChannelBase channel)
        {
            var Client = new EventStore.EventStoreClient(channel);
            var _metadata = new Metadata();
            using var call = Client.AppendEvent(_metadata);

            var messageId = Guid.NewGuid();
            var timestamp = new DateTimeOffset(DateTime.UtcNow).ToUnixTimeMilliseconds();

            var eventt = new Event()
            {
                AggregateIdentifier = persid,
                AggregateType = "aggregate",
                Snapshot = false,
                Timestamp = timestamp,
                MessageIdentifier = messageId.ToString(),
                AggregateSequenceNumber = seq,
                Payload = new SerializedObject()
                {
                    Data = ByteString.CopyFromUtf8(content),
                    Type = "json"
                },
            };

            await call.RequestStream.WriteAsync(eventt);

            await call.RequestStream.CompleteAsync();
            var res = await call.ResponseAsync;

            if (!res.Success)
            {
                return false;
            }

            return true;
        }
    }
}
