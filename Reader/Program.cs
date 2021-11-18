// See https://aka.ms/new-console-template for more information

using Grpc.Core;
using Grpc.Net.Client;
using UvA.AxonServer.Client;

const long EventsPerRequest = 500L;

var i = 0L;

AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
var channel = GrpcChannel.ForAddress(new Uri("http://localhost:8124"));
var client = new EventStore.EventStoreClient(channel);
var metadata = new Metadata();

// Start copying
using var call = client.ListEvents(metadata);
Console.WriteLine("Requesting..");
await call.RequestStream.WriteAsync(new GetEventsRequest()
{
    TrackingToken = 0,
    NumberOfPermits = EventsPerRequest
});

try
{
    while (await call.ResponseStream.MoveNext(CancellationToken.None))
    {
        i++;
        var element = call.ResponseStream.Current;


        if (i % EventsPerRequest == 0)
        {
            Console.WriteLine($"Requesting next set of events: {i}");
            await call.RequestStream.WriteAsync(new GetEventsRequest()
            {
                NumberOfPermits = EventsPerRequest,
            });
        }
    }
}
catch (RpcException e)
{
    // Most likely the request was cancelled, so we will just do nothing..
    Console.WriteLine($"Stopped processing, reason: {e.Status}");
}
