using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.Configuration.AdvanceExtensibility;
using NServiceBus.Routing;
using NServiceBus.SqlServer.Shovel;
using NServiceBus.Transport.SQLServer;

namespace Demo
{
    class Program
    {
        static string SenderConnectionString = @"Data Source=.\SQLEXPRESS;Initial Catalog=nservicebus1;Integrated Security=True";
        static string ReceiverConnectionString = @"Data Source=.\SQLEXPRESS;Initial Catalog=nservicebus2;Integrated Security=True";

        static void Main(string[] args)
        {
            Start().GetAwaiter().GetResult();
        }

        static async Task Start()
        {
            var senderConfig = new EndpointConfiguration("Sender");
            senderConfig.UsePersistence<InMemoryPersistence>();
            var routing = senderConfig.UseTransport<SqlServerTransport>()
                .ConnectionString(SenderConnectionString)
                .UseCatalogForEndpoint("Receiver", "nservicebus2")
                .UseOutgoingQueueForCatalog("nservicebus2", "Outgoing@dbo@nservicebus1")
                .Routing();

            routing.RouteToEndpoint(typeof(MyRequest), "Receiver");

            senderConfig.SendFailedMessagesTo("error");

            var receiverConfig = new EndpointConfiguration("Receiver");
            receiverConfig.UsePersistence<InMemoryPersistence>();
            receiverConfig.UseTransport<SqlServerTransport>()
                .ConnectionString(ReceiverConnectionString)
                .UseOutgoingQueueForCatalog("nservicebus1", "Outgoing@dbo@nservicebus2");

            receiverConfig.SendFailedMessagesTo("error");

            var sender = await Endpoint.Start(senderConfig);
            var receiver = await Endpoint.Start(receiverConfig);

            var shovelConfig = new ShovelConfiguration();
            shovelConfig.AddQueue("Sender", SenderConnectionString, new List<string> { "nservicebus1" });
            shovelConfig.AddQueue("Receiver", ReceiverConnectionString, new List<string> { "nservicebus2" });

            var shovel = await shovelConfig.Start();

            while (true)
            {
                Console.WriteLine("Press <enter> to send a message");
                Console.ReadLine();

                await sender.Send(new MyRequest());
            }
        }
    }

    class MyRequest : IMessage
    {
    }

    class MyResponse : IMessage
    {
    }

    class MyRequestHandler : IHandleMessages<MyRequest>
    {
        public Task Handle(MyRequest message, IMessageHandlerContext context)
        {
            Console.WriteLine("Got request");
            return context.Reply(new MyResponse());
        }
    }

    class MyResponseHandler : IHandleMessages<MyResponse>
    {
        public Task Handle(MyResponse message, IMessageHandlerContext context)
        {
            Console.WriteLine("Got response");
            return Task.FromResult(0);
        }
    }
}
