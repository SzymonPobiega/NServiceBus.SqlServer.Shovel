using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NServiceBus.Configuration.AdvanceExtensibility;
using NServiceBus.Extensibility;
using NServiceBus.Raw;
using NServiceBus.Routing;
using NServiceBus.Transport;
using NServiceBus.Transport.SQLServer;

namespace NServiceBus.SqlServer.Shovel
{
    public class ShovelInstance
    {
        Dictionary<string, IRawEndpointInstance> endpointsByCatalog = new Dictionary<string, IRawEndpointInstance>();
        List<IRawEndpointInstance> instances;

        internal async Task Start(Dictionary<string, QueueInfo> queues, string errorQueue)
        {
            instances = new List<IRawEndpointInstance>();
            var barrier = new TaskCompletionSource<bool>();

            foreach (var queueInfo in queues)
            {
                var instance = await RawEndpoint.Start(CreateEndpointConfig(queueInfo, barrier.Task, errorQueue)).ConfigureAwait(false);
                instances.Add(instance);

                foreach (var catalog in queueInfo.Value.Catalogs)
                {
                    endpointsByCatalog.Add(catalog, instance);
                }
            }

            barrier.SetResult(true);
        }

        RawEndpointConfiguration CreateEndpointConfig(KeyValuePair<string, QueueInfo> queueInfo, Task<bool> barrier, string errorQueue)
        {
            var config = RawEndpointConfiguration.Create(queueInfo.Value.QueueTable, (context, dispatcher) => OnMessage(context, dispatcher, barrier, queueInfo.Value.Catalogs), errorQueue);
            var transport = config.UseTransport<SqlServerTransport>()
                .ConnectionString(queueInfo.Value.ConnectionString)
                .DefaultSchema(queueInfo.Value.Schema);

            transport.GetSettings().Set<EndpointInstances>(new EndpointInstances());

            config.AutoCreateQueue();
            config.DefaultErrorHandlingPolicy(errorQueue, 5);
            return config;
        }

        async Task OnMessage(MessageContext context, IDispatchMessages dispatcher, Task<bool> barrier, ICollection<string> ownCatalogs)
        {
            //Ensure all endpoinst are created.
            await barrier.ConfigureAwait(false); 

            string destination;
            if (!context.Headers.TryGetValue("NServiceBus.SqlServer.Destination", out destination))
            {
                throw new Exception("No destination header present.");
            }
            context.Headers.Remove("NServiceBus.SqlServer.Destination");

            var address = QueueAddress.Parse(destination);

            var outgoingMessage = new OutgoingMessage(context.MessageId, context.Headers, context.Body);
            var operation = new TransportOperation(outgoingMessage, new UnicastAddressTag(destination));
            var operations = new TransportOperations(operation);

            if (ownCatalogs.Contains(address.Catalog)) //Forward to the same instance on the receiving connection/transaction
            {
                await dispatcher.Dispatch(operations, context.TransportTransaction, context.Context).ConfigureAwait(false);
            }
            else //Forward to different instance
            {
                IRawEndpointInstance forwarder;
                if (!endpointsByCatalog.TryGetValue(address.Catalog, out forwarder))
                {
                    throw new Exception($"Destination catalog {address.Catalog} is not configured.");
                }
                await forwarder.SendRaw(operations, new TransportTransaction(), new ContextBag()).ConfigureAwait(false);
            }
        }
        public async Task Stop()
        {
            foreach (var instance in instances)
            {
                await instance.Stop().ConfigureAwait(false);
            }
        }
    }
}
