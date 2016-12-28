using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NServiceBus.Raw;
using NServiceBus.Transport;

namespace NServiceBus.SqlServer.Shovel
{
    public class ShovelConfiguration
    {
        Dictionary<string, QueueInfo> queues = new Dictionary<string, QueueInfo>();

        public void AddQueue(string key, string connectionString, ICollection<string> catalogs, string queueTable = null)
        {
            queues.Add(key, new QueueInfo(connectionString, catalogs, queueTable));
        }

        public async Task<ShovelInstance> Start()
        {
            var instance = new ShovelInstance();
            await instance.Start(queues, "error").ConfigureAwait(false);
            return instance;
        }
    }
}