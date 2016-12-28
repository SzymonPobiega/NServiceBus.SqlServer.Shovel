using System.Collections.Generic;

namespace NServiceBus.SqlServer.Shovel
{
    class QueueInfo
    {
        public string ConnectionString { get; }
        public ICollection<string> Catalogs { get; }
        public string QueueTable { get; set; }
        public string Schema { get; set; }

        public QueueInfo(string connectionString, ICollection<string> catalogs, string queueTable, string schema = null)
        {
            ConnectionString = connectionString;
            Catalogs = catalogs;
            QueueTable = queueTable ?? "Outgoing";
            Schema = schema ?? "dbo";
        }
    }
}