using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventHubsBlobLibrary
{
    public class BlobEventProcessorHostFactory : IEventProcessorFactory
    {
        public string containerName { get; set; }
        public string blobPrefix { get; set; }
        public string storageConnectionString { get; set; }

        public BlobEventProcessorHostFactory(string storageConnectionString, string containerName, string blobPrefix)
        {
            this.containerName = containerName;
            this.blobPrefix = blobPrefix;
            this.storageConnectionString = storageConnectionString;
        }

        public IEventProcessor CreateEventProcessor(PartitionContext context)
        {
            var processor = new BlobEventProcessor();

            Console.WriteLine("BlobEventProcessorHostFactory creating BlobEventProcessor for partition {0}", context.Lease.PartitionId);

            processor.storageConnectionString = storageConnectionString;
            processor.containerName = containerName;
            processor.blobPrefix = blobPrefix;

            return processor;
        }
    }
}
