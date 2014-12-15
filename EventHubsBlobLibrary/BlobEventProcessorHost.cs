using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventHubsBlobLibrary
{
    public class BlobEventProcessorHost
    {
        public static async Task Start(string hostName, string eventHubName, string consumerGroupName, string containerName, string blobPrefix,
            string serviceBusConnectionString, string storageConnectionString)
        {
            // Create Consumer Group if it doesn't exist
            if (consumerGroupName != null)
            {
                NamespaceManager manager = NamespaceManager.CreateFromConnectionString(serviceBusConnectionString);
                ConsumerGroupDescription description = new ConsumerGroupDescription(eventHubName, consumerGroupName);
                manager.CreateConsumerGroupIfNotExists(description);
            }

            // Create the Event Processor Host
            var host = new EventProcessorHost(
                hostName,
                eventHubName,
                consumerGroupName == null ? EventHubConsumerGroup.DefaultGroupName : consumerGroupName,
                serviceBusConnectionString,
                storageConnectionString,
                eventHubName);

            // Create the Factory
            var factory = new BlobEventProcessorHostFactory(storageConnectionString, containerName, blobPrefix);

            // Register the Factory
            await host.RegisterEventProcessorFactoryAsync(factory);
        }
    }
}
