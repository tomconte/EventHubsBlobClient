using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace EventHubsBlobClient
{
    class Program
    {
        static void Main(string[] args)
        {
            // The Storage Account that will be used both to store the Event Processor Host coordination data, and the destination Blobs
            string storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=[account];AccountKey=[key]";

            // The root connection string for the Service Bus namespace where the Event Hub is created
            string serviceBusConnectionString = "Endpoint=sb://[namespace].servicebus.windows.net/;SharedAccessKeyName=[keyname];SharedAccessKey=[key]";
            
            // The Blob container where log files will be stored
            string containerName = "logs";
            // The prefix for the Blob file names
            string blobPrefix = "log";

            // The name of the local machine to be used for the Event Processor Host
            string hostName = System.Environment.MachineName;
            // The Event Hub name
            string eventHubName = "eventhub";
            // The Consumer Group name
            string consumerGroupName = "BlobStorage";

            EventHubsBlobLibrary.BlobEventProcessorHost.Start(hostName, eventHubName, consumerGroupName, containerName, blobPrefix, serviceBusConnectionString, storageConnectionString).Wait();

            while (true)
            {
                Thread.Sleep(TimeSpan.FromMinutes(5));
            }
        }
    }
}
