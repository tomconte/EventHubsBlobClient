using Microsoft.ServiceBus.Messaging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventHubsBlobLibrary
{
    public class BlobEventProcessor : IEventProcessor
    {
        public string containerName { get; set; }
        public string blobPrefix { get; set; }
        public string storageConnectionString { get; set; }

        const int MAX_BLOCK_SIZE = 10 * 1024; // Max block size in bytes
        MemoryStream blockStream;

        byte[] eolBytes = Encoding.UTF8.GetBytes(",\r\n");
        byte[] startBytes = Encoding.UTF8.GetBytes("[");
        byte[] endBytes = Encoding.UTF8.GetBytes("{}]");

        string partitionId;
        private Stopwatch checkpointStopWatch;

        CloudStorageAccount storageAccount;
        CloudBlobClient blobClient;
        CloudBlobContainer container;
        CloudBlockBlob blob;
        List<string> blockList;
        long blockId;
        string startBlockId, endBlockId;

        public async Task CloseAsync(PartitionContext context, CloseReason reason)
        {
            Console.WriteLine("EventProcessor stopped for partition {0}, reason: {1}", partitionId, reason);
        }

        public async Task OpenAsync(PartitionContext context)
        {
            partitionId = context.Lease.PartitionId;

            Console.WriteLine("EventProcessor starting for partition {0}", partitionId);

            storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            blobClient = storageAccount.CreateCloudBlobClient();
            var blobPath = blobPrefix + "-" + partitionId + "-" + DateTime.Now.Ticks + ".json";
            container = blobClient.GetContainerReference(containerName);
            await container.CreateIfNotExistsAsync();
            blob = container.GetBlockBlobReference(blobPath);
            blockList = new List<string>();
            blockId = 0;
            blockStream = new MemoryStream();

            // Insert the initial opening bracket
            startBlockId = Convert.ToBase64String(Encoding.UTF8.GetBytes(999998.ToString("d6")));
            using (var startStream = new MemoryStream(startBytes))
            {
                await blob.PutBlockAsync(startBlockId, startStream, null);
            }
            blockList.Add(startBlockId);

            // Prepare a magic block for the closing bracket
            endBlockId = Convert.ToBase64String(Encoding.UTF8.GetBytes(999999.ToString("d6")));
            using (var endStream = new MemoryStream(endBytes))
            {
                await blob.PutBlockAsync(endBlockId, endStream, null);
            }

            checkpointStopWatch = new Stopwatch();
            checkpointStopWatch.Start();

            Console.WriteLine("EventProcessor started for partition {0}", partitionId);
        }

        public async Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            try
            {
                foreach (EventData message in messages)
                {
                    // If we are reaching the maximum size for a block, send it to storage
                    byte[] messageBytes = message.GetBytes();
                    if (blockStream.Position + messageBytes.Length > MAX_BLOCK_SIZE)
                    {
                        // Go back to beginning of block stream
                        blockStream.Seek(0, SeekOrigin.Begin);
                        // Put the block
                        var blockIdBase64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(blockId.ToString("d6")));
                        //Console.WriteLine("Put Block {0} for partition {1}", blockId, partitionId);
                        await blob.PutBlockAsync(blockIdBase64, blockStream, null);
                        blockList.Add(blockIdBase64);
                        // Increment block ID and reset block stream
                        ++blockId;
                        blockStream.Dispose();
                        blockStream = new MemoryStream();

                        // TODO: checkpoint on time or number of records or size
                        // TODO: also need to checkpoint when/if no records are coming in
                        // TODO: need to rotate blobs

                        // Commit blocks & checkpoint
                        if (blockId % 10 == 0)
                        {
                            Console.WriteLine("EventProcessor checkpoint for partition {0}, committing {1} blocks", partitionId, blockList.Count);

                            // Checkpoint
                            await context.CheckpointAsync();

                            // Adds the end block before committing the block list
                            var newBlockList = new List<string>(blockList);
                            newBlockList.Add(endBlockId);
                            await blob.PutBlockListAsync(newBlockList);
                            
                            Console.WriteLine("EventProcessor checkpoint done for partition {0}", partitionId);
                        }
                    }
                    else
                    {
                        // Append the message body to block stream
                        blockStream.Write(messageBytes, 0, messageBytes.Length);
                        // Add newline characters
                        blockStream.Write(eolBytes, 0, eolBytes.Length);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception for partition {0}: {1}", partitionId, ex.Message);
                Console.WriteLine(ex.StackTrace);
            }
        }
    }
}
