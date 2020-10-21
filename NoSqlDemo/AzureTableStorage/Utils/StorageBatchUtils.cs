using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureTableStorage.Utils
{
  public interface IStorageBatchUtils
	{
    void InsertBatchParallel<T>(CloudTable table, List<T> entities, int maxDegreeParallelism) where T : TableEntity;
    void InsertBatch<T>(CloudTable table, ConcurrentBag<T> entities) where T : TableEntity;
    Task InsertBatchAsync<T>(CloudTable table, List<T> customerEntities) where T : TableEntity;

  }


  public class StorageBatchUtils : IStorageBatchUtils
	{
    public void InsertBatchParallel<T>(CloudTable table, List<T> entities, int maxDegreeParallelism) where T : TableEntity
		{
      var rangePartitioner = Partitioner.Create(0, entities.Count);
      Parallel.ForEach(rangePartitioner,
        new ParallelOptions { MaxDegreeOfParallelism = maxDegreeParallelism },
        (range, loopState) =>
        {
          ConcurrentBag<T> chunk = new ConcurrentBag<T>();
            // Loop over each range element without a delegate invocation. 
            for (int i = range.Item1; i < range.Item2; i++)
          {
            chunk.Add(entities[i]);

          }
          Console.WriteLine($"Range: {range.Item1}-{range.Item2}");
          InsertBatch<T>(table, chunk);
        });
    }

    public void InsertBatch<T>(CloudTable table, ConcurrentBag<T> entities) where T : TableEntity
    {
      int rowOffset = 0;

      // Need to chunk the data down to 100's. Can only insert 100 at a time.
      while (rowOffset < entities.Count)
      {
        var rows = entities.Skip(rowOffset).Take(100).ToList();

        var partitionKeys = rows.Select(_ => _.PartitionKey).Distinct();

        // Can only batch insert same partitionKeys per batch
        foreach (var pkRow in partitionKeys)
        {
          var rowsToAdd = rows.Where(_ => _.PartitionKey == pkRow).ToList();
          var batch = new TableBatchOperation();

          foreach (var row in rowsToAdd)
          {
            batch.InsertOrReplace(row);
          }
          // submit - MUST BE SYNCHRONOUS IN A PARALLEL.FOREACH
          //          so we know when this thread is done.
          var task = Task.Run(async () => await table.ExecuteBatchAsync(batch));
          var result = task.Result;
        }

        rowOffset += rows.Count;
      }
    }






		public async Task InsertBatchAsync<T>(CloudTable table, List<T> entities) where T : TableEntity
		{
      int rowOffset = 0;

      var tasks = new List<Task>();

      entities = entities.OrderBy(_ => _.PartitionKey).ToList();

      // Need to chunk the data down to 100's. Can only insert 100 at a time.
      while (rowOffset < entities.Count)
      {
        // next batch
        Console.WriteLine($"Batch: {rowOffset}");
        var rows = entities.Skip(rowOffset).Take(100).ToList();

        var partitionKeys = rows.Select(_ => _.PartitionKey).Distinct();

        // Can only batch insert same partitionKeys per batch
        foreach (var pkRow in partitionKeys)
        {
          var rowsToAdd = rows.Where(_ => _.PartitionKey == pkRow).ToList();
          var batch = new TableBatchOperation();

          foreach (var row in rowsToAdd)
          {
            batch.InsertOrReplace(row);
          }
          await table.ExecuteBatchAsync(batch);
        }

        rowOffset += rows.Count;
      }
    }



		


	}
}
