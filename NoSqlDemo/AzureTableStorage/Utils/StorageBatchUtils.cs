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
    void InsertBatch<T>(CloudTable table, ConcurrentBag<T> entities) where T : TableEntity;
    Task InsertBatch<T>(CloudTable table, List<T> customerEntities) where T : TableEntity;

  }


  public class StorageBatchUtils : IStorageBatchUtils
	{

    public void InsertBatch<T>(CloudTable table, ConcurrentBag<T> entities) where T : TableEntity
    {
      int rowOffset = 0;

      while (rowOffset < entities.Count)
      {
        var rows = entities.Skip(rowOffset).Take(100).ToList();

        var partitionKeys = rows.Select(_ => _.PartitionKey).Distinct();

        foreach (var pkRow in partitionKeys)
        {
          var rowsToAdd = rows.Where(_ => _.PartitionKey == pkRow).ToList();
          var batch = new TableBatchOperation();

          foreach (var row in rowsToAdd)
          {
            batch.InsertOrReplace(row);
          }
          // submit
          var task = Task.Run(async () => await table.ExecuteBatchAsync(batch));
          var result = task.Result;
        }

        rowOffset += rows.Count;
      }
    }

		public async Task InsertBatch<T>(CloudTable table, List<T> entities) where T : TableEntity
		{
      int rowOffset = 0;

      var tasks = new List<Task>();

      entities = entities.OrderBy(_ => _.PartitionKey).ToList();

      while (rowOffset < entities.Count)
      {
        // next batch
        Console.WriteLine($"Batch: {rowOffset}");
        var rows = entities.Skip(rowOffset).Take(100).ToList();

        var partitionKeys = rows.Select(_ => _.PartitionKey).Distinct();

        foreach (var pkRow in partitionKeys)
        {
          var rowsToAdd = rows.Where(_ => _.PartitionKey == pkRow).ToList();
          await UpdateBatchAsync(table, rowsToAdd);
          //var task = CreateTask(table, rowsToAdd);
          //tasks.Add(task);
        }

        rowOffset += rows.Count;
      }

     // await Task.WhenAll(tasks);

    }


    private async Task UpdateBatchAsync<T>(CloudTable table, List<T> customerEntities) where T : TableEntity
		{
      //var task = Task.Factory.StartNew(() =>
      //{
        var batch = new TableBatchOperation();

        foreach (var row in customerEntities)
        {
          batch.InsertOrReplace(row);
        }

        // submit
        await table.ExecuteBatchAsync(batch);

      //});

      //return task;
    }
		


	}
}
