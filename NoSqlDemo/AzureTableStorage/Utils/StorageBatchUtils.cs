using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureTableStorage.Utils
{
  public interface IStorageBatchUtils
	{
    Task InsertBatch<T>(CloudTable table, List<T> customerEntities) where T : TableEntity;

  }


  public class StorageBatchUtils : IStorageBatchUtils
	{

		public async Task InsertBatch<T>(CloudTable table, List<T> entities) where T : TableEntity
		{
      int rowOffset = 0;

      var tasks = new List<Task>();

      while (rowOffset < entities.Count)
      {
        // next batch
        var rows = entities.Skip(rowOffset).Take(100).ToList();

        var partitionKeys = rows.Select(_ => _.PartitionKey).Distinct();

        foreach (var pkRow in partitionKeys)
        {
          var rowsToAdd = rows.Where(_ => _.PartitionKey == pkRow).ToList();
          var task = CreateTask(table, rowsToAdd);
          tasks.Add(task);
        }

        rowOffset += rows.Count;
      }

      await Task.WhenAll(tasks);

    }


    private Task CreateTask<T>(CloudTable table, List<T> customerEntities) where T : TableEntity
		{
      var task = Task.Factory.StartNew(() =>
      {
        var batch = new TableBatchOperation();

        foreach (var row in customerEntities)
        {
          batch.InsertOrReplace(row);
        }

        // submit
        table.ExecuteBatchAsync(batch);

      });

      return task;
    }
		


	}
}
