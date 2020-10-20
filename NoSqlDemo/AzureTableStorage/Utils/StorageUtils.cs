using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AzureTableStorage.Utils
{
	public interface IStorageUtils
	{
		CloudStorageAccount CreateStorageAccountFromConnectionString(string storageConnectionString);

		Task<CloudTable> CreateOrGetTableAsync(string connectionString, string tableName);

		Task<T> InsertOrMergeEntityAsync<T>(CloudTable table, T entity) where T : TableEntity;

		Task<T> RetrieveEntityUsingPointQueryAsync<T>(CloudTable table, string partitionKey, string rowKey) where T : TableEntity;

		Task<List<T>> SelectAll<T>(CloudTable table) where T : ITableEntity, new();

		Task<List<T>> SelectAllInPartition<T>(CloudTable table, string partitionKey) where T : ITableEntity, new();

		Task DeleteEntityAsync<T>(CloudTable table, T deleteEntity) where T : TableEntity;
	}


	public class StorageUtils : IStorageUtils
	{
		public CloudStorageAccount CreateStorageAccountFromConnectionString(string storageConnectionString)
		{
			CloudStorageAccount storageAccount;
			try
			{
				storageAccount = CloudStorageAccount.Parse(storageConnectionString);
			}
			catch (FormatException)
			{
				Console.WriteLine("Invalid storage account information provided. Please confirm the AccountName and AccountKey are valid in the app.config file - then restart the application.");
				throw;
			}
			catch (ArgumentException)
			{
				Console.WriteLine("Invalid storage account information provided. Please confirm the AccountName and AccountKey are valid in the app.config file - then restart the sample.");
				Console.ReadLine();
				throw;
			}

			return storageAccount;
		}


		public async Task<CloudTable> CreateOrGetTableAsync(string connectionString, string tableName)
		{
			// Retrieve storage account information from connection string.
			CloudStorageAccount storageAccount = CreateStorageAccountFromConnectionString(connectionString);

			// Create a table client for interacting with the table service
			CloudTableClient tableClient = storageAccount.CreateCloudTableClient(); // NOT COSMOS, so dont pass in "new TableClientConfiguration()"

			Console.WriteLine("Create a Table for the demo");

			// Create a table client for interacting with the table service 
			CloudTable table = tableClient.GetTableReference(tableName);
			if (await table.CreateIfNotExistsAsync())
			{
				Console.WriteLine("Created Table named: {0}", tableName);
			}
			else
			{
				Console.WriteLine("Table {0} already exists", tableName);
			}

			Console.WriteLine();
			return table;
		}



		public async Task<T> InsertOrMergeEntityAsync<T>(CloudTable table, T entity) where T: TableEntity
		{
			if (entity == null)
			{
				throw new ArgumentNullException("entity");
			}

			try
			{
				// Create the InsertOrReplace table operation
				TableOperation insertOrMergeOperation = TableOperation.InsertOrMerge(entity);

				// Execute the operation.
				TableResult result = await table.ExecuteAsync(insertOrMergeOperation);
				T insertedRow = result.Result as T;

				// No charge - table storage, not cosmos.
				//if (result.RequestCharge.HasValue)
				//{
				//	Console.WriteLine("Request Charge of InsertOrMerge Operation: " + result.RequestCharge);
				//}

				return insertedRow;
			}
			catch (StorageException e)
			{
				Console.WriteLine(e.Message);
				Console.ReadLine();
				throw;
			}
		}


		public async Task<T> RetrieveEntityUsingPointQueryAsync<T>(CloudTable table, string partitionKey, string rowKey) where T : TableEntity
		{
			try
			{
				TableOperation retrieveOperation = TableOperation.Retrieve<T>(partitionKey, rowKey);
				TableResult result = await table.ExecuteAsync(retrieveOperation);
				T row = result.Result as T;

				// Not cosmos, so no charge
				//if (result.RequestCharge.HasValue)
				//{
				//	Console.WriteLine("Request Charge of Retrieve Operation: " + result.RequestCharge);
				//}

				return row;
			}
			catch (StorageException e)
			{
				Console.WriteLine(e.Message);
				Console.ReadLine();
				throw;
			}
		}


		public async Task<List<T>> SelectAll<T>(CloudTable table) where T : ITableEntity, new()
		{
			TableContinuationToken token = null;
			var entities = new List<T>();
			do
			{
				// This query can only get 1000 results, so need to send in continuation token to get next (1000).
				var queryResult = await table.ExecuteQuerySegmentedAsync(new TableQuery<T>(), token);
				entities.AddRange(queryResult.Results);
				token = queryResult.ContinuationToken;
			} while (token != null);

			return entities;
		}


		public async Task<List<T>> SelectAllInPartition<T>(CloudTable table, string partitionKey) where T : ITableEntity, new()
		{
			TableContinuationToken token = null;
			var entities = new List<T>();
			do
			{
				var query = new TableQuery<T>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));
				// This query can only get 1000 results, so need to send in continuation token to get next (1000).
				var queryResult = await table.ExecuteQuerySegmentedAsync(query, token);
				entities.AddRange(queryResult.Results);
				token = queryResult.ContinuationToken;
			} while (token != null);

			return entities;
		}


		public async Task DeleteEntityAsync<T>(CloudTable table, T deleteEntity) where T : TableEntity
		{
			try
			{
				if (deleteEntity == null)
				{
					throw new ArgumentNullException("deleteEntity");
				}

				TableOperation deleteOperation = TableOperation.Delete(deleteEntity);
				TableResult result = await table.ExecuteAsync(deleteOperation);

				// Not cosmos so no charge.
				//if (result.RequestCharge.HasValue)
				//{
				//	Console.WriteLine("Request Charge of Delete Operation: " + result.RequestCharge);
				//}

			}
			catch (StorageException e)
			{
				Console.WriteLine(e.Message);
				Console.ReadLine();
				throw;
			}
		}



	}
}
