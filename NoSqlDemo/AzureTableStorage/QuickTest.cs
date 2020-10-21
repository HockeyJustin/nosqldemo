using AzureTableStorage.Utils;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AzureTableStorage
{
	public interface IQuickTest
	{
		Task RunSamples();
		Task RunBatchSampleParllel();
		Task RunBatchSampleAsync();
	}

	public class QuickTest : IQuickTest
	{
		string connectionString = "UseDevelopmentStorage=true";

		IStorageUtils _storageUtils;
		IStorageBatchUtils _storageBatchUtils;

		public QuickTest(IStorageUtils storageUtils,
			IStorageBatchUtils storageBatchUtils)
		{
			_storageUtils = storageUtils;
			_storageBatchUtils = storageBatchUtils;
		}




		public async Task RunBatchSampleParllel()
		{
			try
			{
				string tableName = "CustomerBatchDemoTable2";
				CloudTable table = await _storageUtils.CreateOrGetTableAsync(connectionString, tableName);

				var customerEntities = GetCustomers(100000);

				_storageBatchUtils.InsertBatchParallel<CustomerEntity>(table, customerEntities, 4);
				
				Console.WriteLine("Check the data and see if it's any good. Delete the data? (y/n)");
				if (Console.ReadLine().ToLower().Contains("y"))
				{
					// Delete the table
					await table.DeleteIfExistsAsync();
				}
			}
			catch (Exception ex)
			{
				if (ex is AggregateException)
				{
					foreach (var ae in ((AggregateException)ex).Flatten().InnerExceptions)
					{
						var msg = ae.ToString();
						Console.WriteLine(msg);
					}
				}
				else
				{
					var msg = ex.ToString();
					Console.WriteLine(msg);
				}
			}
		}


		public async Task RunBatchSampleAsync()
		{
			try
			{
				string tableName = "CustomerBatchDemoTable2";
				CloudTable table = await _storageUtils.CreateOrGetTableAsync(connectionString, tableName);

				var customerEntities = GetCustomers(2000);

				await _storageBatchUtils.InsertBatchAsync<CustomerEntity>(table, customerEntities); 

				Console.WriteLine("Check the data and see if it's any good. Delete the data? (y/n)");
				if (Console.ReadLine().ToLower().Contains("y"))
				{
					// Delete the table
					await table.DeleteIfExistsAsync();
				}
			}
			catch (Exception ex)
			{
				if (ex is AggregateException)
				{
					foreach (var ae in ((AggregateException)ex).Flatten().InnerExceptions)
					{
						var msg = ae.ToString();
						Console.WriteLine(msg);
					}
				}
				else
				{
					var msg = ex.ToString();
					Console.WriteLine(msg);
				}
			}
		}




		public async Task RunSamples()
		{
			Console.WriteLine("Azure Table Storage - Basic Samples\n");
			Console.WriteLine("Based on https://docs.microsoft.com/en-us/azure/cosmos-db/tutorial-develop-table-dotnet?toc=https%3A%2F%2Fdocs.microsoft.com%2Fen-us%2Fazure%2Fstorage%2Ftables%2Ftoc.json&bc=https%3A%2F%2Fdocs.microsoft.com%2Fen-us%2Fazure%2Fbread%2Ftoc.json");
			Console.WriteLine("And converted to (cheaper) table storage.");
			Console.WriteLine();

			

			try
			{
				//string tableName = "demo" + Guid.NewGuid().ToString().Substring(0, 5);
				string tableName = "CustomerEntityDemoTable";

				// Create or reference an existing table
				CloudTable table = await _storageUtils.CreateOrGetTableAsync(connectionString, tableName);

				// Demonstrate basic CRUD functionality 
				// Create an instance of a customer entity. See the Model\CustomerEntity.cs for a description of the entity.
				CustomerEntity customer = new CustomerEntity("Harp", "Walter")
				{
					Email = "Walter@contoso.com",
					PhoneNumber = "425-555-0101"
				};

				// Demonstrate how to insert the entity
				Console.WriteLine("Insert an Entity.");
				customer = await _storageUtils.InsertOrMergeEntityAsync(table, customer);
				Console.WriteLine($"Email:{customer.Email}, Phone: {customer.PhoneNumber}");

				// Demonstrate how to Update the entity by changing the phone number
				Console.WriteLine("Update an existing Entity using the InsertOrMerge Upsert Operation.");
				customer.PhoneNumber = "425-555-0105";
				await _storageUtils.InsertOrMergeEntityAsync(table, customer);
				Console.WriteLine();

				// Demonstrate how to Read the updated entity using a point query 
				Console.WriteLine("Reading the updated Entity.");
				customer = await _storageUtils.RetrieveEntityUsingPointQueryAsync<CustomerEntity>(table, "Harp", "Walter");
				Console.WriteLine($"Email:{customer.Email}, Phone: {customer.PhoneNumber}");
				Console.WriteLine();

				// Select All CustomerEntities in the table
				Console.WriteLine("Selecting All Customer Entities.");
				var customers = await _storageUtils.SelectAll<CustomerEntity>(table);
				Console.WriteLine();

				// Demonstrate how to Delete an entity
				Console.WriteLine("Delete the entity. ");
				await _storageUtils.DeleteEntityAsync(table, customer);
				Console.WriteLine();

				// Delete the table
				await table.DeleteIfExistsAsync();
			}
			catch(Exception ex)
			{
				// If "target machine actively refused it (running locally vs local emulator)"
				// run Microsoft Azure Storage Emulator


				Console.WriteLine(ex.ToString());
				Console.ReadLine();
			}
			finally
			{
				
			}
		}


		private List<CustomerEntity> GetCustomers(int numberToCreate)
		{
			Random r = new Random();
			string[] lastNames = new string[] { "Smith", "Smith", "Smith", "Smith",
																					"Smith", "Smith", "Smith", "Smith",
																					"Jones", "Roberts" };

			List<CustomerEntity> customerEntities = new List<CustomerEntity>();
			for (int i = 1; i <= numberToCreate; i++)
			{
				var lIndex = r.Next(0, lastNames.Length - 1);
				var surname = lastNames[lIndex];
				var forename = $"Bert{i.ToString()}";
				var email = $"{forename}@contoso.com";
				var phone = $"KLONDIKE-555-" + i;

				CustomerEntity customer = new CustomerEntity(surname, forename, email, phone);
				customerEntities.Add(customer);
			}

			return customerEntities;
		}


	}


	public class CustomerEntity : TableEntity
	{
		public CustomerEntity()
		{
		}

		public CustomerEntity(string lastName, string firstName)
		{
			PartitionKey = lastName;
			RowKey = firstName;
		}

		public CustomerEntity(string lastName, string firstName, string email, string phoneNo)
			: this(lastName, firstName)
		{
			Email = email;
			PhoneNumber = phoneNo;
		}



		public string Email { get; set; }

		public string PhoneNumber { get; set; }
	}

}
