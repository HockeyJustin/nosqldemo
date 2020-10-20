using AzureTableStorage.Utils;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AzureTableStorage
{
	public interface IQuickTest
	{
		Task RunSamples();
	}

	public class QuickTest : IQuickTest
	{
		string connectionString = "UseDevelopmentStorage=true";

		IStorageUtils _storageUtils;

		public QuickTest(IStorageUtils storageUtils)
		{
			_storageUtils = storageUtils;
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

				// Demonstrate how to Update the entity by changing the phone number
				Console.WriteLine("Update an existing Entity using the InsertOrMerge Upsert Operation.");
				customer.PhoneNumber = "425-555-0105";
				await _storageUtils.InsertOrMergeEntityAsync(table, customer);
				Console.WriteLine();

				// Demonstrate how to Read the updated entity using a point query 
				Console.WriteLine("Reading the updated Entity.");
				customer = await _storageUtils.RetrieveEntityUsingPointQueryAsync(table, "Harp", "Walter");
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

		public string Email { get; set; }

		public string PhoneNumber { get; set; }
	}

}
