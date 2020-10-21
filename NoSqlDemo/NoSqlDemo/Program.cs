using AzureTableStorage;
using AzureTableStorage.Utils;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Threading.Tasks;

namespace NoSqlDemo
{
  class Program
  {
    /// <summary>
    /// DI set up in line with 
    /// https://medium.com/swlh/how-to-take-advantage-of-dependency-injection-in-net-core-2-2-console-applications-274e50a6c350
    /// </summary>
    /// <param name="args"></param>
    /// <returns></returns>
    static async Task Main(string[] args)
    {

      Console.WriteLine("Starting");

      // Create service collection and configure our services
      var services = ConfigureServices();    // Generate a provider
      var serviceProvider = services.BuildServiceProvider();

      // Kick off our actual code
      await serviceProvider.GetService<ConsoleApplication>().Run();

      Console.WriteLine("Ending");
    }

    public class ConsoleApplication
    {
      private readonly IMain _atsMain;
      public ConsoleApplication(IMain coreMain)
      {
        _atsMain = coreMain;
      }

      // Application starting point
      public async Task Run()
      {
        await _atsMain.Run();
      }
    }


    private static IServiceCollection ConfigureServices()
    {
      IServiceCollection services = new ServiceCollection();

      services.AddTransient<IMain, Main>();
      services.AddTransient<IStorageUtils, StorageUtils>();
      services.AddTransient<IStorageBatchUtils, StorageBatchUtils>();
      services.AddTransient<IQuickTest, QuickTest>();

      services.AddTransient<ConsoleApplication>(); // IMPORTANT! Register our application entry point
      return services;
    }




  }
}
