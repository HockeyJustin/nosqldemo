using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AzureTableStorage
{

	public interface IMain
	{
		Task Run();
	}

	public class Main : IMain
	{
		IQuickTest _quickTest;

		public Main(IQuickTest quickTest)
		{
			_quickTest = quickTest;
		}


		public async Task Run()
		{
			await _quickTest.RunBatchSampleAsync();
			//await _quickTest.RunBatchSampleParllel();
			//await _quickTest.RunSamples();
		}
	}
}
