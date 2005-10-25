using System;
using NUnit.Framework;
using MaxDBDataProvider;

namespace MaxDBDataProvider.Tests
{
	/// <summary>
	/// Summary description for TestMaxDB.
	/// </summary>
	[TestFixture] 
	public class TestMaxDB
	{
		public TestMaxDB()
		{
			//
			// TODO: Add constructor logic here
			//
		}

		[SetUp] 
		public void Init() 
		{

		}

		[Test] 
		public void TestConnection()
		{
			MaxDBConnection maxdbconn = new MaxDBConnection(System.Configuration.ConfigurationSettings.AppSettings["ConnectionString"]);
				
			maxdbconn.Open();
			maxdbconn.Close();
		}
	}
}
