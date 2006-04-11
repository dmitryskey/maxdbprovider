using System;
using NUnit.Framework;
using MaxDBDataProvider;

namespace MaxDBConsole.UnitTesting
{
	/// <summary>
	/// Summary description for ConnectionTests.
	/// </summary>
	[TestFixture()]
	public class ConnectionTests
	{
		string m_connStr;
#if SAFE
		string m_connStrBadAddr;
#endif
		string m_connStrBadLogin;
		string m_connStrBadPassword;
		string m_connStrBadDbName;

		public ConnectionTests()
		{
			//
			// TODO: Add constructor logic here
			//
		}

		[TestFixtureSetUp]
		public void Init() 
		{
			m_connStr = System.Configuration.ConfigurationSettings.AppSettings["ConnectionString"];
#if SAFE
			m_connStrBadAddr = System.Configuration.ConfigurationSettings.AppSettings["ConnectionStringBadAddr"];
#endif
			m_connStrBadLogin = System.Configuration.ConfigurationSettings.AppSettings["ConnectionStringBadAddr"];
			m_connStrBadPassword = System.Configuration.ConfigurationSettings.AppSettings["ConnectionStringBadPassword"];
			m_connStrBadDbName = System.Configuration.ConfigurationSettings.AppSettings["ConnectionStringBadDbName"];
		}

		[Test] 
		public void TestConnection()
		{
			MaxDBConnection maxdbconn = new MaxDBConnection(m_connStr);
				
			maxdbconn.Open();
			maxdbconn.Close();
		}

#if SAFE
		[Test] 
		public void TestConnectionTimeout()
		{
			MaxDBConnection maxdbconn = new MaxDBConnection(m_connStrBadAddr);
			
			DateTime start = DateTime.Now;

			try
			{
				maxdbconn.Open();
			}
			catch(MaxDBException)
			{
				Assert.IsTrue(DateTime.Now.Subtract(start).TotalSeconds <= maxdbconn.ConnectionTimeout + 2, "Timeout exceeded");
			}
		}
#endif

		[Test]
		[ExpectedException(typeof(MaxDBException))]
		public void TestConnectionBadLogin()
		{
			MaxDBConnection maxdbconn = new MaxDBConnection(m_connStrBadLogin);
				
			maxdbconn.Open();
			maxdbconn.Close();
		}

		[Test]
		[ExpectedException(typeof(MaxDBException))]
		public void TestConnectionBadPassword()
		{
			MaxDBConnection maxdbconn = new MaxDBConnection(m_connStrBadPassword);
				
			maxdbconn.Open();
			maxdbconn.Close();
		}

		[Test]
		[ExpectedException(typeof(MaxDBException))]
		public void TestConnectionBadDbName()
		{
			MaxDBConnection maxdbconn = new MaxDBConnection(m_connStrBadDbName);
				
			maxdbconn.Open();
			maxdbconn.Close();
		}
	}
}
