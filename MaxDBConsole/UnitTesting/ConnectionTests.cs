using System;
using System.Collections.Specialized;
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
		private string m_connStr;
#if SAFE
		private string m_connStrBadAddr;
#endif
		private string m_connStrBadLogin;
		private string m_connStrBadPassword;
		private string m_connStrBadDbName;
        private NameValueCollection m_AppSettings =
#if NET20
            System.Configuration.ConfigurationManager.AppSettings;
#else
            System.Configuration.ConfigurationSettings.AppSettings;
#endif

		public ConnectionTests()
		{
			//
			// TODO: Add constructor logic here
			//
		}

		[TestFixtureSetUp]
		public void Init() 
		{
			m_connStr = m_AppSettings["ConnectionString"];
#if SAFE
			m_connStrBadAddr = m_AppSettings["ConnectionStringBadAddr"];
#endif
			m_connStrBadLogin = m_AppSettings["ConnectionStringBadAddr"];
			m_connStrBadPassword = m_AppSettings["ConnectionStringBadPassword"];
			m_connStrBadDbName = m_AppSettings["ConnectionStringBadDbName"];
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
