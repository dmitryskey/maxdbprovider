using System;
using System.Collections.Specialized;
using NUnit.Framework;
using MaxDB.Data;

namespace MaxDB.UnitTesting
{
	/// <summary>
	/// Summary description for ConnectionTests.
	/// </summary>
	[TestFixture()]
	public class ConnectionTests
	{
		private string mconnStr;
#if SAFE
		private string mconnStrBadAddr;
#endif // SAFE
        private string mconnStrBadLogin;
		private string mconnStrBadPassword;
		private string mconnStrBadDbName;
        private NameValueCollection mAppSettings =
#if NET20
            System.Configuration.ConfigurationManager.AppSettings;
#else
            System.Configuration.ConfigurationSettings.AppSettings;
#endif // NET20

        public ConnectionTests()
		{
			//
			// TODO: Add constructor logic here
			//
		}

		[TestFixtureSetUp]
		public void Init() 
		{
			mconnStr = mAppSettings["ConnectionString"];
#if SAFE
			mconnStrBadAddr = mAppSettings["ConnectionStringBadAddr"];
#endif // SAFE
            mconnStrBadLogin = mAppSettings["ConnectionStringBadLogin"];
			mconnStrBadPassword = mAppSettings["ConnectionStringBadPassword"];
			mconnStrBadDbName = mAppSettings["ConnectionStringBadDbName"];
		}

		[Test] 
		public void TestConnection()
		{
            TestConnectionByString(mconnStr);
		}

#if SAFE
		[Test] 
		public void TestConnectionTimeout()
		{
            using(MaxDBConnection maxdbconn = new MaxDBConnection(mconnStrBadAddr))
            {
                DateTime start = DateTime.Now;

                try
                {
                    maxdbconn.Open();
                }
                catch (MaxDBException)
                {
                    Assert.IsTrue(DateTime.Now.Subtract(start).TotalSeconds <= maxdbconn.ConnectionTimeout + 2, "Timeout exceeded");
                }
            }
        }
#endif // SAFE

        [Test]
		[ExpectedException(typeof(MaxDBException))]
		public void TestConnectionBadLogin()
		{
            TestConnectionByString(mconnStrBadLogin);
		}

		[Test]
		[ExpectedException(typeof(MaxDBException))]
		public void TestConnectionBadPassword()
		{
            TestConnectionByString(mconnStrBadPassword);
		}

		[Test]
		[ExpectedException(typeof(MaxDBException))]
		public void TestConnectionBadDbName()
		{
            TestConnectionByString(mconnStrBadDbName);
		}

        private void TestConnectionByString(string connection)
        {
            using (MaxDBConnection maxdbconn = new MaxDBConnection(connection))
            {
                maxdbconn.Open();
                maxdbconn.Close();
            }
        }
	}
}
