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
	public class ConnectionTests : BaseTest
	{
		private string mconnStr;
#if SAFE
		private string mconnStrBadAddr;
#endif // SAFE
#if NET20 && !MONO
        private string mconnStrSsl;
#endif // NET20 && !MONO
        private string mconnStrBadLogin;
		private string mconnStrBadPassword;
		private string mconnStrBadDbName;

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
#if NET20 && !MONO
            mconnStrSsl = mAppSettings["ConnectionStringSsl"];
#endif // NET20 && !MONO
            mconnStrBadLogin = mAppSettings["ConnectionStringBadLogin"];
			mconnStrBadPassword = mAppSettings["ConnectionStringBadPassword"];
			mconnStrBadDbName = mAppSettings["ConnectionStringBadDbName"];
		}

		[Test] 
		public void TestConnection()
		{
            TestConnectionByString(mconnStr);
		}

#if NET20 && !MONO
		[Test] 
		public void TestConnectionSsl()
		{
            TestConnectionByString(mconnStrSsl);
        }
#endif // NET20 && !MONO

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
