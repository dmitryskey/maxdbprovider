// Copyright (C) 2005-2006 Dmitry S. Kataev
// Copyright (C) 2004-2005 MySQL AB
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License version 2 as published by
// the Free Software Foundation
//
// Copyright (C) 2005-2006 Dmitry S. Kataev
// There are special exceptions to the terms and conditions of the GPL 
// as it is applied to this software. View the full text of the 
// exception in file EXCEPTIONS in the directory of this software 
// distribution.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

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

		[TestFixtureSetUp]
		public void SetUp() 
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
		//[Test] 
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
