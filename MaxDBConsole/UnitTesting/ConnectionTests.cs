//-----------------------------------------------------------------------------------------------
// <copyright file="ConnectionTests.cs" company="Dmitry S. Kataev">
//     Copyright © 2005-2018 Dmitry S. Kataev
//     Copyright © 2004-2005 MySQL AB
// </copyright>
//-----------------------------------------------------------------------------------------------
//
//	This program is free software; you can redistribute it and/or
//	modify it under the terms of the GNU General Public License
//	as published by the Free Software Foundation; either version 2
//	of the License, or (at your option) any later version.
//
//	This program is distributed in the hope that it will be useful,
//	but WITHOUT ANY WARRANTY; without even the implied warranty of
//	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//	GNU General Public License for more details.
//
//	You should have received a copy of the GNU General Public License
//	along with this program; if not, write to the Free Software
//	Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

using System;
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
        private string mconnStrBadAddr;
        private string mconnStrBadLogin;
        private string mconnStrBadPassword;
        private string mconnStrBadDbName;

        [SetUp]
        public void SetUp()
        {
            mconnStr = config["ConnectionString"];
            mconnStrBadAddr = config["ConnectionStringBadAddr"];
            mconnStrBadLogin = config["ConnectionStringBadLogin"];
            mconnStrBadPassword = config["ConnectionStringBadPassword"];
            mconnStrBadDbName = config["ConnectionStringBadDbName"];
        }

        [TearDown]
        public void TearDown()
        {
            if (msw != null)
            {
                msw.Close();
            }
        }

        [Test]
        public void TestConnection()
        {
            TestConnectionByString(mconnStr);
        }

        [Test]
        public void TestConnectionTimeout()
        {
            using (var maxdbconn = new MaxDBConnection(mconnStrBadAddr))
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

        [Test]
        public void TestConnectionBadLogin()
        {
            Assert.Throws(typeof(MaxDBException), () => TestConnectionByString(mconnStrBadLogin));
        }

        [Test]
        public void TestConnectionBadPassword()
        {
            Assert.Throws(typeof(MaxDBException), () => TestConnectionByString(mconnStrBadPassword));
        }

        [Test]
        public void TestConnectionBadDbName()
        {
            Assert.Throws(typeof(MaxDBException), () => TestConnectionByString(mconnStrBadDbName));
        }

        [Test]
        public void TestGetSchema()
        {
            using (var maxdbconn = new MaxDBConnection(mconnStr))
            {
                maxdbconn.Open();

                var schema = maxdbconn.GetSchema();

                Assert.AreEqual(schema.TableName, "SchemaTable", "Schema table name");

                maxdbconn.Close();
            }
        }

        private void TestConnectionByString(string connection)
        {
            using (var maxdbconn = new MaxDBConnection(connection))
            {
                maxdbconn.Open();
                maxdbconn.Close();
            }
        }
    }
}
