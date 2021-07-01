//-----------------------------------------------------------------------------------------------
// <copyright file="ConnectionTests.cs" company="Dmitry S. Kataev">
//     Copyright © 2005-2021 Dmitry S. Kataev
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

using FluentAssertions;
using MaxDB.Data;
using NUnit.Framework;
using System;

namespace MaxDB.IntegrationTests
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
        public void TestConnection() => TestConnectionByString(mconnStr);

        [Test]
        public void TestConnectionTimeout()
        {
            using var maxdbconn = new MaxDBConnection(mconnStrBadAddr);
            DateTime start = DateTime.Now;

            Assert.Throws<MaxDBException>(() => maxdbconn.Open())
                .Message.Should().Be("Cannot connect to host 1.1.1.1:7210.");

            DateTime.Now.Subtract(start).TotalSeconds.Should().BeLessOrEqualTo(maxdbconn.ConnectionTimeout + 2, "Timeout exceeded");
        }

        [Test]
        public void TestConnectionBadLogin() =>
            Assert.Throws<MaxDBException>(() => TestConnectionByString(mconnStrBadLogin))
                .Message.Should().Be("Unknown user name/password combination");

        [Test]
        public void TestConnectionBadPassword() =>
            Assert.Throws<MaxDBException>(() => TestConnectionByString(mconnStrBadPassword))
                .Message.Should().Be("Unknown user name/password combination");

        [Test]
        public void TestConnectionBadDbName() =>
            Assert.Throws<MaxDBException>(() => TestConnectionByString(mconnStrBadDbName))
                .Message.Should().Be("Cannot connect to host localhost:7210.");

        [Test]
        public void TestGetSchema()
        {
            using var maxdbconn = new MaxDBConnection(mconnStr);
            maxdbconn.Open();

            var schema = maxdbconn.GetSchema();

            maxdbconn.Close();

            schema.TableName.Should().Be("SchemaTable", "Schema table name");
        }

        private static void TestConnectionByString(string connection)
        {
            using var maxdbconn = new MaxDBConnection(connection);
            maxdbconn.Open();
            maxdbconn.Close();
        }
    }
}
