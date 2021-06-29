//-----------------------------------------------------------------------------------------------
// <copyright file="CommandBuilderTests.cs" company="Dmitry S. Kataev">
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

using System.Data;
using FluentAssertions;
using MaxDB.Data;
using NUnit.Framework;

namespace MaxDB.IntegrationTests
{
    [TestFixture]
    public class CommandBuilderTests : BaseTest
    {
        [SetUp]
        public void SetUp() =>
            Init("CREATE TABLE Test (id INT NOT NULL DEFAULT SERIAL, id2 INT NOT NULL UNIQUE, name VARCHAR(100), tm TIME, PRIMARY KEY(id, id2))");

        [TearDown]
        public void TearDown() => Close();

        [Test]
        public void GetInsertCommandTest()
        {
            using var da = new MaxDBDataAdapter("SELECT id, id2, name, tm FROM Test FOR UPDATE", mconn);
            using var cb = new MaxDBCommandBuilder(da);
            var ds = new DataSet();
            da.Fill(ds);

            ds.Tables.Count.Should().Be(1, "At least one table should be filled");

            // serial column is skipped
            cb.GetInsertCommand().CommandText.Should().BeEquivalentTo(
                "INSERT INTO Test(id2, name, tm) VALUES(:id2, :name, :tm)",
                "GetInsertCommand method returns wrong SQL");
        }

        [Test]
        public void CommandParameterTest()
        {
            using var da = new MaxDBDataAdapter("SELECT id, id2, name, tm FROM Test FOR UPDATE", mconn);
            using var cb = new MaxDBCommandBuilder(da);

            var ds = new DataSet();
            da.Fill(ds);
            ds.Tables.Count.Should().Be(1, "At least one table should be filled");

            var collect = cb.GetInsertCommand().Parameters;

            // serial column is skipped
            collect.Count.Should().Be(3, "GetInsertCommand method returns command with wrong number of parameters");

            collect[0].ParameterName.Should().BeEquivalentTo("id2", "First parameter");
            collect[0].DbType.Should().Be(DbType.Int32, "Wrong type of the first parameter");

            collect[1].ParameterName.Should().BeEquivalentTo("name", "Second parameter");
            collect[1].DbType.Should().Be(DbType.AnsiString, "Wrong type of the second parameter");

            collect[2].ParameterName.Should().BeEquivalentTo("tm", "Third parameter");
            collect[2].DbType.Should().Be(DbType.Time, "Wrong type of the third parameter");
        }

        [Test]
        public void GetUpdateCommandTest()
        {
            using var da = new MaxDBDataAdapter("SELECT id, id2, name, tm FROM Test FOR UPDATE", mconn);
            using var cb = new MaxDBCommandBuilder(da);
            var ds = new DataSet();
            da.Fill(ds);
            ds.Tables.Count.Should().Be(1, "At least one table should be filled");

            cb.GetUpdateCommand().CommandText.Should().BeEquivalentTo(
                "UPDATE Test SET name = :name, tm = :tm WHERE id = :id AND id2 = :id2",
                "GetUpdateCommand method returns wrong SQL");
        }

        [Test]
        public void GetDeleteCommandTest()
        {
            using var da = new MaxDBDataAdapter("SELECT id, id2, name, tm FROM Test FOR UPDATE", mconn);
            using var cb = new MaxDBCommandBuilder(da);
            var ds = new DataSet();
            da.Fill(ds);
            ds.Tables.Count.Should().Be(1, "At least one table should be filled");
            cb.GetDeleteCommand().CommandText.Should().BeEquivalentTo(
                "DELETE FROM Test WHERE id = :id AND id2 = :id2",
                "GetDeleteCommand method returns wrong SQL");
        }
    }
}
