//-----------------------------------------------------------------------------------------------
// <copyright file="StressTests.cs" company="Dmitry S. Kataev">
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

using System;
using System.Data;
using System.Linq;
using System.Security.Cryptography;
using FluentAssertions;
using MaxDB.Data;
using NUnit.Framework;

namespace MaxDB.IntegrationTests
{
    [TestFixture]
    public class StressTests : BaseTest
    {
        [SetUp]
        public void SetUp() =>
            Init("CREATE TABLE Test (id INT NOT NULL, name varchar(100), blob1 LONG BYTE, text1 LONG ASCII, PRIMARY KEY(id))");

        [TearDown]
        public void TearDown() => Close();

        [Test]
        public void TestMultiPacket()
        {
            const int len = 20000000;

            byte[] dataIn = CreateBlob(len);
            byte[] dataIn2 = CreateBlob(len);

            using var cmd = new MaxDBCommand("INSERT INTO Test VALUES (:id, NULL, :blob, NULL)", mconn);
            cmd.Parameters.Add(new MaxDBParameter(":id", 1));
            cmd.Parameters.Add(new MaxDBParameter(":blob", dataIn));
            cmd.ExecuteNonQuery();

            cmd.Parameters[0].Value = 2;
            cmd.Parameters[1].Value = dataIn2;
            cmd.ExecuteNonQuery();

            var sha = new SHA1CryptoServiceProvider();

            cmd.CommandText = "SELECT blob1 FROM Test";

            using var reader = cmd.ExecuteReader();
            reader.Read().Should().BeTrue();
            byte[] dataOut = new byte[len];
            reader.GetBytes(0, 0, dataOut, 0, len).Should().Be(len);
            sha.ComputeHash(dataIn).Should().BeEquivalentTo(sha.ComputeHash(dataOut));

            reader.Read().Should().BeTrue();
            reader.GetBytes(0, 0, dataOut, 0, len).Should().Be(len);

            byte[] hashIn = sha.ComputeHash(dataIn2);
            byte[] hashOut = sha.ComputeHash(dataOut);

            if (!hashIn.SequenceEqual(hashOut))
            {
                // LINQ generates Out-Of-Memory;
                for (int i = 0; i < len; i++)
                {
                    dataIn2[i].Should().Be(dataOut[i], $"wrong blob value at position {i}");
                }
            }
        }

        [Test]
        public void TestSequence()
        {
            const int count = 8000;
            int[] id2_values = new int[count];
            int[] id_values = new int[count];

            using (var da = new MaxDBDataAdapter())
            {
                da.InsertCommand = new MaxDBCommand("INSERT INTO Test (id, name) VALUES (:id, 'test')", mconn);
                da.InsertCommand.Parameters.Add(new MaxDBParameter(":id", MaxDBType.Integer, "id"));

                var dt = new DataTable();
                dt.Columns.Add(new DataColumn("id", typeof(int)));

                for (int i = 1; i <= count; i++)
                {
                    var row = dt.NewRow();
                    row["id"] = i;
                    dt.Rows.Add(row);
                }

                da.Update(dt);
            }

            using var cmd = new MaxDBCommand("SELECT * FROM Test", mconn);
            int i2 = 0;

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                id_values[i2] = i2 + 1;
                id2_values[i2++] = reader.GetInt32(0);
            }

            count.Should().Be(i2, "Sequence count");

            if (!id_values.SequenceEqual(id2_values))
            {
                for (int i = 0; i < id_values.Length; i++)
                {
                    id_values[i].Should().Be(id2_values[i], $"wrong sequence value at position {i}");
                }
            }
        }
    }
}
