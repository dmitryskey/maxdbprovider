//-----------------------------------------------------------------------------------------------
// <copyright file="DataReaderTests.cs" company="Dmitry S. Kataev">
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
using System.Data;
using System.Text;

namespace MaxDB.IntegrationTests
{
    /// <summary>
    /// Summary description for DataReaderTests.
    /// </summary>
    [TestFixture]
    public class DataReaderTests : BaseTest
    {
        [SetUp]
        public void SetUp() =>
            Init("CREATE TABLE Test (id INT NOT NULL, name VARCHAR(100), d DATE, t TIME, dt TIMESTAMP, b1 LONG BYTE, c1 LONG ASCII, PRIMARY KEY(id))");

        [TearDown]
        public void TearDown() => Close();

        [Test]
        public void TestResultSet()
        {
            using (var cmd = new MaxDBCommand(string.Empty, mconn))
            {
                // insert 100 records
                cmd.CommandText = "INSERT INTO Test (id, name) VALUES (:id, 'test')";
                cmd.Parameters.Add(new MaxDBParameter(":id", 1));
                for (int i = 1; i <= 100; i++)
                {
                    cmd.Parameters[0].Value = i;
                    cmd.ExecuteNonQuery();
                }
            }

            using (var cmd = new MaxDBCommand("SELECT * FROM Test WHERE id >= 50", mconn))
            {
                // execute it one time
                using (var reader = cmd.ExecuteReader())
                {
                    reader.Should().NotBeNull("First execution - data reader shouldn't be null");
                    while (reader.Read()) ;
                    reader.HasRows.Should().BeTrue("First execution - data reader must has rows");
                    reader.FieldCount.Should().Be(7, "First execution - field count");
                }

                // execute it again
                using (var reader = cmd.ExecuteReader())
                {
                    reader.Should().NotBeNull("Second execution - Data Reader shouldn't be null");
                    while (reader.Read()) ;
                    reader.HasRows.Should().BeTrue("Second execution - Data Reader must has rows");
                    reader.FieldCount.Should().Be(7, "Second execution - field count");
                }
            }
        }

        [Test]
        public void TestNotReadingResultSet()
        {
            for (int x = 0; x < 10; x++)
            {
                using (var cmd = new MaxDBCommand("INSERT INTO Test (id, name, b1) VALUES(:val, 'Test', NULL)", mconn))
                {
                    cmd.Parameters.Add(new MaxDBParameter(":val", x));
                    cmd.ExecuteNonQuery().Should().Be(1, "Affected rows count");
                }

                using (var cmd = new MaxDBCommand("SELECT * FROM Test", mconn))
                {
                    cmd.ExecuteReader().Close();
                }
            }
        }

        [Test]
        public void TestSingleRowBehavior()
        {
            using var cmd = new MaxDBCommand(string.Empty, mconn);
            ExecuteNonQuery("INSERT INTO Test(id, name) VALUES(1, 'test1')");
            ExecuteNonQuery("INSERT INTO Test(id, name) VALUES(2, 'test2')");
            ExecuteNonQuery("INSERT INTO Test(id, name) VALUES(3, 'test3')");

            cmd.CommandText = "SELECT * FROM Test";

            using (var reader = cmd.ExecuteReader(CommandBehavior.SingleRow))
            {
                reader.Read().Should().BeTrue("First read");
                reader.Read().Should().BeFalse("Second read");
                reader.Close();
            }

            cmd.CommandText = "SELECT * FROM test WHERE id = 1";

            using (var reader = cmd.ExecuteReader(CommandBehavior.SingleRow))
            {
                reader.Read().Should().BeTrue();
                reader.GetString(1).Should().Be("test1", "name field for id = 1");
                reader.Read().Should().BeFalse("Data reader should contain only one row");
                reader.Close();
            }

            cmd.CommandText = "SELECT * FROM test WHERE rowno <= 2";

            using (var reader = cmd.ExecuteReader(CommandBehavior.SingleRow))
            {
                reader.Read().Should().BeTrue();
                reader.GetString(1).Should().Be("test1", "name field for id <= 2");
                reader.Read().Should().BeFalse("Data reader should contain only one row");
                reader.Close();
            }
        }

        [Test]
        public void TestCloseConnectionBehavior()
        {
            using var cmd = new MaxDBCommand(String.Empty, mconn);
            ExecuteNonQuery("INSERT INTO Test(id, name) VALUES(1, 'test')");

            cmd.CommandText = "SELECT * FROM Test";

            using (var reader = cmd.ExecuteReader(CommandBehavior.CloseConnection))
            {
                reader.Read().Should().BeTrue("Table should contain at least one row");
                reader.Close();
                mconn.State.Should().Be(ConnectionState.Closed, "Connection should be closed");
            }

            mconn.Open();
        }

        [Test]
        public void TestSchemaOnlyBehavior()
        {
            using var cmd = new MaxDBCommand(string.Empty, mconn);
            ExecuteNonQuery("INSERT INTO Test(id, name) VALUES(1, 'test1')");
            ExecuteNonQuery("INSERT INTO Test(id, name) VALUES(2, 'test2')");
            ExecuteNonQuery("INSERT INTO Test(id, name) VALUES(3, 'test3')");

            cmd.CommandText = "SELECT * FROM Test FOR UPDATE";

            using var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly);
            var table = reader.GetSchemaTable();
            table.Rows.Count.Should().Be(7, "Table rows count");
            table.Columns.Count.Should().Be(16, "Table columns count");
            reader.Read().Should().BeFalse("Table should be empty");
        }

        [Test]
        public void TestDBNulls()
        {
            using var cmd = new MaxDBCommand(string.Empty, mconn);
            ExecuteNonQuery("INSERT INTO Test(id, name) VALUES(1, 'Test')");

            cmd.CommandText = "INSERT INTO Test(id, name) VALUES(2, :null)";
            cmd.Parameters.Add(":null", MaxDBType.VarCharA).Value = DBNull.Value;
            cmd.ExecuteNonQuery();
            ExecuteNonQuery("INSERT INTO Test(id, name) VALUES(3, 'Test2')");

            cmd.CommandText = "SELECT * FROM Test";

            using var reader = cmd.ExecuteReader();
            reader.Read().Should().BeTrue();
            reader.GetValue(0).Should().Be(1, "id field of the first row as object");
            reader.GetInt32(0).Should().Be(1, "id field of the first row as integer");
            reader.GetValue(1).Should().Be("Test".ToString(), "name field of the first row as object");
            reader.GetString(1).Should().Be("Test", "name field of the first row as string");
            reader.Read().Should().BeTrue();
            reader.GetValue(0).Should().Be(2, "id field of the second row as object");
            reader.GetInt32(0).Should().Be(2, "id field of the second row as integer");
            reader.GetValue(1).Should().Be(DBNull.Value, "name field of the second row as object");
            reader.GetString(1).Should().BeNull("name field of the second row as string");
            reader.Read().Should().BeTrue();
            reader.GetValue(0).Should().Be(3, "id field of the third row as object");
            reader.GetInt32(0).Should().Be(3, "id field of the third row as string");
            reader.GetValue(1).Should().Be("Test2", "name field of the third row as object");
            reader.GetString(1).Should().Be("Test2", "name field of the third row as string");
            reader.Read().Should().BeFalse();
        }

        [Test]
        public void TestGetByte()
        {
            ExecuteNonQuery("INSERT INTO Test (id, name) VALUES (123, 'a')");

            using var cmd = new MaxDBCommand("SELECT * FROM Test", mconn);
            using var reader = cmd.ExecuteReader();
            reader.Read().Should().BeTrue(); ;
            reader.GetByte(0).Should().Be(123, "id field of the first row as byte");
            reader.GetByte(1).Should().Be(97, "name field of the first row as byte");
        }

        [Test]
        public void TestGetBytes()
        {
            const int len = 50000;
            byte[] bytes = CreateBlob(len);

            using var cmd = new MaxDBCommand("INSERT INTO Test (id, name, b1) VALUES(1, :t, :b1)", mconn);
            cmd.Parameters.Add(":t", "Test");
            cmd.Parameters.Add(":b1", bytes);
            cmd.ExecuteNonQuery();

            cmd.CommandText = "SELECT * FROM Test";

            // now check with sequential access
            using var reader = cmd.ExecuteReader();
            reader.Read().Should().BeTrue("data reader shouldn't be empty");
            int mylen = len;
            int startIndex = 0;
            byte[] buff = new byte[8192];
            while (mylen > 0)
            {
                int readLen = Math.Min(mylen, buff.Length);
                readLen.Should().Be((int)reader.GetBytes(5, startIndex, buff, 0, readLen), "wrong length of the chunk");
                for (int i = 0; i < readLen; i++)
                {
                    bytes[startIndex + i].Should().Be(buff[i], $"wrong value at position {i} of the chunk");
                }

                startIndex += readLen;
                mylen -= readLen;
            }
        }

        [Test]
        public void TestGetChar()
        {
            ExecuteNonQuery("INSERT INTO Test (id, name) VALUES (1, 'a')");

            using var cmd = new MaxDBCommand("SELECT * FROM Test", mconn);
            using var reader = cmd.ExecuteReader();
            reader.Read().Should().BeTrue();
            'a'.Should().Be(reader.GetChar(1), "name field of the first row as char");
        }

        [Test]
        public void TestGetChars()
        {
            int len = 50000;

            char[] chars = new char[len];
            byte[] bytes = new byte[len];
            for (int i = 0; i < len; i++)
            {
                bytes[i] = (byte)(i % 127 + 1); // we can not use null
            }

            Encoding.ASCII.GetChars(bytes, 0, len, chars, 0);

            using var cmd = new MaxDBCommand("INSERT INTO Test (id, name, c1) VALUES(1, :t, :c1)", mconn);
            cmd.Parameters.Add(":t", "Test");
            cmd.Parameters.Add(":c1", MaxDBType.LongA).Value = chars;
            cmd.ExecuteNonQuery();

            cmd.CommandText = "SELECT * FROM Test";

            // now check with sequential access
            using var reader = cmd.ExecuteReader();
            reader.Read().Should().BeTrue();
            int mylen = len;
            int startIndex = 0;
            char[] buff = new char[8192];

            while (mylen > 0)
            {
                int readLen = Math.Min(mylen, buff.Length);
                readLen.Should().Be((int)reader.GetChars(6, startIndex, buff, 0, readLen), "check length of the chunk");
                for (int i = 0; i < readLen; i++)
                {
                    chars[startIndex + i].Should().Be(buff[i], $"check value at position {i} of the chunk");
                }

                startIndex += readLen;
                mylen -= readLen;
            }
        }

        [Test]
        public void TestTextFields()
        {
            ExecuteNonQuery("INSERT INTO Test (id, name) VALUES (1, 'Text value')");
            ExecuteNonQuery("INSERT INTO Test (id, name) VALUES (2, '123.456')");

            using var cmd = new MaxDBCommand("SELECT * FROM Test", mconn);
            using var reader = cmd.ExecuteReader();
            reader.Read().Should().BeTrue();
            reader["name"].Should().Be("Text value", "wrong name field of the first row a string");
            reader.Read().Should().BeTrue();
            reader.GetDouble(1).Should().Be(123.456, "wrong name field of the first row a double");
        }

        [Test]
        public void TestDateAndTimeFields()
        {
            var dt = DateTime.Now;

            using (var cmd = new MaxDBCommand("INSERT INTO Test (id, d, t, dt) VALUES (1, :a, :b, :c)", mconn))
            {
                cmd.Parameters.Add(new MaxDBParameter(":a", MaxDBType.Date)).Value = dt.Date;
                cmd.Parameters.Add(new MaxDBParameter(":b", MaxDBType.Time)).Value = dt.TimeOfDay;
                cmd.Parameters.Add(new MaxDBParameter(":c", MaxDBType.Timestamp)).Value = dt;
                cmd.ExecuteNonQuery();
            }

            using (var cmd = new MaxDBCommand("SELECT * FROM Test", mconn))
            {
                using var reader = cmd.ExecuteReader();
                reader.Read().Should().BeTrue();

                long period = TimeSpan.TicksPerMillisecond / 1000;

                reader["d"].Should().Be(dt.Date, "wrong date");
                (((DateTime)reader["t"]).Ticks / TimeSpan.TicksPerSecond).Should().Be(dt.TimeOfDay.Ticks / TimeSpan.TicksPerSecond, "wrong time of day");
                (((DateTime)reader["dt"]).Ticks / period).Should().Be(dt.Ticks / period, "wrong time stamp value");
            }
        }

        [Test]
        public void TestReadingBeforeRead()
        {
            Assert.Throws<MaxDBException>(() =>
            {
                using var cmd = new MaxDBCommand("SELECT * FROM Test", mconn);
                using MaxDBDataReader reader = cmd.ExecuteReader();
                reader.GetInt32(0);
            }).Message.Should().Be("Result set is positioned before first row.");
        }
    }
}
