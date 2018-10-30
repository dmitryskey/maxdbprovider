//-----------------------------------------------------------------------------------------------
// <copyright file="DataReaderTests.cs" company="Dmitry S. Kataev">
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
using System.Data;

namespace MaxDB.UnitTesting
{
    /// <summary>
    /// Summary description for DataReaderTests.
    /// </summary>
    [TestFixture]
    public class DataReaderTests : BaseTest
    {
        [SetUp]
        public void SetUp()
        {
            Init("CREATE TABLE Test (id INT NOT NULL, name VARCHAR(100), d DATE, t TIME, dt TIMESTAMP, b1 LONG BYTE, c1 LONG ASCII, PRIMARY KEY(id))");
        }

        [TearDown]
        public void TearDown()
        {
            Close();
        }

        [Test]
        public void TestResultSet()
        {
            try
            {
                ClearTestTable();

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
                        Assert.IsNotNull(reader, "First execution - data reader shouldn't be null");
                        while (reader.Read()) ;
                        Assert.IsTrue(reader.HasRows, "First execution - data reader must has rows");
                        Assert.AreEqual(7, reader.FieldCount, "First execution - field count");
                    }

                    // execute it again
                    using (var reader = cmd.ExecuteReader())
                    {
                        Assert.IsNotNull(reader, "Second execution - Data Reader shouldn't be null");
                        while (reader.Read());
                        Assert.IsTrue(reader.HasRows, "Second execution - Data Reader must has rows");
                        Assert.AreEqual(7, reader.FieldCount, "Second execution - field count");
                    }
                }
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        [Test]
        public void TestNotReadingResultSet()
        {
            try
            {
                ClearTestTable();

                for (int x = 0; x < 10; x++)
                {
                    using (var cmd = new MaxDBCommand("INSERT INTO Test (id, name, b1) VALUES(:val, 'Test', NULL)", mconn))
                    {
                        cmd.Parameters.Add(new MaxDBParameter(":val", x));
                        int affected = cmd.ExecuteNonQuery();
                        Assert.AreEqual(1, affected, "Affected rows count");
                    }

                    using (var cmd = new MaxDBCommand("SELECT * FROM Test", mconn))
                    {
                        cmd.ExecuteReader().Close();
                    }
                }
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        [Test]
        public void TestSingleRowBehavior()
        {
            try
            {
                ClearTestTable();

                using (var cmd = new MaxDBCommand(string.Empty, mconn))
                {
                    ExecuteNonQuery("INSERT INTO Test(id, name) VALUES(1, 'test1')");
                    ExecuteNonQuery("INSERT INTO Test(id, name) VALUES(2, 'test2')");
                    ExecuteNonQuery("INSERT INTO Test(id, name) VALUES(3, 'test3')");

                    cmd.CommandText = "SELECT * FROM Test";

                    using (var reader = cmd.ExecuteReader(CommandBehavior.SingleRow))
                    {
                        Assert.IsTrue(reader.Read(), "First read");
                        Assert.IsFalse(reader.Read(), "Second read");
                        reader.Close();
                    }

                    cmd.CommandText = "SELECT * FROM test WHERE id = 1";

                    using (var reader = cmd.ExecuteReader(CommandBehavior.SingleRow))
                    {
                        Assert.IsTrue(reader.Read());
                        Assert.AreEqual("test1", reader.GetString(1), "name field for id = 1");
                        Assert.IsFalse(reader.Read(), "Data reader should contain only one row");
                        reader.Close();
                    }

                    cmd.CommandText = "SELECT * FROM test WHERE rowno <= 2";

                    using (var reader = cmd.ExecuteReader(CommandBehavior.SingleRow))
                    {
                        Assert.IsTrue(reader.Read());
                        Assert.AreEqual("test1", reader.GetString(1), "name field for id <= 2");
                        Assert.IsFalse(reader.Read(), "Data reader should contain only one row");
                        reader.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        [Test]
        public void TestCloseConnectionBehavior()
        {
            try
            {
                ClearTestTable();

                using (var cmd = new MaxDBCommand(String.Empty, mconn))
                {
                    ExecuteNonQuery("INSERT INTO Test(id, name) VALUES(1, 'test')");

                    cmd.CommandText = "SELECT * FROM Test";

                    using (var reader = cmd.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        Assert.IsTrue(reader.Read(), "Table should contain at least one row");
                        reader.Close();
                        Assert.IsTrue(mconn.State == ConnectionState.Closed, "Connection should be closed");
                    }

                    mconn.Open();
                }
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        [Test]
        public void TestSchemaOnlyBehavior()
        {
            try
            {
                ClearTestTable();

                using (var cmd = new MaxDBCommand(string.Empty, mconn))
                {
                    ExecuteNonQuery("INSERT INTO Test(id, name) VALUES(1, 'test1')");
                    ExecuteNonQuery("INSERT INTO Test(id, name) VALUES(2, 'test2')");
                    ExecuteNonQuery("INSERT INTO Test(id, name) VALUES(3, 'test3')");

                    cmd.CommandText = "SELECT * FROM Test FOR UPDATE";

                    using (var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly))
                    {
                        DataTable table = reader.GetSchemaTable();
                        Assert.AreEqual(7, table.Rows.Count, "Table rows count");
                        Assert.AreEqual(16, table.Columns.Count, "Table columns count");
                        Assert.IsFalse(reader.Read(), "Table should be empty");
                    }
                }
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        [Test]
        public void TestDBNulls()
        {
            try
            {
                ClearTestTable();

                using (var cmd = new MaxDBCommand(string.Empty, mconn))
                {
                    ExecuteNonQuery("INSERT INTO Test(id, name) VALUES(1, 'Test')");

                    cmd.CommandText = "INSERT INTO Test(id, name) VALUES(2, :null)";
                    cmd.Parameters.Add(":null", MaxDBType.VarCharA).Value = DBNull.Value;
                    cmd.ExecuteNonQuery();
                    ExecuteNonQuery("INSERT INTO Test(id, name) VALUES(3, 'Test2')");

                    cmd.CommandText = "SELECT * FROM Test";

                    using (var reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.AreEqual(1, reader.GetValue(0), "id field of the first row as object");
                        Assert.AreEqual(1, reader.GetInt32(0), "id field of the first row as integer");
                        Assert.AreEqual("Test", reader.GetValue(1).ToString(), "name field of the first row as object");
                        Assert.AreEqual("Test", reader.GetString(1), "name field of the first row as string");
                        reader.Read();
                        Assert.AreEqual(2, reader.GetValue(0), "id field of the second row as object");
                        Assert.AreEqual(2, reader.GetInt32(0), "id field of the second row as integer");
                        Assert.AreEqual(DBNull.Value, reader.GetValue(1), "name field of the second row as object");
                        Assert.AreEqual(null, reader.GetString(1), "name field of the second row as string");
                        reader.Read();
                        Assert.AreEqual(3, reader.GetValue(0), "id field of the third row as object");
                        Assert.AreEqual(3, reader.GetInt32(0), "id field of the third row as string");
                        Assert.AreEqual("Test2", reader.GetValue(1).ToString(), "name field of the third row as object");
                        Assert.AreEqual("Test2", reader.GetString(1), "name field of the third row as string");
                        Assert.IsFalse(reader.Read());
                    }
                }
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        [Test]
        public void TestGetByte()
        {
            try
            {
                ClearTestTable();

                ExecuteNonQuery("INSERT INTO Test (id, name) VALUES (123, 'a')");

                using (var cmd = new MaxDBCommand("SELECT * FROM Test", mconn))
                {
                    using (var reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.AreEqual(123, reader.GetByte(0), "id field of the first row as byte");
                        Assert.AreEqual(97, reader.GetByte(1), "name field of the first row as byte");
                    }
                }
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        [Test]
        public void TestGetBytes()
        {
            const int len = 50000;
            byte[] bytes = CreateBlob(len);

            try
            {
                ClearTestTable();

                using (var cmd = new MaxDBCommand("INSERT INTO Test (id, name, b1) VALUES(1, :t, :b1)", mconn))
                {
                    cmd.Parameters.Add(":t", "Test");
                    cmd.Parameters.Add(":b1", bytes);
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "SELECT * FROM Test";

                    // now check with sequential access
                    using (var reader = cmd.ExecuteReader())
                    {
                        Assert.IsTrue(reader.Read(), "data reader shouldn't be empty");
                        int mylen = len;
                        int startIndex = 0;
                        byte[] buff = new byte[8192];
                        while (mylen > 0)
                        {
                            int readLen = Math.Min(mylen, buff.Length);
                            int retVal = (int)reader.GetBytes(5, startIndex, buff, 0, readLen);
                            Assert.AreEqual(readLen, retVal, "wrong length of the chunk");
                            for (int i = 0; i < readLen; i++)
                            {
                                Assert.AreEqual(bytes[startIndex + i], buff[i], "wrong value at position " + i.ToString() + " of the chunk");
                            }

                            startIndex += readLen;
                            mylen -= readLen;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        [Test]
        public void TestGetChar()
        {
            try
            {
                ClearTestTable();

                ExecuteNonQuery("INSERT INTO Test (id, name) VALUES (1, 'a')");

                using (var cmd = new MaxDBCommand("SELECT * FROM Test", mconn))
                {
                    using (var reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.AreEqual('a', reader.GetChar(1), "name field of the first row as char");
                    }
                }
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        [Test]
        public void TestGetChars()
        {
            int len = 50000;

            char[] chars = new char[len];
            byte[] bytes = new byte[len];
            for (int i = 0; i < len; i++)
            {
                bytes[i] = (byte)(i % 127 + 1); //we can not use null
            }

            System.Text.Encoding.ASCII.GetChars(bytes, 0, len, chars, 0);

            try
            {
                ClearTestTable();

                using (var cmd = new MaxDBCommand("INSERT INTO Test (id, name, c1) VALUES(1, :t, :c1)", mconn))
                {
                    cmd.Parameters.Add(":t", "Test");
                    cmd.Parameters.Add(":c1", MaxDBType.LongA).Value = chars;
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "SELECT * FROM Test";

                    // now check with sequential access
                    using (var reader = cmd.ExecuteReader())
                    {
                        Assert.IsTrue(reader.Read());
                        int mylen = len;
                        int startIndex = 0;
                        char[] buff = new char[8192];

                        while (mylen > 0)
                        {
                            int readLen = Math.Min(mylen, buff.Length);
                            int retVal = (int)reader.GetChars(6, startIndex, buff, 0, readLen);
                            Assert.AreEqual(readLen, retVal, "check length of the chunk");
                            for (int i = 0; i < readLen; i++)
                            {
                                Assert.AreEqual(chars[startIndex + i], buff[i], "check value at position " + i.ToString() + " of the chunk");
                            }

                            startIndex += readLen;
                            mylen -= readLen;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        [Test]
        public void TestTextFields()
        {
            try
            {
                ClearTestTable();

                ExecuteNonQuery("INSERT INTO Test (id, name) VALUES (1, 'Text value')");
                ExecuteNonQuery("INSERT INTO Test (id, name) VALUES (2, '123.456')");

                using (var cmd = new MaxDBCommand("SELECT * FROM Test", mconn))
                {
                    using (var reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.AreEqual("Text value", reader["name"].ToString(), "wrong name field of the first row a string");
                        reader.Read();
                        Assert.AreEqual(123.456, reader.GetDouble(1), "wrong name field of the first row a double");
                    }
                }
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        [Test]
        public void TestDateAndTimeFields()
        {
            MaxDBDataReader reader = null;
            try
            {
                ClearTestTable();

                DateTime dt = DateTime.Now;

                using (var cmd = new MaxDBCommand("INSERT INTO Test (id, d, t, dt) VALUES (1, :a, :b, :c)", mconn))
                {
                    cmd.Parameters.Add(new MaxDBParameter(":a", MaxDBType.Date)).Value = dt.Date;
                    cmd.Parameters.Add(new MaxDBParameter(":b", MaxDBType.Time)).Value = dt.TimeOfDay;
                    cmd.Parameters.Add(new MaxDBParameter(":c", MaxDBType.Timestamp)).Value = dt;
                    cmd.ExecuteNonQuery();
                }

                using (var cmd = new MaxDBCommand("SELECT * FROM Test", mconn))
                {
                    using (reader = cmd.ExecuteReader())
                    {
                        reader.Read();

                        long period = TimeSpan.TicksPerMillisecond / 1000;

                        Assert.AreEqual(dt.Date, reader["d"], "wrong date");
                        Assert.AreEqual(dt.TimeOfDay.Ticks / TimeSpan.TicksPerSecond, ((DateTime)reader["t"]).Ticks / TimeSpan.TicksPerSecond, "wrong time of day");
                        Assert.AreEqual(dt.Ticks / period, ((DateTime)reader["dt"]).Ticks / period, "wrong time stamp value");
                    }
                }
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        [Test]
        public void TestReadingBeforeRead()
        {
            Assert.Throws(typeof(MaxDBException), () =>
            {
                try
                {
                    using (var cmd = new MaxDBCommand("SELECT * FROM Test", mconn))
                    {
                        using (MaxDBDataReader reader = cmd.ExecuteReader())
                        {
                            reader.GetInt32(0);
                        }
                    }
                }
                catch (Exception)
                {
                    throw;
                }
            });
        }
    }
}
