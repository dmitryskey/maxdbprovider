//	Copyright (C) 2005-2006 Dmitry S. Kataev
//	Copyright (C) 2004-2005 MySQL AB
//
//	This program is free software; you can redistribute it and/or
//	modify it under the terms of the GNU General Public License
//	as published by the Free Software Foundation; either version 2
//  of the License, or (at your option) any later version.
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
using System.Data.Common;

namespace MaxDB.UnitTesting
{
	/// <summary>
	/// Summary description for DataReaderTests.
	/// </summary>
	[TestFixture]
	public class DataReaderTests : BaseTest
	{
		[TestFixtureSetUp]
		public void Init() 
		{
			Init("CREATE TABLE Test (id INT NOT NULL, name VARCHAR(100), d DATE, t TIME, dt TIMESTAMP, b1 LONG BYTE, c1 LONG ASCII, PRIMARY KEY(id))");
		}

		[TestFixtureTearDown]
		public void TestFixtureTearDown()
		{
			Close();
		}

		[Test]
		public void TestResultSet()
		{
			try 
			{
				ClearTestTable();

                using (MaxDBCommand cmd = new MaxDBCommand(string.Empty, mconn))
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

                using (MaxDBCommand cmd = new MaxDBCommand("SELECT * FROM Test WHERE id >= 50", mconn))
                {
                    // execute it one time
                    using (MaxDBDataReader reader = cmd.ExecuteReader())
                    {
                        Assert.IsNotNull(reader);
                        while (reader.Read()) ;
                        Assert.IsTrue(reader.HasRows);
                        Assert.AreEqual(7, reader.FieldCount);
                    }

                        // execute it again
                    using (MaxDBDataReader reader = cmd.ExecuteReader())
                    {
                        Assert.IsNotNull(reader);
                        while (reader.Read()) ;
                        Assert.IsTrue(reader.HasRows);
                        Assert.AreEqual(7, reader.FieldCount);
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

				for (int x=0; x < 10; x++)
				{
                    using (MaxDBCommand cmd = new MaxDBCommand("INSERT INTO Test (id, name, b1) VALUES(:val, 'Test', NULL)", mconn))
                    {
                        cmd.Parameters.Add(new MaxDBParameter(":val", x));
                        int affected = cmd.ExecuteNonQuery();
                        Assert.AreEqual(1, affected);
                    }

					using (MaxDBCommand cmd = new MaxDBCommand("SELECT * FROM Test", mconn))
					    cmd.ExecuteReader().Close();
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

                using (MaxDBCommand cmd = new MaxDBCommand(string.Empty, mconn))
                {
                    ExecuteNonQuery("INSERT INTO Test(id,name) VALUES(1,'test1')");
                    ExecuteNonQuery("INSERT INTO Test(id,name) VALUES(2,'test2')");
                    ExecuteNonQuery("INSERT INTO Test(id,name) VALUES(3,'test3')");

                    cmd.CommandText = "SELECT * FROM Test";

                    using (MaxDBDataReader reader = cmd.ExecuteReader(CommandBehavior.SingleRow))
                    {
                        Assert.IsTrue(reader.Read(), "First read");
                        Assert.IsFalse(reader.Read(), "Second read");
                        reader.Close();
                    }

                    cmd.CommandText = "SELECT * FROM test WHERE id = 1";

                    using (MaxDBDataReader reader = cmd.ExecuteReader(CommandBehavior.SingleRow))
                    {
                        Assert.IsTrue(reader.Read());
                        Assert.AreEqual("test1", reader.GetString(1));
                        Assert.IsFalse(reader.Read());
                        reader.Close();
                    }

                    cmd.CommandText = "SELECT * FROM test WHERE rowno <= 2";

                    using (MaxDBDataReader reader = cmd.ExecuteReader(CommandBehavior.SingleRow))
                    {
                        Assert.IsTrue(reader.Read());
                        Assert.AreEqual("test1", reader.GetString(1));
                        Assert.IsFalse(reader.Read());
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

                using (MaxDBCommand cmd = new MaxDBCommand(String.Empty, mconn))
                {
                    ExecuteNonQuery("INSERT INTO Test(id, name) VALUES(1, 'test')");

                    cmd.CommandText = "SELECT * FROM Test";

                    using (MaxDBDataReader reader = cmd.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        Assert.IsTrue(reader.Read());
                        reader.Close();
                        Assert.IsTrue(mconn.State == ConnectionState.Closed);
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

                using (MaxDBCommand cmd = new MaxDBCommand(string.Empty, mconn))
                {
                    ExecuteNonQuery("INSERT INTO Test(id, name) VALUES(1, 'test1')");
                    ExecuteNonQuery("INSERT INTO Test(id, name) VALUES(2, 'test2')");
                    ExecuteNonQuery("INSERT INTO Test(id, name) VALUES(3, 'test3')");

                    cmd.CommandText = "SELECT * FROM Test FOR UPDATE";

                    using (MaxDBDataReader reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly))
                    {
                        DataTable table = reader.GetSchemaTable();
                        Assert.AreEqual(7, table.Rows.Count);
                        Assert.AreEqual(16, table.Columns.Count);
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
		public void TestDBNulls() 
		{
			try 
			{
				ClearTestTable();

                using (MaxDBCommand cmd = new MaxDBCommand(string.Empty, mconn))
                {
                    ExecuteNonQuery("INSERT INTO Test(id, name) VALUES(1, 'Test')");

                    cmd.CommandText = "INSERT INTO Test(id, name) VALUES(2, :null)";
                    cmd.Parameters.Add(":null", MaxDBType.VarCharA).Value = DBNull.Value;
                    cmd.ExecuteNonQuery();
                    ExecuteNonQuery("INSERT INTO Test(id, name) VALUES(3, 'Test2')");

                    cmd.CommandText = "SELECT * FROM Test";

                    using (MaxDBDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.AreEqual(1, reader.GetValue(0));
                        Assert.AreEqual(1, reader.GetInt32(0));
                        Assert.AreEqual("Test", reader.GetValue(1).ToString());
                        Assert.AreEqual("Test", reader.GetString(1));
                        reader.Read();
                        Assert.AreEqual(2, reader.GetValue(0));
                        Assert.AreEqual(2, reader.GetInt32(0));
                        Assert.AreEqual(DBNull.Value, reader.GetValue(1));
                        Assert.AreEqual(null, reader.GetString(1));
                        reader.Read();
                        Assert.AreEqual(3, reader.GetValue(0));
                        Assert.AreEqual(3, reader.GetInt32(0));
                        Assert.AreEqual("Test2", reader.GetValue(1).ToString());
                        Assert.AreEqual("Test2", reader.GetString(1));
                        Assert.IsFalse(reader.Read());
                    }
                }
			}
			catch (Exception ex) 
			{
				Assert.Fail( ex.Message );
			}
		}

		[Test]
		public void TestGetByte() 
		{
			try 
			{
				ClearTestTable();
				
                ExecuteNonQuery("INSERT INTO Test (id, name) VALUES (123, 'a')");

                using (MaxDBCommand cmd = new MaxDBCommand("SELECT * FROM Test", mconn))
                using (MaxDBDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.AreEqual(123, reader.GetByte(0));
                        Assert.AreEqual(97, reader.GetByte(1));
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

                using (MaxDBCommand cmd = new MaxDBCommand("INSERT INTO Test (id, name, b1) VALUES(1, :t, :b1)", mconn))
                {
                    cmd.Parameters.Add(":t", "Test");
                    cmd.Parameters.Add(":b1", bytes);
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "SELECT * FROM Test";

                    //  now check with sequential access
                    using (MaxDBDataReader reader = cmd.ExecuteReader())
                    {
                        Assert.IsTrue(reader.Read());
                        int mylen = len;
                        int startIndex = 0;
                        byte[] buff = new byte[8192];
                        while (mylen > 0)
                        {
                            int readLen = Math.Min(mylen, buff.Length);
                            int retVal = (int)reader.GetBytes(5, startIndex, buff, 0, readLen);
                            Assert.AreEqual(readLen, retVal);
                            for (int i = 0; i < readLen; i++)
                                Assert.AreEqual(bytes[startIndex + i], buff[i]);
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

				using(MaxDBCommand cmd = new MaxDBCommand("SELECT * FROM Test", mconn))
                using (MaxDBDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.AreEqual('a', reader.GetChar(1));
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
				bytes[i] = (byte)(i % 127 + 1); //we can not use null

			System.Text.Encoding.ASCII.GetChars(bytes, 0, len, chars, 0);
			
			try 
			{
				ClearTestTable();

                using (MaxDBCommand cmd = new MaxDBCommand("INSERT INTO Test (id, name, c1) VALUES(1, :t, :c1)", mconn))
                {
                    cmd.Parameters.Add(":t", "Test");
                    cmd.Parameters.Add(":c1", MaxDBType.LongA).Value = chars;
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "SELECT * FROM Test";

                    // now check with sequential access
                    using (MaxDBDataReader reader = cmd.ExecuteReader())
                    {
                        Assert.IsTrue(reader.Read());
                        int mylen = len;
                        int startIndex = 0;
                        char[] buff = new char[8192];

                        while (mylen > 0)
                        {
                            int readLen = Math.Min(mylen, buff.Length);
                            int retVal = (int)reader.GetChars(6, startIndex, buff, 0, readLen);
                            Assert.AreEqual(readLen, retVal);
                            for (int i = 0; i < readLen; i++)
                                Assert.AreEqual(chars[startIndex + i], buff[i]);
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

                using (MaxDBCommand cmd = new MaxDBCommand("SELECT * FROM Test", mconn))
                using (MaxDBDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.AreEqual("Text value", reader["name"].ToString());
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

                using (MaxDBCommand cmd = new MaxDBCommand("INSERT INTO Test (id, d, t, dt) VALUES (1, :a, :b, :c)", mconn))
                {
                    cmd.Parameters.Add(new MaxDBParameter(":a", MaxDBType.Date)).Value = dt.Date;
                    cmd.Parameters.Add(new MaxDBParameter(":b", MaxDBType.Time)).Value = dt.TimeOfDay;
                    cmd.Parameters.Add(new MaxDBParameter(":c", MaxDBType.Timestamp)).Value = dt;
                    cmd.ExecuteNonQuery();
                }

				using (MaxDBCommand cmd = new MaxDBCommand("SELECT * FROM Test", mconn))
                    using (reader = cmd.ExecuteReader())
                    {
                        reader.Read();

                        long period = TimeSpan.TicksPerMillisecond / 1000;

                        Assert.AreEqual(dt.Date, reader["d"]);
                        Assert.AreEqual(dt.TimeOfDay.Ticks / TimeSpan.TicksPerSecond, ((DateTime)reader["t"]).Ticks / TimeSpan.TicksPerSecond);
                        Assert.AreEqual(dt.Ticks / period, ((DateTime)reader["dt"]).Ticks / period);
                    }
			}
			catch (Exception ex) 
			{
				Assert.Fail(ex.Message);
			}
		}

		[Test]
		[ExpectedException(typeof(MaxDBException))]
		public void TestReadingBeforeRead() 
		{
			try 
			{
				using(MaxDBCommand cmd = new MaxDBCommand("SELECT * FROM Test", mconn))
                using (MaxDBDataReader reader = cmd.ExecuteReader())
				        reader.GetInt32(0);
			}
			catch (Exception) 
			{
				throw;
			}
		}
	}
}
