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
using System.Text;
using System.Threading;
using System.Diagnostics;

namespace MaxDB.UnitTesting
{
	/// <summary>
	/// Summary description for CommandTests.
	/// </summary>
	[TestFixture()]
	public class CommandTests : BaseTest
	{
		public CommandTests()
		{
			//
			// TODO: Add constructor logic here
			//
		}

		[TestFixtureSetUp]
		public void TestFixtureSetUp() 
		{
			Init("CREATE TABLE Test (id int NOT NULL, name VARCHAR(100))");
		}

		[TestFixtureTearDown]
		public void TestFixtureTearDown()
		{
			Close();
		}

		[Test()]
		public void TestInsert()
		{
			try 
			{
				ClearTestTable();

				// do the insert
				using(MaxDBCommand cmd = new MaxDBCommand("INSERT INTO Test (id, name) VALUES(10, 'Test')", mconn))
				{
					int cnt = cmd.ExecuteNonQuery();
					Assert.AreEqual(1, cnt, "Insert Count");

					// make sure we get the right value back out
					cmd.CommandText = "SELECT name FROM Test WHERE id = 10";
					string name = (string)cmd.ExecuteScalar();
					Assert.AreEqual("Test", name, "Insert result" );

					// now do the insert with parameters
					cmd.CommandText = "INSERT INTO Test (id, name) VALUES( :id, :name)";
					cmd.Parameters.Add(new MaxDBParameter(":id", 11));
					cmd.Parameters.Add(new MaxDBParameter(":name", "Test2"));
					cnt = cmd.ExecuteNonQuery();
					Assert.AreEqual(1, cnt, "Insert with Parameters Count");

					// make sure we get the right value back out
					cmd.Parameters.Clear();
					cmd.CommandText = "SELECT name FROM Test WHERE id = 11";
					name = (string)cmd.ExecuteScalar();
					Assert.AreEqual("Test2", name, "Insert with parameters result" );
				}
			}
			catch(MaxDBException ex)
			{
				Assert.Fail(ex.Message);
			}
		}

		[Test()]
		public void TestUpdate()
		{
			try 
			{
				ClearTestTable();

				ExecuteNonQuery("INSERT INTO Test (id, name) VALUES(10, 'Test')");
				ExecuteNonQuery("INSERT INTO Test (id,name) VALUES(11, 'Test2')");

				// do the update
				using(MaxDBCommand cmd = new MaxDBCommand("UPDATE Test SET name='Test3' WHERE id = 10 OR id = 11", mconn))
				{
					MaxDBConnection c = (MaxDBConnection)cmd.Connection;
					Assert.AreEqual(mconn, c);
					int cnt = cmd.ExecuteNonQuery();
					Assert.AreEqual(2, cnt);

					// make sure we get the right value back out
					cmd.CommandText = "SELECT name FROM Test WHERE id = 10";
					string name = (string)cmd.ExecuteScalar();
					Assert.AreEqual("Test3", name);
			
					cmd.CommandText = "SELECT name FROM Test WHERE id = 11";
					name = (string)cmd.ExecuteScalar();
					Assert.AreEqual("Test3", name);

					// now do the update with parameters
					cmd.CommandText = "UPDATE Test SET name = :name WHERE id = :id";
					cmd.Parameters.Add(new MaxDBParameter(":name", "Test5"));
					cmd.Parameters.Add(new MaxDBParameter(":id", 11));
					cnt = cmd.ExecuteNonQuery();
					Assert.AreEqual(1, cnt, "Update with Parameters Count" );

					// make sure we get the right value back out
					cmd.Parameters.Clear();
					cmd.CommandText = "SELECT name FROM Test WHERE id = 11";
					name = (string)cmd.ExecuteScalar();
					Assert.AreEqual("Test5", name);
				}
			}
			catch (Exception ex)
			{
				Assert.Fail(ex.Message);
			}
		}

		[Test()]
		public void TestDelete()
		{
			try 
			{
				ClearTestTable();

				ExecuteNonQuery("INSERT INTO Test (id, name) VALUES(1, 'Test')");
				ExecuteNonQuery("INSERT INTO Test (id, name) VALUES(2, 'Test2')");

				using(MaxDBCommand cmd = new MaxDBCommand("DELETE FROM Test WHERE id = 1 or id = 2", mconn))
				{
					int delcnt = cmd.ExecuteNonQuery();
					Assert.AreEqual(2, delcnt);
			
					// find out how many rows we have now
					cmd.CommandText = "SELECT COUNT(*) FROM Test";
					object after_cnt = cmd.ExecuteScalar();
					Assert.AreEqual(0, after_cnt);
				}
			}
			catch (Exception ex)
			{
				Assert.Fail( ex.Message );
			}
		}

		[Test]
		public void TestInsertNullParameter()
		{
			MaxDBDataReader reader = null;
			try 
			{
				ClearTestTable();
				
				using(MaxDBCommand cmd = new MaxDBCommand("INSERT INTO test VALUES(1, :str)", mconn))
				{
					cmd.Parameters.Add(":str", MaxDBType.VarCharA);
					cmd.Parameters[0].Value = null;
					cmd.ExecuteNonQuery();

					cmd.CommandText = "SELECT * FROM test";

					using(reader = cmd.ExecuteReader())
					{
						Assert.IsTrue(reader.Read());
						Assert.AreEqual(DBNull.Value, reader[1]);
					}
					reader = null;
				}
			}
			catch(Exception ex)
			{
				Assert.Fail(ex.Message);
			}
			finally 
			{
				if (reader != null) reader.Close();
			}
		}

		[Test]
		public void TestInsertUsingReader()
		{
			MaxDBDataReader reader = null;
			try 
			{
				ClearTestTable();

				using(MaxDBCommand cmd = new MaxDBCommand("INSERT INTO Test VALUES(1, 'Test')", mconn))
				{
					cmd.ExecuteNonQuery();

					cmd.CommandText = "SELECT * FROM Test";
					using(reader = cmd.ExecuteReader())
					{
						Assert.IsTrue(reader.Read());
						Assert.IsFalse(reader.Read());
						Assert.IsFalse(reader.NextResult());
					}
					reader = null;
				}
			}
			catch (Exception ex)
			{
				Assert.Fail(ex.Message);
			}
			finally 
			{
				if (reader != null) reader.Close();
			}
		}

		[Test]
		public void TestTransaction()
		{
			MaxDBTransaction trans = null;
			try
			{
				ClearTestTable();
				mconn.AutoCommit = false;

                using (MaxDBCommand cnt_cmd = new MaxDBCommand("SELECT count(*) FROM Test", mconn))
                {
                    using (trans = mconn.BeginTransaction())
                    {
                        using (MaxDBCommand cmd = new MaxDBCommand("INSERT INTO Test VALUES(1, 'Test1')", mconn, trans))
                        {
                            cmd.ExecuteNonQuery();
                            trans.Commit();
                        }

                        Assert.AreEqual(1, cnt_cmd.ExecuteScalar());

                        trans = mconn.BeginTransaction();

                        using (MaxDBCommand cmd = new MaxDBCommand("INSERT INTO Test VALUES(2, 'Test2')", mconn, trans))
                        {
                            cmd.ExecuteNonQuery();
                            trans.Rollback();
                        }
                    }

                    Assert.AreEqual(1, cnt_cmd.ExecuteScalar());
                }
			}
			catch (Exception ex)
			{
				if (trans != null) trans.Rollback();
				Assert.Fail(ex.Message);
			}
			finally
			{
				mconn.AutoCommit = true;
			}
		}

		[Test]
		public void TestCloneCommand() 
		{
			try
			{
				using(MaxDBTransaction txn = mconn.BeginTransaction())
                    using(MaxDBCommand cmd = new MaxDBCommand("SELECT * FROM Test WHERE id = :id", mconn, txn))
				    {
					    cmd.Parameters.Add(":test", 1);

					    IDbCommand cmd2 = ((ICloneable)cmd).Clone() as IDbCommand;
					    cmd2.ToString();
					    txn.Rollback();
				    }
			}
			catch (Exception ex)
			{
				Assert.Fail(ex.Message);
			}
		}

		private MaxDBCommand mcmd;

		private void CancelQuery() 
		{
			System.Threading.Thread.Sleep(100);
			mcmd.Cancel();
		}

		[Test]
		public void TestCancel() 
		{
            // we use db procedure in order to insert rows as fast as possible
			string dbProc = "CREATE DBPROC InsertManyRows (IN cnt INTEGER) AS" +
					"    VAR i INTEGER;" +
					"TRY" +
					"  SET i = 1; " +
					"  WHILE $rc = 0 AND i <= cnt DO BEGIN" +
					"      INSERT INTO SCOTT.TEST (id, name) VALUES (:i, '*');" +
					"      SET i = i + 1;" +
					"  END;" +
					"CATCH" +
					"  IF $rc <> 100 THEN STOP ($rc, 'unexpected error');";

			try
			{
				ClearTestTable();
				ExecuteNonQuery(dbProc);
			}
			catch(MaxDBException ex)
			{
				if (ex.VendorCode != -6006)
					Assert.Fail(ex.Message);
			}

			try
			{
				ExecuteNonQuery("CALL InsertManyRows(128000)");
			}
			catch(Exception ex)
			{
				Assert.Fail(ex.Message);
			}

            using (mcmd = new MaxDBCommand("UPDATE test SET name = '" + DateTime.Now.ToString() + "'", mconn))
            {
                Thread cancel_thread = new Thread(new ThreadStart(this.CancelQuery));
                cancel_thread.Start();

                try
                {
                    mcmd.ExecuteNonQuery();

                    Assert.Fail("Execution should not have finished.");
                }
                catch (MaxDBException ex)
                {
                    if (ex.VendorCode != -102)
                        Assert.Fail(ex.Message);
                }
            }

            try
            {
                ExecuteNonQuery("DROP DBPROC InsertManyRows");
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
		}
	}
}
