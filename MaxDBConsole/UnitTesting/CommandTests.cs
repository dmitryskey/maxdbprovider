using System;
using NUnit.Framework;
using MaxDBDataProvider;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading;

namespace MaxDBConsole.UnitTesting
{
	/// <summary>
	/// Summary description for CommandTests.
	/// </summary>
	[TestFixture()]
	public class CommandTests
	{
		private MaxDBConnection m_conn;

		public CommandTests()
		{
			//
			// TODO: Add constructor logic here
			//
		}

		[TestFixtureSetUp]
		public void Init() 
		{
			try
			{
				m_conn = new MaxDBConnection(System.Configuration.ConfigurationSettings.AppSettings["ConnectionString"]);
				m_conn.Open();
				m_conn.AutoCommit = true;
				try
				{
					(new MaxDBCommand("EXISTS TABLE Test", m_conn)).ExecuteNonQuery();
					(new MaxDBCommand("DROP TABLE Test", m_conn)).ExecuteNonQuery();
				}
				catch(MaxDBException ex)
				{
					if (ex.DetailErrorCode != -708)
						throw;
				}

				(new MaxDBCommand("CREATE TABLE Test (id int NOT NULL, name VARCHAR(100))", m_conn)).ExecuteNonQuery();
			}
			catch(Exception ex)
			{
				Assert.Fail(ex.Message);
			}
		}

		[TestFixtureTearDown]
		public void TestFixtureTearDown() 
		{
			(new MaxDBCommand("DROP TABLE Test", m_conn)).ExecuteNonQuery();
			m_conn.Close();
		}

		[Test()]
		public void TestInsert()
		{
			try 
			{
				ClearTestTable();

				// do the insert
				MaxDBCommand cmd = new MaxDBCommand("INSERT INTO Test (id, name) VALUES(10, 'Test')", m_conn);
				int cnt = cmd.ExecuteNonQuery();
				Assert.AreEqual(1, cnt, "Insert Count");

				// make sure we get the right value back out
				cmd.CommandText = "SELECT name FROM Test WHERE id = 10";
				string name = (string)cmd.ExecuteScalar();
				Assert.AreEqual("Test", name.Trim(), "Insert result" );

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
				Assert.AreEqual("Test2", name.Trim(), "Insert with parameters result" );
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

				(new MaxDBCommand("INSERT INTO Test (id, name) VALUES(10, 'Test')", m_conn)).ExecuteNonQuery();
				(new MaxDBCommand("INSERT INTO Test (id,name) VALUES(11, 'Test2')", m_conn)).ExecuteNonQuery();

				// do the update
				MaxDBCommand cmd = new MaxDBCommand("UPDATE Test SET name='Test3' WHERE id = 10 OR id = 11", m_conn);
				MaxDBConnection c = (MaxDBConnection)cmd.Connection;
				Assert.AreEqual(m_conn, c);
				int cnt = cmd.ExecuteNonQuery();
				Assert.AreEqual(2, cnt);

				// make sure we get the right value back out
				cmd.CommandText = "SELECT name FROM Test WHERE id = 10";
				string name = (string)cmd.ExecuteScalar();
				Assert.AreEqual("Test3", name.Trim());
			
				cmd.CommandText = "SELECT name FROM Test WHERE id = 11";
				name = (string)cmd.ExecuteScalar();
				Assert.AreEqual("Test3", name.Trim());

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
				Assert.AreEqual("Test5", name.Trim());
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

				(new MaxDBCommand("INSERT INTO Test (id, name) VALUES(1, 'Test')", m_conn)).ExecuteNonQuery();
				(new MaxDBCommand("INSERT INTO Test (id, name) VALUES(2, 'Test2')", m_conn)).ExecuteNonQuery();

				MaxDBCommand cmd = new MaxDBCommand("DELETE FROM Test WHERE id = 1 or id = 2", m_conn);
				int delcnt = cmd.ExecuteNonQuery();
				Assert.AreEqual(2, delcnt);
			
				// find out how many rows we have now
				cmd.CommandText = "SELECT COUNT(*) FROM Test";
				object after_cnt = cmd.ExecuteScalar();
				Assert.AreEqual(0, after_cnt);
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
				MaxDBCommand cmd = new MaxDBCommand("INSERT INTO test VALUES(1, :str)", m_conn);
				cmd.Parameters.Add(":str", MaxDBType.VarCharA);
				cmd.Parameters[0].Value = null;
				cmd.ExecuteNonQuery();

				cmd.CommandText = "SELECT * FROM test";

				reader = cmd.ExecuteReader();
				Assert.IsTrue(reader.Read());
				Assert.AreEqual(DBNull.Value, reader[1]);
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
				MaxDBCommand cmd = new MaxDBCommand("INSERT INTO Test VALUES(1, 'Test')", m_conn);
				cmd.ExecuteNonQuery();

				cmd.CommandText = "SELECT * FROM Test";
				reader = cmd.ExecuteReader();
				Assert.IsTrue(reader.Read());
				Assert.IsFalse(reader.Read());
				Assert.IsFalse(reader.NextResult());
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
				m_conn.AutoCommit = false;

				MaxDBCommand cnt_cmd = new MaxDBCommand("SELECT count(*) FROM Test", m_conn);

				trans = m_conn.BeginTransaction();
				MaxDBCommand cmd = new MaxDBCommand("INSERT INTO Test VALUES(1, 'Test1')", m_conn, trans);
				cmd.ExecuteNonQuery();
				trans.Commit();

				Assert.AreEqual(1, cnt_cmd.ExecuteScalar());

				trans = m_conn.BeginTransaction();
				cmd = new MaxDBCommand("INSERT INTO Test VALUES(2, 'Test2')", m_conn, trans);
				cmd.ExecuteNonQuery();
				trans.Rollback();

				Assert.AreEqual(1, cnt_cmd.ExecuteScalar());
			}
			catch (Exception ex)
			{
				if (trans != null) trans.Rollback();
				Assert.Fail(ex.Message);
			}
			finally
			{
				m_conn.AutoCommit = true;
			}
		}

		[Test]
		public void TestCloneCommand() 
		{
			try
			{
				MaxDBTransaction txn = m_conn.BeginTransaction();
				MaxDBCommand cmd = new MaxDBCommand("SELECT * FROM Test WHERE id = :id", m_conn, txn);
				cmd.Parameters.Add(":test", 1);

				IDbCommand cmd2 = ((ICloneable)cmd).Clone() as IDbCommand;
				cmd2.ToString();
				txn.Rollback();
			}
			catch (Exception ex)
			{
				Assert.Fail(ex.Message);
			}
		}

		/*
		private MaxDBCommand m_cmd;

		public void CancelQuery() 
		{
			DateTime start = DateTime.Now;
			while(DateTime.Now.Subtract(start).TotalSeconds < 2);
			m_cmd.Cancel();
		}

		[Test]
		public void TestCancel() 
		{
			m_cmd = new MaxDBCommand("CALL ForEver(5000)", m_conn);
        
			Thread cancel_thread = new Thread(new ThreadStart(this.CancelQuery));
			cancel_thread.Start();

			try 
			{
				m_cmd.ExecuteNonQuery();

				Assert.Fail("Execution should not have finished.");
			}            
			catch(MaxDBException) 
			{
			}
		}
		*/

		private void ClearTestTable()
		{
			(new MaxDBCommand("DELETE FROM Test", m_conn)).ExecuteNonQuery();
		}

	}
}
