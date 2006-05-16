using System;
using NUnit.Framework;
using MaxDBDataProvider;
using System.Data;
using System.Data.Common;
using System.Diagnostics;

namespace MaxDBConsole.UnitTesting
{
	/// <summary>
	/// Summary description for DataAdapterTests.
	/// </summary>
	[TestFixture()]
	public class DataAdapterTests : BaseTest
	{
		public DataAdapterTests()
		{
			//
			// TODO: Add constructor logic here
			//
		}

		[TestFixtureSetUp]
		public void Init() 
		{
			Init("CREATE TABLE Test (id INT NOT NULL DEFAULT SERIAL, id2 INT NOT NULL, name VARCHAR(100), dt DATE, tm TIME, ts TIMESTAMP, OriginalId INT, PRIMARY KEY(id, id2))");
		}

		[TestFixtureTearDown]
		public void TestFixtureTearDown()
		{
			Close();
		}

		[Test]
		public void TestFill()
		{
			try 
			{
				ClearTestTable();

				MaxDBCommand cmd = new MaxDBCommand(string.Empty, m_conn);
				cmd.Parameters.Add(new MaxDBParameter(":now", MaxDBType.Date)).Value = DateTime.Now;
				
				cmd.CommandText = "INSERT INTO Test (id2, name, dt) VALUES (1, 'Name 1', :now)";
				cmd.ExecuteNonQuery();
				cmd.CommandText = "INSERT INTO Test (id2, name, dt) VALUES (2, NULL, :now)";
				cmd.ExecuteNonQuery();
				cmd.CommandText = "INSERT INTO Test (id2, name, dt) VALUES (3, '', :now)";
				cmd.ExecuteNonQuery();

				MaxDBDataAdapter da = new MaxDBDataAdapter("SELECT id, id2, name FROM Test", m_conn);
				DataSet ds = new DataSet();
				da.Fill(ds, "Test");

				Assert.AreEqual(1, ds.Tables.Count);
				Assert.AreEqual(3, ds.Tables[0].Rows.Count);

				Assert.AreEqual(1, ds.Tables[0].Rows[0]["id"]);
				Assert.AreEqual(2, ds.Tables[0].Rows[1]["id"]);
				Assert.AreEqual(3, ds.Tables[0].Rows[2]["id"]);

				Assert.AreEqual(1, ds.Tables[0].Rows[0]["id2"]);
				Assert.AreEqual(2, ds.Tables[0].Rows[1]["id2"]);
				Assert.AreEqual(3, ds.Tables[0].Rows[2]["id2"]);

				Assert.AreEqual("Name 1", ds.Tables[0].Rows[0]["name"].ToString().Trim());
				Assert.AreEqual(DBNull.Value, ds.Tables[0].Rows[1]["name"]);
				Assert.AreEqual(String.Empty, ds.Tables[0].Rows[2]["name"].ToString().Trim());
			}
			catch (Exception ex)
			{
				Assert.Fail(ex.Message);
			}
		}

		[Test]
		public void TestUpdate()
		{
			try 
			{
				ClearTestTable();
				
				MaxDBDataAdapter da = new MaxDBDataAdapter("SELECT * FROM Test FOR UPDATE", m_conn);
				MaxDBCommandBuilder cb = new MaxDBCommandBuilder(da, new string[]{"id", "id2"}, new string[]{"id"});
				cb.GetType(); //add this command since mcs compiler supposes that cb is never used
				DataTable dt = new DataTable();
				da.Fill(dt);

				DataRow dr = dt.NewRow();
				dr["id"] = 1;
				dr["id2"] = 2;
				dr["name"] = "TestName1";
				dt.Rows.Add(dr);

				int count = da.Update(dt);

				MaxDBCommand cmd = new MaxDBCommand("SELECT Max(id) FROM Test", m_conn);
				dr["id"] = cmd.ExecuteScalar();

				// make sure our refresh of auto increment values worked
				Assert.AreEqual(1, count);
				Assert.IsFalse(dt.Rows[dt.Rows.Count - 1]["id"] == DBNull.Value, "auto increment value can't be NULL");

				dt.Rows[0]["id2"] = 2;
				dt.Rows[0]["name"] = "TestName2";
				dt.Rows[0]["ts"] = DBNull.Value;
				DateTime day1 = new DateTime(2003, 1, 16, 12, 24, 0);
				dt.Rows[0]["dt"] = day1;
				dt.Rows[0]["tm"] = new DateTime(DateTime.MinValue.Year, DateTime.MinValue.Month, DateTime.MinValue.Day, 
					day1.TimeOfDay.Hours, day1.TimeOfDay.Minutes, day1.TimeOfDay.Seconds);
				count = da.Update(dt);

				Assert.AreEqual(DBNull.Value, dt.Rows[0]["ts"]);
				Assert.AreEqual(2, dt.Rows[0]["id2"]);

				dt.Rows.Clear();
				da.Fill(dt);

				DateTime dateTime = (DateTime)dt.Rows[0]["dt"];
				Assert.AreEqual(day1.Date, dateTime);
				Assert.AreEqual(day1.TimeOfDay, ((DateTime)dt.Rows[0]["tm"]).TimeOfDay);

				dt.Rows[0].Delete();
				count = da.Update(dt);

				Assert.AreEqual(1, count);

				dt.Rows.Clear();
				da.Fill(dt);
				Assert.AreEqual(0, dt.Rows.Count);
			}
			catch (Exception ex)
			{
				Assert.Fail(ex.Message);
			}
		}

		[Test]
		public void TestOriginalId()
		{
			try 
			{
				ClearTestTable();
				MaxDBDataAdapter da = new MaxDBDataAdapter("SELECT * FROM Test FOR UPDATE", m_conn);
				MaxDBCommandBuilder cb = new MaxDBCommandBuilder(da, new string[]{"id", "id2"}, new string[]{"id"});
				cb.GetType(); //add this command since mcs compiler supposes that cb is never used
				DataTable dt = new DataTable();
				da.Fill(dt);

				DataRow row = dt.NewRow();
				row["id"] = 1;//DBNull.Value; 
				row["id2"] = 1;
				row["name"] = "Test";
				row["dt"] = DBNull.Value;
				row["tm"] = DBNull.Value;
				row["ts"] = DBNull.Value;
				row["OriginalId"] = 2;
				dt.Rows.Add(row);
				da.Update(dt);

				Assert.AreEqual(1, dt.Rows.Count);
				Assert.AreEqual(2, dt.Rows[0]["OriginalId"]);
			}
			catch (Exception ex)
			{
				Assert.Fail(ex.Message);
			}
		}

		[Test]
		public void TestUpdatingOfManyRows() 
		{
			try 
			{
				ClearTestTable();
				MaxDBDataAdapter da = new MaxDBDataAdapter("SELECT * FROM test FOR UPDATE", m_conn);
				MaxDBCommandBuilder cb = new MaxDBCommandBuilder(da, new string[]{"id", "id2"}, new string[]{"id"});
				cb.GetType(); //add this command since mcs compiler supposes that cb is never used
				DataTable dt = new DataTable();
				da.Fill(dt);

				for (int i = 0; i < 1000; i++)
				{
					DataRow dr = dt.NewRow();
					dr["id"] = 1;//DBNull.Value;
					dr["id2"] = i;
					dt.Rows.Add(dr);
					DataTable changes = dt.GetChanges();
					da.Update(changes);
					dt.AcceptChanges();
				}

				dt.Clear();
				da.Fill(dt);
				Assert.AreEqual(1000, dt.Rows.Count);
			}
			catch (Exception ex)
			{
				Assert.Fail(ex.Message);
			}
		}
	}
}
