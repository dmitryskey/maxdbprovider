//	Copyright (C) 2005-2006 Dmitry S. Kataev
//	Copyright (C) 2004-2005 MySQL AB
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
using System.Data.Common;
using System.Diagnostics;

namespace MaxDB.UnitTesting
{
	/// <summary>
	/// Summary description for DataAdapterTests.
	/// </summary>
	[TestFixture]
	public class DataAdapterTests : BaseTest
	{
		[TestFixtureSetUp]
		public void SetUp()
		{
			Init("CREATE TABLE Test (id INT NOT NULL DEFAULT SERIAL, id2 INT NOT NULL UNIQUE, name VARCHAR(100), dt DATE, tm TIME, ts TIMESTAMP, OriginalId INT, PRIMARY KEY(id, id2))");
		}

		[TestFixtureTearDown]
		public void TearDown()
		{
			Close();
		}

		[Test]
		public void TestFill()
		{
			try
			{
				ClearTestTable();

				MaxDBCommand cmd = new MaxDBCommand(string.Empty, mconn);
				cmd.Parameters.Add(new MaxDBParameter(":now", MaxDBType.Date)).Value = DateTime.Now;

				cmd.CommandText = "INSERT INTO Test (id2, name, dt) VALUES (1, 'Name 1', :now)";
				cmd.ExecuteNonQuery();
				cmd.CommandText = "INSERT INTO Test (id2, name, dt) VALUES (2, NULL, :now)";
				cmd.ExecuteNonQuery();
				cmd.CommandText = "INSERT INTO Test (id2, name, dt) VALUES (3, '', :now)";
				cmd.ExecuteNonQuery();

				using (MaxDBDataAdapter da = new MaxDBDataAdapter("SELECT id, id2, name FROM Test", mconn))
				{
					DataSet ds = new DataSet();
					da.Fill(ds, "Test");

					Assert.AreEqual(1, ds.Tables.Count, "At least one table should be filled");
					Assert.AreEqual(3, ds.Tables[0].Rows.Count, "At least three rows should be inserted");

					Assert.AreEqual(1, ds.Tables[0].Rows[0]["id"], "id field of the first row");
					Assert.AreEqual(2, ds.Tables[0].Rows[1]["id"], "id field of the second row");
					Assert.AreEqual(3, ds.Tables[0].Rows[2]["id"], "id field of the third row");

					Assert.AreEqual(1, ds.Tables[0].Rows[0]["id2"], "id2 field of the first row");
					Assert.AreEqual(2, ds.Tables[0].Rows[1]["id2"], "id2 field of the second row");
					Assert.AreEqual(3, ds.Tables[0].Rows[2]["id2"], "id2 field of the third row");

					Assert.AreEqual("Name 1", ds.Tables[0].Rows[0]["name"].ToString(), "name field of the first row");
					Assert.AreEqual(DBNull.Value, ds.Tables[0].Rows[1]["name"], "name field of the second row");
					Assert.AreEqual(String.Empty, ds.Tables[0].Rows[2]["name"].ToString(), "name field of the third row");
				}
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

				using (MaxDBDataAdapter da = new MaxDBDataAdapter("SELECT * FROM Test FOR UPDATE", mconn))
				{
					MaxDBCommandBuilder cb = new MaxDBCommandBuilder(da);
					cb.GetType(); //add this command since mcs compiler supposes that cb is never used
					DataTable dt = new DataTable();
					da.Fill(dt);

					DataRow dr = dt.NewRow();
#if MONO
					dr["id"] = 0;
#endif // MONO
					dr["id2"] = 2;
					dr["name"] = "TestName1";
					dt.Rows.Add(dr);

					int count = da.Update(dt);

					using (MaxDBCommand cmd = new MaxDBCommand("SELECT MAX(id) FROM Test", mconn))
						dr["id"] = cmd.ExecuteScalar();

					// make sure our refresh of auto increment values worked
					Assert.AreEqual(1, count, "At least one row should be inserted");
					Assert.IsFalse(dt.Rows[dt.Rows.Count - 1]["id"] == DBNull.Value, "id field shouldn't be NULL");

					dt.Rows[0]["id2"] = 2;
					dt.Rows[0]["name"] = "TestName2";
					dt.Rows[0]["ts"] = DBNull.Value;
					DateTime day1 = new DateTime(2003, 1, 16, 12, 24, 0);
					dt.Rows[0]["dt"] = day1;
					dt.Rows[0]["tm"] = new DateTime(DateTime.MinValue.Year, DateTime.MinValue.Month, DateTime.MinValue.Day,
						day1.TimeOfDay.Hours, day1.TimeOfDay.Minutes, day1.TimeOfDay.Seconds);
					count = da.Update(dt);

					Assert.AreEqual(DBNull.Value, dt.Rows[0]["ts"], "ts field should be NULL");
					Assert.AreEqual(2, dt.Rows[0]["id2"], "id2 field");

					dt.Rows.Clear();
					da.Fill(dt);

					DateTime dateTime = (DateTime)dt.Rows[0]["dt"];
					Assert.AreEqual(day1.Date, dateTime, "dt field");
					Assert.AreEqual(day1.TimeOfDay, ((DateTime)dt.Rows[0]["tm"]).TimeOfDay, "tm field");

					dt.Rows[0].Delete();
					count = da.Update(dt);

					Assert.AreEqual(1, count, "Table should contain at least one row");

					dt.Rows.Clear();
					da.Fill(dt);
					Assert.AreEqual(0, dt.Rows.Count, "Table should be empty");
				}
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
				using (MaxDBDataAdapter da = new MaxDBDataAdapter("SELECT * FROM Test FOR UPDATE", mconn))
				{
					MaxDBCommandBuilder cb = new MaxDBCommandBuilder(da);
					cb.GetType(); //add this command since mcs compiler supposes that cb is never used
					DataTable dt = new DataTable();
					da.Fill(dt);

					DataRow row = dt.NewRow();
#if MONO
					row["id"] = 0;
#endif // MONO
					row["id2"] = 1;
					row["name"] = "Test";
					row["dt"] = DBNull.Value;
					row["tm"] = DBNull.Value;
					row["ts"] = DBNull.Value;
					row["OriginalId"] = 2;
					dt.Rows.Add(row);
					da.Update(dt);

					Assert.AreEqual(1, dt.Rows.Count, "Table should contain at least one row");
					Assert.AreEqual(2, dt.Rows[0]["OriginalId"], "OriginalId field");
				}
			}
			catch (Exception ex)
			{
				Assert.Fail(ex.Message);
			}
		}

		[Test]
		public void TestUpdatingOfManyRows()
		{
			MaxDBTransaction trans = null;
			try
			{
				mconn.AutoCommit = false;
				using (trans = mconn.BeginTransaction())
				{
					const int rowCount = 1000;
					ClearTestTable();
					using (MaxDBDataAdapter da = new MaxDBDataAdapter("SELECT * FROM Test FOR UPDATE", mconn))
					{
						MaxDBCommandBuilder cb = new MaxDBCommandBuilder(da);
						cb.GetType(); //add this command since mcs compiler supposes that cb is never used
						DataTable dt = new DataTable();
						da.Fill(dt);

#if NET20
						da.UpdateBatchSize = 0;
#endif // NET20

						DataRow dr;

						for (int i = 0; i < rowCount; i++)
						{
							dr = dt.NewRow();
#if MONO
							dr["id"] = 0;
#endif // MONO
							dr["id2"] = i;
							dt.Rows.Add(dr);
						}

						da.Update(dt.GetChanges());
						dt.AcceptChanges();

						dt.Clear();
						da.Fill(dt);
						Assert.AreEqual(rowCount, dt.Rows.Count, "Table row count");

						for (int i = 0; i < rowCount; i++)
							dt.Rows[i]["name"] = "Name " + i.ToString();

						dr = dt.NewRow();
#if MONO
						dr["id"] = 0;
#endif // MONO
						dr["id2"] = rowCount + 1;
						dr["name"] = "Name " + rowCount.ToString();
						dt.Rows.Add(dr);

#if NET20
						da.UpdateBatchSize = rowCount / 15;
#endif // NET20
						da.Update(dt.GetChanges());
						dt.AcceptChanges();

						dt.Clear();
						da.Fill(dt);

						Assert.AreEqual(rowCount + 1, dt.Rows.Count, "Table row count + 1");

						for (int i = 0; i < rowCount + 1; i++)
							Assert.AreEqual(dt.Rows[i]["name"].ToString(), "Name " + i.ToString(), "Table row #" + (i + 1).ToString());

						trans.Commit();
					}
				}
			}
			catch (Exception ex)
			{
				if (trans != null)
					trans.Rollback();
				Assert.Fail(ex.Message);
			}
			finally
			{
				mconn.AutoCommit = true;
			}
		}
	}
}
