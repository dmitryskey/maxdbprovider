//-----------------------------------------------------------------------------------------------
// <copyright file="DataAdapterTests.cs" company="Dmitry S. Kataev">
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
using FluentAssertions;
using MaxDB.Data;
using NUnit.Framework;

namespace MaxDB.IntegrationTests
{
    /// <summary>
    /// Summary description for DataAdapterTests.
    /// </summary>
    [TestFixture]
    public class DataAdapterTests : BaseTest
    {
        [SetUp]
        public void SetUp() => Init(
            "CREATE TABLE Test (id INT NOT NULL DEFAULT SERIAL, id2 INT NOT NULL UNIQUE, name VARCHAR(100), dt DATE, tm TIME, ts TIMESTAMP, OriginalId INT, PRIMARY KEY(id, id2))");

        [TearDown]
        public void TearDown() => Close();

        [Test]
        public void TestFill()
        {
            var cmd = new MaxDBCommand(string.Empty, mconn);
            cmd.Parameters.Add(new MaxDBParameter(":now", MaxDBType.Date)).Value = DateTime.Now;

            cmd.CommandText = "INSERT INTO Test (id2, name, dt) VALUES (1, 'Name 1', :now)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "INSERT INTO Test (id2, name, dt) VALUES (2, NULL, :now)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "INSERT INTO Test (id2, name, dt) VALUES (3, '', :now)";
            cmd.ExecuteNonQuery();

            using var da = new MaxDBDataAdapter("SELECT id, id2, name FROM Test", mconn);
            var ds = new DataSet();
            da.Fill(ds, "Test");

            ds.Tables.Count.Should().Be(1, "At least one table should be filled");
            ds.Tables[0].Rows.Count.Should().Be(3, "At least three rows should be inserted");

            ds.Tables[0].Rows[0]["id"].Should().Be(1, "id field of the first row");
            ds.Tables[0].Rows[1]["id"].Should().Be(2, "id field of the second row");
            ds.Tables[0].Rows[2]["id"].Should().Be(3, "id field of the third row");

            ds.Tables[0].Rows[0]["id2"].Should().Be(1, "id2 field of the first row");
            ds.Tables[0].Rows[1]["id2"].Should().Be(2, "id2 field of the second row");
            ds.Tables[0].Rows[2]["id2"].Should().Be(3, "id2 field of the third row");

            ds.Tables[0].Rows[0]["name"].Should().Be("Name 1", "name field of the first row");
            ds.Tables[0].Rows[1]["name"].Should().Be(DBNull.Value, "name field of the second row");
            ds.Tables[0].Rows[2]["name"].Should().Be(string.Empty, "name field of the third row");
        }

        [Test]
        public void TestUpdate()
        {
            using var da = new MaxDBDataAdapter("SELECT * FROM Test FOR UPDATE", mconn);
            var cb = new MaxDBCommandBuilder(da);
            var dt = new DataTable();
            da.Fill(dt);

            var dr = dt.NewRow();

            dr["id2"] = 2;
            dr["name"] = "TestName1";
            dt.Rows.Add(dr);

            da.Update(dt).Should().Be(1, "At least one row should be inserted");

            using (var cmd = new MaxDBCommand("SELECT MAX(id) FROM Test", mconn))
            {
                dr["id"] = cmd.ExecuteScalar();
            }

            // make sure our refresh of auto increment values worked
            dt.Rows[^1]["id"].Should().NotBe(DBNull.Value, "id field shouldn't be NULL");

            dt.Rows[0]["id2"] = 2;
            dt.Rows[0]["name"] = "TestName2";
            dt.Rows[0]["ts"] = DBNull.Value;
            var day1 = new DateTime(2003, 1, 16, 12, 24, 0);
            dt.Rows[0]["dt"] = day1;
            dt.Rows[0]["tm"] = new DateTime(DateTime.MinValue.Year, DateTime.MinValue.Month, DateTime.MinValue.Day,
                day1.TimeOfDay.Hours, day1.TimeOfDay.Minutes, day1.TimeOfDay.Seconds);
            da.Update(dt);

            dt.Rows[0]["ts"].Should().Be(DBNull.Value, "ts field should be NULL");
            dt.Rows[0]["id2"].Should().Be(2, "id2 field");

            dt.Rows.Clear();
            da.Fill(dt);

            var dateTime = (DateTime)dt.Rows[0]["dt"];
            day1.Date.Should().Be(dateTime, "dt field");
            day1.TimeOfDay.Should().Be(((DateTime)dt.Rows[0]["tm"]).TimeOfDay, "tm field");

            dt.Rows[0].Delete();
            da.Update(dt).Should().Be(1, "Table should contain at least one row");

            dt.Rows.Clear();
            da.Fill(dt);
            dt.Rows.Count.Should().Be(0, "Table should be empty");
        }

        [Test]
        public void TestOriginalId()
        {
            using var da = new MaxDBDataAdapter("SELECT * FROM Test FOR UPDATE", mconn);
            var cb = new MaxDBCommandBuilder(da);
            var dt = new DataTable();
            da.Fill(dt);

            var row = dt.NewRow();

            row["id2"] = 1;
            row["name"] = "Test";
            row["dt"] = DBNull.Value;
            row["tm"] = DBNull.Value;
            row["ts"] = DBNull.Value;
            row["OriginalId"] = 2;
            dt.Rows.Add(row);
            da.Update(dt);

            dt.Rows.Count.Should().Be(1, "Table should contain at least one row");
            dt.Rows[0]["OriginalId"].Should().Be(2, "OriginalId field");
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
                    using var da = new MaxDBDataAdapter("SELECT * FROM Test FOR UPDATE", mconn);
                    var cb = new MaxDBCommandBuilder(da);
                    var dt = new DataTable();
                    da.Fill(dt);

                    da.UpdateBatchSize = 0;

                    DataRow dr;

                    for (int i = 0; i < rowCount; i++)
                    {
                        dr = dt.NewRow();
                        dr["id2"] = i;
                        dt.Rows.Add(dr);
                    }

                    da.Update(dt.GetChanges());
                    dt.AcceptChanges();

                    dt.Clear();
                    da.Fill(dt);
                    rowCount.Should().Be(dt.Rows.Count, "Table row count");

                    for (int i = 0; i < rowCount; i++)
                    {
                        dt.Rows[i]["name"] = "Name " + i.ToString();
                    }

                    dr = dt.NewRow();
                    dr["id2"] = rowCount + 1;
                    dr["name"] = "Name " + rowCount.ToString();
                    dt.Rows.Add(dr);

                    da.UpdateBatchSize = rowCount / 15;
                    da.Update(dt.GetChanges());
                    dt.AcceptChanges();

                    dt.Clear();
                    da.Fill(dt);

                    rowCount.Should().Be(dt.Rows.Count - 1, "Table row count + 1");

                    for (int i = 0; i < rowCount + 1; i++)
                    {
                        dt.Rows[i]["name"].Should().Be($"Name {i}", $"Table row #{i + 1}");
                    }

                    trans.Commit();
                }
            }
            catch (Exception ex)
            {
                if (trans != null)
                {
                    trans.Rollback();
                }

                Assert.Fail(ex.Message);
            }
            finally
            {
                mconn.AutoCommit = true;
            }
        }
    }
}
