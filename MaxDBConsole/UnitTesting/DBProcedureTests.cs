//-----------------------------------------------------------------------------------------------
// <copyright file="DBProcedureTests.cs" company="Dmitry S. Kataev">
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
using System.Data;
using NUnit.Framework;
using MaxDB.Data;

namespace MaxDB.UnitTesting
{
    [TestFixture()]
    public class DBProcedureTests : BaseTest
    {
        [SetUp]
        public void SetUp()
        {
            Init("CREATE TABLE Test (id int NOT NULL, name VARCHAR(100))");
        }

        [TearDown]
        public void TearDown()
        {
            Close();
        }

        [Test]
        public void TestCursorProcedure()
        {
            try
            {
                DropDbProcedure("spTest");

                ExecuteNonQuery("CREATE DBPROC spTest(IN val decimal(10,3)) " +
                                "RETURNS CURSOR AS " +
                                "$CURSOR = 'TEST_CURSOR'; " +
                                "DECLARE :$CURSOR CURSOR FOR " +
                                "SELECT :val, :val*1000 FROM DUAL;");

                ClearTestTable();

                //setup testing data
                using (var cmd = new MaxDBCommand("CALL spTest(:val)", mconn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    var p = cmd.Parameters.Add(":val", MaxDBType.Number);
                    p.Precision = 10;
                    p.Scale = 3;
                    p.Value = 123.334;

                    using (var adapter = new MaxDBDataAdapter())
                    {
                        adapter.SelectCommand = cmd;

                        DataSet dataSet = new DataSet();
                        adapter.Fill(dataSet);
                        Assert.AreEqual(dataSet.Tables.Count, 1, "there must be a table");
                        Assert.AreEqual(dataSet.Tables[0].Rows.Count, 1, "there must be one row");
                        Assert.AreEqual(dataSet.Tables[0].Columns.Count, 2, "there must be 2 columns");
                        Assert.AreEqual(p.Value, Convert.ToDouble(dataSet.Tables[0].Rows[0].ItemArray[0]), "wrong decimal value of the first column");
                        Assert.AreEqual((double)p.Value * 1000, Convert.ToDouble(dataSet.Tables[0].Rows[0].ItemArray[1]), "wrong decimal value of the second column");
                    }

                    using (var reader = cmd.ExecuteReader())
                    {
                        Assert.IsTrue(reader.Read(), "data reader shouldn't be empty");
                        Assert.AreEqual(p.Value, reader.GetDecimal(0), "wrong decimal value of the first column");
                        Assert.AreEqual((double)p.Value * 1000, reader.GetDecimal(1), "wrong decimal value of the second column");
                        Assert.IsFalse(reader.Read(), "data reader should contain single row");
                    }
                }

                DropDbProcedure("spTest");
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        [Test]
        public void TestNonQueryProcedure()
        {
            try
            {
                DropDbProcedure("spTest");

                string mSchema = (new MaxDBConnectionStringBuilder(mconn.ConnectionString)).UserId;

                ExecuteNonQuery("CREATE DBPROC spTest(IN val INTEGER) AS INSERT INTO " + mSchema + ".Test VALUES(:val, 'Test');");

                ClearTestTable();
                //setup testing data
                using (var cmd = new MaxDBCommand("CALL spTest(:value)", mconn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add(":value", 2);
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "SELECT * FROM Test";
                    cmd.CommandType = CommandType.Text;

                    using (var reader = cmd.ExecuteReader())
                    {
                        Assert.IsTrue(reader.Read(), "data reader shouldn't be empty");
                        Assert.AreEqual(2, reader.GetInt32(0), "wrong integer value of the first column");
                        Assert.AreEqual("Test", reader.GetString(1), "wrong integer value of the second column");
                        Assert.IsFalse(reader.Read(), "data reader should contain single row");
                    }
                }

                DropDbProcedure("spTest");
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        [Test]
        public void TestOutputParameters()
        {
            try
            {
                // create our procedure 
                DropDbProcedure("spTest");
                ExecuteNonQuery("CREATE DBPROC spTest(OUT charVal VARCHAR(10), OUT intVal INT, OUT dateVal TIMESTAMP, OUT floatVal FLOAT) AS " +
                    "charVal='42'; intVal=33; dateVal='2004-06-05 07:58:09'; floatVal = 1.2;");

                using (var cmd = new MaxDBCommand("CALL spTest(:charVal, :intVal, :dateVal, :floatVal)", mconn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add(new MaxDBParameter(":charVal", MaxDBType.VarCharA));
                    cmd.Parameters.Add(new MaxDBParameter(":intVal", MaxDBType.Integer));
                    cmd.Parameters.Add(new MaxDBParameter(":dateVal", MaxDBType.Timestamp));
                    cmd.Parameters.Add(new MaxDBParameter(":floatVal", MaxDBType.Float));
                    cmd.Parameters[0].Direction = ParameterDirection.Output;
                    cmd.Parameters[1].Direction = ParameterDirection.Output;
                    cmd.Parameters[2].Direction = ParameterDirection.Output;
                    cmd.Parameters[3].Direction = ParameterDirection.Output;

                    cmd.Parameters[2].Value = DateTime.Now;

                    cmd.ExecuteNonQuery();

                    Assert.AreEqual("42", cmd.Parameters[0].Value.ToString().Trim(), "wrong value of the first parameter");
                    Assert.AreEqual(33, cmd.Parameters[1].Value, "wrong value of the second parameter");
                    Assert.AreEqual(new DateTime(2004, 6, 5, 7, 58, 9), cmd.Parameters[2].Value, "wrong value of the third parameter");
                    Assert.AreEqual(1.2, cmd.Parameters[3].Value, "wrong value of the fourth parameter");
                }

                DropDbProcedure("spTest");
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        [Test]
        public void TestInputOutputParameters()
        {
            // create our procedure
            try
            {
                DropDbProcedure("spTest");
                ExecuteNonQuery("CREATE DBPROC spTest(INOUT strVal VARCHAR(50), INOUT numVal INTEGER, OUT outVal INTEGER) AS " +
                    "strVal = strVal || 'ending'; numVal = numVal * 2; outVal = 99;");

                using (var cmd = new MaxDBCommand("CALL spTest(:strVal, :numVal, :outVal)", mconn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    //cmd.Parameters.Add(":strVal", "beginning");
                    cmd.Parameters.Add(new MaxDBParameter(":strVal", MaxDBType.LongUni)).Value = "beginning";
                    cmd.Parameters.Add(":numVal", 33);
                    cmd.Parameters.Add(":outVal", MaxDBType.Integer);
                    cmd.Parameters[0].Direction = ParameterDirection.InputOutput;
                    cmd.Parameters[1].Direction = ParameterDirection.InputOutput;
                    cmd.Parameters[2].Direction = ParameterDirection.Output;
                    cmd.ExecuteNonQuery();
                    Assert.AreEqual("beginningending", cmd.Parameters[0].Value.ToString().Trim(), "wrong value of the first parameter");
                    Assert.AreEqual(66, cmd.Parameters[1].Value, "wrong value of the second parameter");
                    Assert.AreEqual(99, cmd.Parameters[2].Value, "wrong value of the third parameter");
                }
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }


        [Test]
        public void TestDbFunction()
        {
            try
            {
                DropDbFunction("fnTest");

                ExecuteNonQuery("CREATE FUNCTION fnTest(valuein VARCHAR) RETURNS VARCHAR AS RETURN valuein;");

                using (var cmd = new MaxDBCommand("SELECT fnTest(:valuein) FROM DUAL", mconn))
                {
                    cmd.Parameters.Add(":valuein", "Test");
                    Assert.AreEqual(cmd.Parameters[0].Value, cmd.ExecuteScalar().ToString().Trim(), "wrong function result");
                }

                DropDbFunction("fnTest");
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }
    }
}
