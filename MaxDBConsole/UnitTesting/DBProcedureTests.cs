// Copyright (C) 2005-2006 Dmitry S. Kataev
// Copyright (C) 2004-2005 MySQL AB
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License version 2 as published by
// the Free Software Foundation
//
// Copyright (C) 2005-2006 Dmitry S. Kataev
// There are special exceptions to the terms and conditions of the GPL 
// as it is applied to this software. View the full text of the 
// exception in file EXCEPTIONS in the directory of this software 
// distribution.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

using System;
using System.Collections;
using System.Text;
using System.Data;
using NUnit.Framework;
using MaxDB.Data;

namespace MaxDB.UnitTesting
{
    [TestFixture()]
    public class DBProcedureTests : BaseTest
	{
        [TestFixtureSetUp]
        public void SetUp()
        {
            Init("CREATE TABLE Test (id int NOT NULL, name VARCHAR(100))");
        }

        [TestFixtureTearDown]
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
                                "SELECT :val, :val*1000 FROM dba.dual;");

                ClearTestTable();
                //setup testing data
                using (MaxDBCommand cmd = new MaxDBCommand("CALL spTest(:val)", mconn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    MaxDBParameter p = cmd.Parameters.Add(":val", MaxDBType.Number);
                    p.Precision = 10;
                    p.Scale = 3;
                    p.Value = 123.334;

                    using (MaxDBDataReader reader = cmd.ExecuteReader())
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

				MaxDBConnectionStringBuilder builder = new MaxDBConnectionStringBuilder(mconn.ConnectionString);

				ExecuteNonQuery("CREATE DBPROC spTest(IN val INTEGER) AS INSERT INTO " + builder.UserId + ".Test VALUES(:val, 'Test');");

                ClearTestTable();
                //setup testing data
                using (MaxDBCommand cmd = new MaxDBCommand("CALL spTest(:value)", mconn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.Add(":value", 2);
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "SELECT * FROM Test";
                    cmd.CommandType = CommandType.Text;

                    using (MaxDBDataReader reader = cmd.ExecuteReader())
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

                using (MaxDBCommand cmd = new MaxDBCommand("CALL spTest(:charVal, :intVal, :dateVal, :floatVal)", mconn))
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

                using (MaxDBCommand cmd = new MaxDBCommand("CALL spTest(:strVal, :numVal, :outVal)", mconn))
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

                using (MaxDBCommand cmd = new MaxDBCommand("SELECT fnTest(:valuein) FROM DUAL", mconn))
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
