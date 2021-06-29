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
using FluentAssertions;
using MaxDB.Data;
using NUnit.Framework;

namespace MaxDB.IntegrationTests
{
    [TestFixture()]
    public class DBProcedureTests : BaseTest
    {
        [SetUp]
        public void SetUp() => Init("CREATE TABLE Test (id int NOT NULL, name VARCHAR(100))");

        [TearDown]
        public void TearDown() => Close();

        [Test]
        public void TestCursorProcedure()
        {
            DropDbProcedure("spTest");

            ExecuteNonQuery(@"CREATE DBPROC spTest(IN val decimal(10,3)) RETURNS CURSOR AS $CURSOR = 'TEST_CURSOR'; 
                              DECLARE :$CURSOR CURSOR FOR SELECT :val, :val * 1000 FROM DUAL;");

            // setup testing data
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

                    var dataSet = new DataSet();
                    adapter.Fill(dataSet);
                    dataSet.Tables.Count.Should().Be(1, "there must be a table");
                    dataSet.Tables[0].Rows.Count.Should().Be(1, "there must be one row");
                    dataSet.Tables[0].Columns.Count.Should().Be(2, "there must be 2 columns");
                    p.Value.Should().Be(Convert.ToDouble(dataSet.Tables[0].Rows[0].ItemArray[0]), "wrong decimal value of the first column");
                    ((double)p.Value * 1000).Should().Be(Convert.ToDouble(dataSet.Tables[0].Rows[0].ItemArray[1]), "wrong decimal value of the second column");
                }

                using var reader = cmd.ExecuteReader();
                reader.Read().Should().BeTrue("data reader shouldn't be empty");
                p.Value.Should().Be(reader.GetDecimal(0), "wrong decimal value of the first column");
                ((double)p.Value * 1000).Should().Be((double)reader.GetDecimal(1), "wrong decimal value of the second column");
                reader.Read().Should().BeFalse("data reader should contain single row");
            }

            DropDbProcedure("spTest");
        }

        [Test]
        public void TestNonQueryProcedure()
        {
            DropDbProcedure("spTest");

            string mSchema = new MaxDBConnectionStringBuilder(mconn.ConnectionString).UserId;

            ExecuteNonQuery("CREATE DBPROC spTest(IN val INTEGER) AS INSERT INTO " + mSchema + ".Test VALUES(:val, 'Test');");

            // setup testing data
            using (var cmd = new MaxDBCommand("CALL spTest(:value)", mconn))
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.Add(":value", 2);
                cmd.ExecuteNonQuery();

                cmd.CommandText = "SELECT * FROM Test";
                cmd.CommandType = CommandType.Text;

                using var reader = cmd.ExecuteReader();
                reader.Read().Should().BeTrue("data reader shouldn't be empty");
                reader.GetInt32(0).Should().Be(2, "wrong integer value of the first column");
                reader.GetString(1).Should().Be("Test", "wrong integer value of the second column");
                reader.Read().Should().BeFalse("data reader should contain single row");
            }

            DropDbProcedure("spTest");
        }

        [Test]
        public void TestOutputParameters()
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

                cmd.Parameters[0].Value.ToString().Trim().Should().Be("42", "wrong value of the first parameter");
                cmd.Parameters[1].Value.Should().Be(33, "wrong value of the second parameter");
                cmd.Parameters[2].Value.Should().Be(new DateTime(2004, 6, 5, 7, 58, 9), "wrong value of the third parameter");
                cmd.Parameters[3].Value.Should().Be(1.2, "wrong value of the fourth parameter");
            }

            DropDbProcedure("spTest");
        }

        [Test]
        public void TestInputOutputParameters()
        {
            // create our procedure
            DropDbProcedure("spTest");
            ExecuteNonQuery(@"CREATE DBPROC spTest(INOUT strVal VARCHAR(50), INOUT numVal INTEGER, OUT outVal INTEGER) AS
                strVal = strVal || 'ending'; numVal = numVal * 2; outVal = 99;");

            using var cmd = new MaxDBCommand("CALL spTest(:strVal, :numVal, :outVal)", mconn);
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.Add(new MaxDBParameter(":strVal", MaxDBType.LongUni)).Value = "beginning";
            cmd.Parameters.Add(":numVal", 33);
            cmd.Parameters.Add(":outVal", MaxDBType.Integer);
            cmd.Parameters[0].Direction = ParameterDirection.InputOutput;
            cmd.Parameters[1].Direction = ParameterDirection.InputOutput;
            cmd.Parameters[2].Direction = ParameterDirection.Output;
            cmd.ExecuteNonQuery();
            cmd.Parameters[0].Value.ToString().Trim().Should().Be("beginningending", "wrong value of the first parameter");
            cmd.Parameters[1].Value.Should().Be(66, "wrong value of the second parameter");
            cmd.Parameters[2].Value.Should().Be(99, "wrong value of the third parameter");
        }

        [Test]
        public void TestDbFunction()
        {
            DropDbFunction("fnTest");

            ExecuteNonQuery("CREATE FUNCTION fnTest(valuein VARCHAR) RETURNS VARCHAR AS RETURN valuein;");

            using (var cmd = new MaxDBCommand("SELECT fnTest(:valuein) FROM DUAL", mconn))
            {
                cmd.Parameters.Add(":valuein", "Test");

                // by some reason reply package returns the value 38 for the string length
                // As result we have a garbage in the tail of the string
                cmd.ExecuteScalar().ToString().Should().StartWith(cmd.Parameters[0].Value.ToString(), "wrong function result");
            }

            DropDbFunction("fnTest");

        }
    }
}
