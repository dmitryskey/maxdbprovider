//-----------------------------------------------------------------------------------------------
// <copyright file="CommandTests.cs" company="Dmitry S. Kataev">
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

using FluentAssertions;
using MaxDB.Data;
using NUnit.Framework;
using System;
using System.Data;
using System.Threading;

namespace MaxDB.IntegrationTests
{
    /// <summary>
    /// Summary description for CommandTests.
    /// </summary>
    [TestFixture]
    public class CommandTests : BaseTest
    {
        [SetUp]
        public void SetUp() => Init("CREATE TABLE Test (id int NOT NULL, name VARCHAR(100))");

        [TearDown]
        public void TearDown() => Close();

        [Test]
        public void TestInsert()
        {
            // do the insert
            using var cmd = new MaxDBCommand("INSERT INTO Test (id, name) VALUES(10, 'Test')", mconn);
            int cnt = cmd.ExecuteNonQuery();
            cnt.Should().Be(1, "Insert Count");

            // make sure we get the right value back out
            cmd.CommandText = "SELECT name FROM Test WHERE id = 10";
            cmd.ExecuteScalar().Should().Be("Test", "Insert result");

            // now do the insert with parameters
            cmd.CommandText = "INSERT INTO Test (id, name) VALUES( :id, :name)";
            cmd.Parameters.Add(new MaxDBParameter(":id", 11));
            cmd.Parameters.Add(new MaxDBParameter(":name", "Test2"));
            cmd.ExecuteNonQuery().Should().Be(1, "Insert with Parameters Count");

            // make sure we get the right value back out
            cmd.Parameters.Clear();
            cmd.CommandText = "SELECT name FROM Test WHERE id = 11";
            cmd.ExecuteScalar().Should().Be("Test2", "Insert with parameters result");
        }

        [Test]
        public void TestUpdate()
        {
            ExecuteNonQuery("INSERT INTO Test (id, name) VALUES(10, 'Test')");
            ExecuteNonQuery("INSERT INTO Test (id,name) VALUES(11, 'Test2')");

            // do the update
            using var cmd = new MaxDBCommand("UPDATE Test SET name='Test3' WHERE id = 10 OR id = 11", mconn);
            cmd.Connection.Should().Be(mconn);
            cmd.ExecuteNonQuery().Should().Be(2);

            // make sure we get the right value back out
            cmd.CommandText = "SELECT name FROM Test WHERE id = 10";
            cmd.ExecuteScalar().Should().Be("Test3", "Update result for id = 10");

            cmd.CommandText = "SELECT name FROM Test WHERE id = 11";
            cmd.ExecuteScalar().Should().Be("Test3", "Update result for id = 11");

            // now do the update with parameters
            cmd.CommandText = "UPDATE Test SET name = :name WHERE id = :id";
            cmd.Parameters.Add(new MaxDBParameter(":name", "Test5"));
            cmd.Parameters.Add(new MaxDBParameter(":id", 11));
            cmd.ExecuteNonQuery().Should().Be(1, "Update with Parameters Count");

            // make sure we get the right value back out
            cmd.Parameters.Clear();
            cmd.CommandText = "SELECT name FROM Test WHERE id = 11";
            cmd.ExecuteScalar().Should().Be("Test5", "Update with Parameters result");
        }

        [Test]
        public void TestDelete()
        {
            ExecuteNonQuery("INSERT INTO Test (id, name) VALUES(1, 'Test')");
            ExecuteNonQuery("INSERT INTO Test (id, name) VALUES(2, 'Test2')");

            using var cmd = new MaxDBCommand("DELETE FROM Test WHERE id = 1 or id = 2", mconn);
            cmd.ExecuteNonQuery().Should().Be(2);

            // find out how many rows we have now
            cmd.CommandText = "SELECT COUNT(*) FROM Test";
            cmd.ExecuteScalar().Should().Be(0, "Delete all count");
        }

        [Test]
        public void TestInsertNullParameter()
        {
            using var cmd = new MaxDBCommand("INSERT INTO test VALUES(1, :str)", mconn);
            cmd.Parameters.Add(":str", MaxDBType.VarCharA);
            cmd.Parameters[0].Value = null;
            cmd.ExecuteNonQuery();

            cmd.CommandText = "SELECT * FROM test";

            using var reader = cmd.ExecuteReader();
            reader.Read().Should().BeTrue("Read first row");
            reader[1].Should().Be(DBNull.Value, "Check whether column is NULL or not");
        }

        [Test]
        public void TestInsertUsingReader()
        {
            using var cmd = new MaxDBCommand("INSERT INTO Test VALUES(1, 'Test')", mconn);
            cmd.ExecuteNonQuery();

            cmd.CommandText = "SELECT * FROM Test";
            using var reader = cmd.ExecuteReader();
            reader.Read().Should().BeTrue("Read first row");
            reader.Read().Should().BeFalse("Only one row in the result set");
            reader.NextResult().Should().BeFalse("No next result");
        }

        [Test]
        public void TestOracleMode()
        {
            var oldMode = mconn.SqlMode;
            try
            {
                mconn.SqlMode = SqlMode.Oracle;
                using (var cmd = new MaxDBCommand("SELECT sysdate FROM DUAL", mconn))
                {
                    DateTime.Now.Subtract(DateTime.Parse(cmd.ExecuteScalar().ToString())).TotalSeconds
                        .Should().BeLessThan(10, "Oracle returned bad time " + cmd.ExecuteScalar().ToString());
                }

                using (var cmd = new MaxDBCommand("SELECT sysdate FROM DUAL FOR UPDATE", mconn))
                {
                    var reader = cmd.ExecuteReader();
                    reader.GetSchemaTable().TableName.Should().Be("SchemaTable", "Schema Table");
                    reader.Close();
                }
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
            finally
            {
                mconn.SqlMode = oldMode;
            }
        }

        [Test]
        public void TestTransaction()
        {
            MaxDBTransaction trans = null;
            try
            {
                mconn.AutoCommit = false;

                using var cnt_cmd = new MaxDBCommand("SELECT count(*) FROM Test", mconn);
                using (trans = mconn.BeginTransaction())
                {
                    using (var cmd = new MaxDBCommand("INSERT INTO Test VALUES(1, 'Test1')", mconn, trans))
                    {
                        cmd.ExecuteNonQuery();
                        trans.Commit();
                    }

                    cnt_cmd.ExecuteScalar().Should().Be(1, "Insert count after commit");

                    trans = mconn.BeginTransaction();

                    using (var cmd = new MaxDBCommand("INSERT INTO Test VALUES(2, 'Test2')", mconn, trans))
                    {
                        cmd.ExecuteNonQuery();
                        trans.Rollback();
                    }
                }

                cnt_cmd.ExecuteScalar().Should().Be(1, "Insert count after rollback");
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

        [Test]
        public void TestCloneCommand()
        {
            using var txn = mconn.BeginTransaction();
            using var cmd = new MaxDBCommand("SELECT * FROM Test WHERE id = :id", mconn, txn);
            cmd.Parameters.Add(":test", 1);

            var cmd2 = ((ICloneable)cmd).Clone() as IDbCommand;
            cmd2.CommandText.Should().BeEquivalentTo(cmd.CommandText);
            cmd2.Parameters.Should().BeEquivalentTo(cmd.Parameters);
            txn.Rollback();
        }

        private MaxDBCommand mCmd;

        [Test]
        public void TestCancel()
        {
            var mConnStrBuilder = new MaxDBConnectionStringBuilder(mconn.ConnectionString);

            // we use db procedure in order to insert rows as fast as possible
            string dbProc = $@"CREATE DBPROC InsertManyRows (IN cnt INTEGER) AS
                VAR i INTEGER;
                TRY
                  SET i = 1;
                  WHILE $rc = 0 AND i <= cnt DO BEGIN
                      INSERT INTO {mConnStrBuilder.UserId}.TEST (id, name) VALUES (:i, '*');
                      SET i = i + 1;
                  END;
                CATCH
                  IF $rc <> 100 THEN STOP ($rc, 'unexpected error');";

            try
            {
                ExecuteNonQuery(dbProc);
            }
            catch (MaxDBException ex)
            {
                // Ignore duplicated DB Proc error -6006
                if (ex.ErrorCode != -6006)
                {
                    Assert.Fail(ex.Message);
                }
            }

            ExecuteNonQuery("CALL InsertManyRows(128000)");

            using (mCmd = new MaxDBCommand($"UPDATE test SET name = '{DateTime.Now}'", mconn))
            {
                new Thread(() =>
                {
                    Thread.Sleep(100);
                    mCmd.Cancel();
                }).Start();

                try
                {
                    mCmd.ExecuteNonQuery();

                    Assert.Fail("Execution should not have finished.");
                }
                catch (MaxDBException ex)
                {
                    if (ex.ErrorCode != -102)
                    {
                        Assert.Fail(ex.Message);
                    }
                }
            }

            DropDbProcedure("InsertManyRows");
        }
    }
}
