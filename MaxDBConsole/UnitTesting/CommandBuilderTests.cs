using System;
#if NET20
using System.Collections.Generic;
#endif // NET20
using System.Text;
using System.Data;
using NUnit.Framework;
using MaxDB.Data;

namespace MaxDB.UnitTesting
{
    [TestFixture]
    public class CommandBuilderTests : BaseTest
    {
        [TestFixtureSetUp]
        public void SetUp()
        {
            Init("CREATE TABLE Test (id INT NOT NULL DEFAULT SERIAL, id2 INT NOT NULL UNIQUE, name VARCHAR(100), tm TIME, PRIMARY KEY(id, id2))");
        }

        [TestFixtureTearDown]
        public void TearDown()
        {
            Close();
        }

        [Test]
        public void GetInsertCommandTest()
        {
            try
            {
                using (MaxDBDataAdapter da = new MaxDBDataAdapter("SELECT id, id2, name, tm FROM Test FOR UPDATE", mconn))
                {
                    MaxDBCommandBuilder cb = new MaxDBCommandBuilder(da);
                    cb.GetType(); //add this command since mcs compiler supposes that cb is never used
                    DataSet ds = new DataSet();
                    da.Fill(ds);
                    Assert.AreEqual(1, ds.Tables.Count, "At least one table should be filled");

					Assert.AreEqual("INSERT INTO Test(id2, name, tm) VALUES(:id2, :name, :tm)".ToUpper(),
						cb.GetInsertCommand().CommandText.ToUpper(), "GetInsertCommand method returns wrong SQL"); // serial column is skipped
                }
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        [Test]
        public void CommandParameterTest()
        {
            try
            {
                using (MaxDBDataAdapter da = new MaxDBDataAdapter("SELECT id, id2, name, tm FROM Test FOR UPDATE", mconn))
                {
                    MaxDBCommandBuilder cb = new MaxDBCommandBuilder(da);
                    cb.GetType(); //add this command since mcs compiler supposes that cb is never used
                    DataSet ds = new DataSet();
                    da.Fill(ds);
                    Assert.AreEqual(1, ds.Tables.Count, "At least one table should be filled");

                    MaxDBParameterCollection collect =  cb.GetInsertCommand().Parameters;

                    Assert.AreEqual(3, collect.Count, "GetInsertCommand method returns command with wrong number of parameters");  // serial column is skipped
					Assert.AreEqual("id2".ToUpper(), collect[0].ParameterName.ToUpper(), "First parameter");
					Assert.AreEqual(DbType.Int32, collect[0].DbType, "Wrong type of the first parameter");
					Assert.AreEqual("name".ToUpper(), collect[1].ParameterName.ToUpper(), "Second parameter");
					Assert.AreEqual(DbType.AnsiString, collect[1].DbType, "Wrong type of the second parameter");
					Assert.AreEqual("tm".ToUpper(), collect[2].ParameterName.ToUpper(), "Third parameter");
					Assert.AreEqual(DbType.Time, collect[2].DbType, "Wrong type of the third parameter");
                }
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        [Test]
        public void GetUpdateCommandTest()
        {
            try
            {
                using (MaxDBDataAdapter da = new MaxDBDataAdapter("SELECT id, id2, name, tm FROM Test FOR UPDATE", mconn))
                {
                    MaxDBCommandBuilder cb = new MaxDBCommandBuilder(da);
                    cb.GetType(); //add this command since mcs compiler supposes that cb is never used
                    DataSet ds = new DataSet();
                    da.Fill(ds);
                    Assert.AreEqual(1, ds.Tables.Count, "At least one table should be filled");

					Assert.AreEqual("UPDATE Test SET name = :name, tm = :tm WHERE id = :id AND id2 = :id2".ToUpper(),
						cb.GetUpdateCommand().CommandText.ToUpper(), "GetUpdateCommand method returns wrong SQL");
                }
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        [Test]
        public void GetDeleteCommandTest()
        {
            try
            {
                using (MaxDBDataAdapter da = new MaxDBDataAdapter("SELECT id, id2, name, tm FROM Test FOR UPDATE", mconn))
                {
                    MaxDBCommandBuilder cb = new MaxDBCommandBuilder(da);
                    cb.GetType(); //add this command since mcs compiler supposes that cb is never used
                    DataSet ds = new DataSet();
                    da.Fill(ds);
                    Assert.AreEqual(1, ds.Tables.Count, "At least one table should be filled");

					Assert.AreEqual("DELETE FROM Test WHERE id = :id AND id2 = :id2".ToUpper(),
						cb.GetDeleteCommand().CommandText.ToUpper(), "GetDeleteCommand method returns wrong SQL");
                }
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }
    }
}
