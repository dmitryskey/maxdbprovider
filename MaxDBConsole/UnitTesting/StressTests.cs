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
using MaxDB.UnitTesting;
using NUnit.Framework;
using MaxDB.Data;

namespace MaxDB.UnitTesting
{
    [TestFixture]
    public class StressTests : BaseTest
    {
        [TestFixtureSetUp]
        public void SetUp()
        {
            Init("CREATE TABLE Test (id INT NOT NULL, name varchar(100), blob1 LONG BYTE, text1 LONG ASCII, PRIMARY KEY(id))");
        }

        [TestFixtureTearDown]
        public void TearDown()
        {
            Close();
        }

        [Test]
        public void TestMultiPacket()
        {
            const int len = 20000000;

            byte[] dataIn = CreateBlob(len);
            byte[] dataIn2 = CreateBlob(len);

            ClearTestTable();

            using (MaxDBCommand cmd = new MaxDBCommand("INSERT INTO Test VALUES (:id, NULL, :blob, NULL)", mconn))
            {
                cmd.Parameters.Add(new MaxDBParameter(":id", 1));
                cmd.Parameters.Add(new MaxDBParameter(":blob", dataIn));
                try
                {
                    cmd.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Assert.Fail(ex.Message);
                }

                cmd.Parameters[0].Value = 2;
                cmd.Parameters[1].Value = dataIn2;
                cmd.ExecuteNonQuery();

                cmd.CommandText = "SELECT * FROM Test";
                try
                {
                    using (MaxDBDataReader reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        byte[] dataOut = new byte[len];
                        long count = reader.GetBytes(2, 0, dataOut, 0, len);
                        Assert.AreEqual(len, count);
                        Assert.AreEqual(dataIn, dataOut);

                        reader.Read();
                        count = reader.GetBytes(2, 0, dataOut, 0, len);
                        Assert.AreEqual(len, count);
                        Assert.AreEqual(dataIn2, dataOut);
                    }
                }
                catch (Exception ex)
                {
                    Assert.Fail(ex.Message);
                }
            }
        }

        [Test]
        public void TestSequence()
        {
            const int count = 8000;

            ClearTestTable();

            using (MaxDBCommand cmd = new MaxDBCommand("INSERT INTO Test (id, name) VALUES (:id, 'test')", mconn))
            {
                cmd.Parameters.Add(new MaxDBParameter(":id", 1));

                for (int i = 1; i <= count; i++)
                {
                    cmd.Parameters[0].Value = i;
                    cmd.ExecuteNonQuery();
                }

                int i2 = 0;
                cmd.CommandText = "SELECT * FROM Test";

                try
                {
                    using (MaxDBDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            Assert.AreEqual(i2 + 1, reader.GetInt32(0), "Sequence out of order");
                            i2++;
                        }

                        Assert.AreEqual(count, i2);
                    }
                }
                catch (Exception ex)
                {
                    Assert.Fail(ex.Message);
                }
            }
        }

    }
}
