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
using System.Collections;
using System.Text;
using MaxDB.UnitTesting;
using NUnit.Framework;
using MaxDB.Data;
using System.Security.Cryptography;
using System.Data;

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

				SHA1 sha = new SHA1CryptoServiceProvider();

				cmd.CommandText = "SELECT blob1 FROM Test";

				try
				{
					using (MaxDBDataReader reader = cmd.ExecuteReader())
					{
						reader.Read();
						byte[] dataOut = new byte[len];
						long count = reader.GetBytes(0, 0, dataOut, 0, len);
						Assert.AreEqual(len, count);
						Assert.AreEqual(sha.ComputeHash(dataIn), sha.ComputeHash(dataOut));

						reader.Read();
						count = reader.GetBytes(0, 0, dataOut, 0, len);
						Assert.AreEqual(len, count);

						byte[] hashIn = sha.ComputeHash(dataIn2);
						byte[] hashOut = sha.ComputeHash(dataOut);

						bool isEqual = true;

						for (int i = 0; i < hashIn.Length; i++)
							if (hashIn[i] != hashOut[i])
							{
								isEqual = false;
								break;
							}

						if (!isEqual)
							for (int i = 0; i < len; i++)
								Assert.AreEqual(dataIn2[i], dataOut[i], "wrong blob value at position " + i.ToString());
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
			int[] id2_values = new int[count];
			int[] id_values = new int[count];

			ClearTestTable();

			using (MaxDBDataAdapter da = new MaxDBDataAdapter())
			{
				da.InsertCommand = new MaxDBCommand("INSERT INTO Test (id, name) VALUES (:id, 'test')", mconn);
				da.InsertCommand.Parameters.Add(new MaxDBParameter(":id", MaxDBType.Integer, "id"));

				DataTable dt = new DataTable();
				dt.Columns.Add(new DataColumn("id", typeof(int)));

				for (int i = 1; i <= count; i++)
				{
					DataRow row = dt.NewRow();
					row["id"] = i;
					dt.Rows.Add(row);
				}

				da.Update(dt);
			}

			using (MaxDBCommand cmd = new MaxDBCommand("SELECT * FROM Test", mconn))
			{
				int i2 = 0;

				try
				{
					using (MaxDBDataReader reader = cmd.ExecuteReader())
					{
						while (reader.Read())
						{
							id_values[i2] = i2 + 1;
							id2_values[i2] = reader.GetInt32(0);
							i2++;
						}

						Assert.AreEqual(count, i2);
						Assert.AreEqual(id_values, id2_values, "Sequence out of order");
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
