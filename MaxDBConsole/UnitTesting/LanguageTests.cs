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
using System.Globalization;
using System.Threading;
using NUnit.Framework;
using MaxDB.Data;
using System.Text;

namespace MaxDB.UnitTesting
{
	[TestFixture()]
	public class LanguageTests : BaseTest
	{
		[TestFixtureSetUp]
		public void SetUp()
		{
			try
			{
				Init("CREATE TABLE Test (name VARCHAR(255) UNICODE, fl FLOAT, db DOUBLE PRECISION, decim DECIMAL(5,2), d DATE, t TIME, dt TIMESTAMP)");
			}
			catch
			{
				if (mconn.DatabaseEncoding != Encoding.Unicode)
				{
					Close();
					Assert.Ignore("Non-unicode database");
				}

				throw;
			}
		}

		[TestFixtureTearDown]
		public void TearDown()
		{
			Close();
		}

		[Test]
		public void TestUnicodeStatement()
		{
			if (mconn.DatabaseEncoding != Encoding.Unicode)
				Assert.Ignore("Non-unicode database");

			ClearTestTable();

			ExecuteNonQuery("INSERT INTO Test (name) VALUES ('abcАБВ')"); // Russian
			ExecuteNonQuery("INSERT INTO Test (name) VALUES ('兣冘凥凷冋')"); // simplified Chinese
			ExecuteNonQuery("INSERT INTO Test (name) VALUES ('困巫忘否役')"); // traditional Chinese
			ExecuteNonQuery("INSERT INTO Test (name) VALUES ('ئابةتثجح')"); // Arabian
			ExecuteNonQuery("INSERT INTO Test (name) VALUES ('涯割晦叶角')"); // Japanese
			ExecuteNonQuery("INSERT INTO Test (name) VALUES ('ברחפע')"); // Hebrew
			ExecuteNonQuery("INSERT INTO Test (name) VALUES ('ψόβΩΞ')"); // Greek
			ExecuteNonQuery("INSERT INTO Test (name) VALUES ('þðüçöÝÞÐÜÇÖ')"); // Turkish
			ExecuteNonQuery("INSERT INTO Test (name) VALUES ('ฅๆษ')"); // Thai

			using (MaxDBCommand cmd = new MaxDBCommand("SELECT * FROM Test", mconn))
			using (MaxDBDataReader reader = cmd.ExecuteReader())
			{
				try
				{
					reader.Read();
					Assert.AreEqual("abcАБВ", reader.GetString(0), "wrong Russian string");
					reader.Read();
					Assert.AreEqual("兣冘凥凷冋", reader.GetString(0), "wrong simplified Chinese string");
					reader.Read();
					Assert.AreEqual("困巫忘否役", reader.GetString(0), "wrong traditional Chinese string");
					reader.Read();
					Assert.AreEqual("ئابةتثجح", reader.GetString(0), "wrong Arabian string");
					reader.Read();
					Assert.AreEqual("涯割晦叶角", reader.GetString(0), "wrong Japanese string");
					reader.Read();
					Assert.AreEqual("ברחפע", reader.GetString(0), "wrong Hebrew string");
					reader.Read();
					Assert.AreEqual("ψόβΩΞ", reader.GetString(0), "wrong Greek string");
					reader.Read();
					Assert.AreEqual("þðüçöÝÞÐÜÇÖ", reader.GetString(0), "wrong Turkish string");
					reader.Read();
					Assert.AreEqual("ฅๆษ", reader.GetString(0), "wrong Thai string");
				}
				catch (Exception ex)
				{
					Assert.Fail(ex.Message);
				}
				finally
				{
					if (reader != null) reader.Close();
				}
			}
		}

		[Test]
		public void TestUnicodeParameter()
		{
			if (mconn.DatabaseEncoding != Encoding.Unicode)
			{
			    Assert.Ignore("Non-unicode database");
			}

			ClearTestTable();

			using (MaxDBCommand cmd = new MaxDBCommand("INSERT INTO Test (name) VALUES (:a)", mconn))
			{
				cmd.Parameters.Add(new MaxDBParameter(":a", MaxDBType.VarCharUni));

				cmd.Parameters[0].Value = "abcАБВ"; // Russian
				cmd.ExecuteNonQuery();

				cmd.Parameters[0].Value = "兣冘凥凷冋"; // simplified Chinese
				cmd.ExecuteNonQuery();

				cmd.Parameters[0].Value = "困巫忘否役"; // traditional Chinese
				cmd.ExecuteNonQuery();

				cmd.Parameters[0].Value = "ئابةتثجح"; //Arabian
				cmd.ExecuteNonQuery();

				cmd.Parameters[0].Value = "涯割晦叶角"; // Japanese
				cmd.ExecuteNonQuery();

				cmd.Parameters[0].Value = "ברחפע"; // Hebrew
				cmd.ExecuteNonQuery();

				cmd.Parameters[0].Value = "ψόβΩΞ"; // Greek
				cmd.ExecuteNonQuery();

				cmd.Parameters[0].Value = "þðüçöÝÞÐÜÇÖ"; // Turkish
				cmd.ExecuteNonQuery();

				cmd.Parameters[0].Value = "ฅๆษ"; // Thai
				cmd.ExecuteNonQuery();
			}

			using (MaxDBCommand cmd = new MaxDBCommand("SELECT * FROM Test", mconn))
			using (MaxDBDataReader reader = cmd.ExecuteReader())
			{
				try
				{
					reader.Read();
					Assert.AreEqual("abcАБВ", reader.GetString(0), "wrong Russian string");
					reader.Read();
					Assert.AreEqual("兣冘凥凷冋", reader.GetString(0), "wrong simplified Chinese string");
					reader.Read();
					Assert.AreEqual("困巫忘否役", reader.GetString(0), "wrong traditional Chinese string");
					reader.Read();
					Assert.AreEqual("ئابةتثجح", reader.GetString(0), "wrong Arabian string");
					reader.Read();
					Assert.AreEqual("涯割晦叶角", reader.GetString(0), "wrong Japanese string");
					reader.Read();
					Assert.AreEqual("ברחפע", reader.GetString(0), "wrong Hebrew string");
					reader.Read();
					Assert.AreEqual("ψόβΩΞ", reader.GetString(0), "wrong Greek string");
					reader.Read();
					Assert.AreEqual("þðüçöÝÞÐÜÇÖ", reader.GetString(0), "wrong Turkish string");
					reader.Read();
					Assert.AreEqual("ฅๆษ", reader.GetString(0), "wrong Thai string");
				}
				catch (Exception ex)
				{
					Assert.Fail(ex.Message);
				}
				finally
				{
					if (reader != null) reader.Close();
				}
			}
		}

		[Test]
		public void TestFloatNumbers()
		{
			CultureInfo curCulture = Thread.CurrentThread.CurrentCulture;
			CultureInfo curUICulture = Thread.CurrentThread.CurrentUICulture;
			CultureInfo c = new CultureInfo("de-De");
			Thread.CurrentThread.CurrentCulture = c;
			Thread.CurrentThread.CurrentUICulture = c;

			ClearTestTable();

			using (MaxDBCommand cmd = new MaxDBCommand("INSERT INTO Test (fl, db, decim) VALUES (:fl, :db, :decim)", mconn))
			{
                const float floatValue = 2.3f;
                const double doubleValue = 4.6;
                const decimal decimalValue = 23.82m;

				cmd.Parameters.Add(":fl", MaxDBType.Float);
				cmd.Parameters.Add(":db", MaxDBType.Number);
				cmd.Parameters.Add(":decim", MaxDBType.Number);
			    cmd.Parameters[0].Value = floatValue;
			    cmd.Parameters[1].Value = doubleValue;
			    cmd.Parameters[2].Value = decimalValue;
				int count = cmd.ExecuteNonQuery();
				Assert.AreEqual(1, count);

				try
				{
					cmd.CommandText = "SELECT fl, db, decim FROM Test";
					using (MaxDBDataReader reader = cmd.ExecuteReader())
					{
						reader.Read();
                        Assert.IsTrue(Math.Abs(floatValue - reader.GetFloat(0)) <= float.Epsilon, "wrong float value");
                        Assert.IsTrue(Math.Abs(doubleValue - reader.GetDouble(1)) <= double.Epsilon, "wrong double value");
                        Assert.IsTrue(Math.Abs(decimalValue - reader.GetDecimal(2)) <= 1e-28m, "wrong decimal value");
					}
				}
				catch (Exception ex)
				{
					Assert.Fail(ex.Message);
				}
				finally
				{
					Thread.CurrentThread.CurrentCulture = curCulture;
					Thread.CurrentThread.CurrentUICulture = curUICulture;
				}
			}
		}

	}
}

