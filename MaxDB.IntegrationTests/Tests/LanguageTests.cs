//-----------------------------------------------------------------------------------------------
// <copyright file="LanguageTests.cs" company="Dmitry S. Kataev">
//     Copyright © 2005-2011 Dmitry S. Kataev
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

namespace MaxDB.IntegrationTests
{
    using System;
    using System.Globalization;
    using System.Text;
    using System.Threading;
    using FluentAssertions;
    using MaxDB.Data;
    using NUnit.Framework;

    [TestFixture()]
    public class LanguageTests : BaseTest
    {
        [SetUp]
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

        [TearDown]
        public void TearDown() => Close();

        [Test]
        public void TestUnicodeStatement()
        {
            if (mconn.DatabaseEncoding != Encoding.Unicode)
            {
                Assert.Ignore("Non-unicode database");
            }

            ExecuteNonQuery("INSERT INTO Test (name) VALUES ('abcАБВ')"); // Russian
            ExecuteNonQuery("INSERT INTO Test (name) VALUES ('兣冘凥凷冋')"); // simplified Chinese
            ExecuteNonQuery("INSERT INTO Test (name) VALUES ('困巫忘否役')"); // traditional Chinese
            ExecuteNonQuery("INSERT INTO Test (name) VALUES ('ئابةتثجح')"); // Arabian
            ExecuteNonQuery("INSERT INTO Test (name) VALUES ('涯割晦叶角')"); // Japanese
            ExecuteNonQuery("INSERT INTO Test (name) VALUES ('ברחפע')"); // Hebrew
            ExecuteNonQuery("INSERT INTO Test (name) VALUES ('ψόβΩΞ')"); // Greek
            ExecuteNonQuery("INSERT INTO Test (name) VALUES ('þðüçöÝÞÐÜÇÖ')"); // Turkish
            ExecuteNonQuery("INSERT INTO Test (name) VALUES ('ฅๆษ')"); // Thai

            using var cmd = new MaxDBCommand("SELECT * FROM Test", mconn);
            using var reader = cmd.ExecuteReader();
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be("abcАБВ", "wrong Russian string");
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be("兣冘凥凷冋", "wrong simplified Chinese string");
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be("困巫忘否役", "wrong traditional Chinese string");
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be("ئابةتثجح", "wrong Arabian string");
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be("涯割晦叶角", "wrong Japanese string");
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be("ברחפע", "wrong Hebrew string");
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be("ψόβΩΞ", "wrong Greek string");
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be("þðüçöÝÞÐÜÇÖ", "wrong Turkish string");
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be("ฅๆษ", "wrong Thai string");
        }

        [Test]
        public void TestUnicodeParameter()
        {
            if (mconn.DatabaseEncoding != Encoding.Unicode)
            {
                Assert.Ignore("Non-unicode database");
            }

            using (var cmd = new MaxDBCommand("INSERT INTO Test (name) VALUES (:a)", mconn))
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

            using (var cmd = new MaxDBCommand("SELECT * FROM Test", mconn))
            {
                using var reader = cmd.ExecuteReader();
                reader.Read().Should().BeTrue();
                reader.GetString(0).Should().Be("abcАБВ", "wrong Russian string");
                reader.Read().Should().BeTrue();
                reader.GetString(0).Should().Be("兣冘凥凷冋", "wrong simplified Chinese string");
                reader.Read().Should().BeTrue();
                reader.GetString(0).Should().Be("困巫忘否役", "wrong traditional Chinese string");
                reader.Read().Should().BeTrue();
                reader.GetString(0).Should().Be("ئابةتثجح", "wrong Arabian string");
                reader.Read().Should().BeTrue();
                reader.GetString(0).Should().Be("涯割晦叶角", "wrong Japanese string");
                reader.Read().Should().BeTrue();
                reader.GetString(0).Should().Be("ברחפע", "wrong Hebrew string");
                reader.Read().Should().BeTrue();
                reader.GetString(0).Should().Be("ψόβΩΞ", "wrong Greek string");
                reader.Read().Should().BeTrue();
                reader.GetString(0).Should().Be("þðüçöÝÞÐÜÇÖ", "wrong Turkish string");
                reader.Read().Should().BeTrue();
                reader.GetString(0).Should().Be("ฅๆษ", "wrong Thai string");
            }
        }

        [Test]
        public void TestFloatNumbers()
        {
            var curCulture = Thread.CurrentThread.CurrentCulture;
            var curUICulture = Thread.CurrentThread.CurrentUICulture;
            var c = new CultureInfo("de-De");
            Thread.CurrentThread.CurrentCulture = c;
            Thread.CurrentThread.CurrentUICulture = c;

            using var cmd = new MaxDBCommand("INSERT INTO Test (fl, db, decim) VALUES (:fl, :db, :decim)", mconn);
            const float floatValue = 2.3f;
            const double doubleValue = 4.6;
            const decimal decimalValue = 23.82m;

            cmd.Parameters.Add(":fl", MaxDBType.Float);
            cmd.Parameters.Add(":db", MaxDBType.Number);
            cmd.Parameters.Add(":decim", MaxDBType.Number);
            cmd.Parameters[0].Value = floatValue;
            cmd.Parameters[1].Value = doubleValue;
            cmd.Parameters[2].Value = decimalValue;
            cmd.ExecuteNonQuery().Should().Be(1);

            try
            {
                cmd.CommandText = "SELECT fl, db, decim FROM Test";
                using var reader = cmd.ExecuteReader();
                reader.Read().Should().BeTrue();
                Math.Abs(floatValue - reader.GetFloat(0)).Should().BeLessOrEqualTo(float.Epsilon, "wrong float value");
                Math.Abs(doubleValue - reader.GetDouble(1)).Should().BeLessOrEqualTo(double.Epsilon, "wrong double value");
                Math.Abs(decimalValue - reader.GetDecimal(2)).Should().BeLessOrEqualTo(1e-28m, "wrong decimal value");
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

