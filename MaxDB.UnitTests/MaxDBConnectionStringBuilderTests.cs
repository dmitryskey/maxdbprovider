//-----------------------------------------------------------------------------------------------
// <copyright file="MaxDBConnectionStringBuilderTests.cs" company="2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG">
// Copyright © 2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------------------------------
//
//  This program is free software; you can redistribute it and/or
//  modify it under the terms of the GNU General Public License
//  as published by the Free Software Foundation; either version 2
//  of the License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
//
//  You should have received a copy of the GNU General Public License
//  along with this program; if not, write to the Free Software
//  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

using System.Text;
using FluentAssertions;
using MaxDB.Data;
using NUnit.Framework;

namespace MaxDB.UnitTests
{
    [TestFixture]
    public class MaxDBConnectionStringBuilderTests
    {
        private class AppendKeyValuePairTests
        {
            [Test]
            public void ShouldThrowException_WhenBuilderIsNull() =>
                Assert.Throws<MaxDBException>(() => MaxDBConnectionStringBuilder.AppendKeyValuePair(null, null, null))
                    .Message.Should().Be("Parameter builder is null.");

            [Test]
            public void ShouldThrowException_WhenKeyIsNull() =>
                Assert.Throws<MaxDBException>(() => MaxDBConnectionStringBuilder.AppendKeyValuePair(new StringBuilder(), null, null))
                    .Message.Should().Be("Parameter keyword is null.");

            [TestCase(null)]
            [TestCase("")]
            [TestCase(" ")]
            [TestCase("\t")]
            public void ShouldSkip_WhenKeyIsNullOrEmptyOrWhiteSpace(string val)
            {
                var sb = new StringBuilder();
                MaxDBConnectionStringBuilder.AppendKeyValuePair(sb, "key", val);
                sb.ToString().Should().BeNullOrEmpty();
            }

            [Test]
            public void ShouldAddKeyAndValue()
            {
                var sb = new StringBuilder();
                MaxDBConnectionStringBuilder.AppendKeyValuePair(sb, "key", "val");
                sb.ToString().Should().Be("key=val");

                MaxDBConnectionStringBuilder.AppendKeyValuePair(sb, "key2", "val2");
                sb.ToString().Should().Be("key=val;key2=val2");
            }
        }

        private class ShouldSerializeTests
        {
            private readonly MaxDBConnectionStringBuilder builder = new("server=value");

            [Test]
            public void ShouldSerialize_AndHasValue() =>
                builder.ShouldSerialize("DATA SOURCE").Should().BeTrue();

            [Test]
            public void ShouldSerialize_AndHasNoValue() =>
                builder.ShouldSerialize("key").Should().BeFalse();

            [Test]
            public void ToString_ShouldReturnConnectionString() =>
                builder.ToString().Should().Be("DATA SOURCE=value;MODE=Internal");

            [Test]
            public void TryGetValue_ShouldReturnValue()
            {
                builder.TryGetValue("DATA SOURCE", out object val).Should().BeTrue();
                val.Should().Be("value");
            }

            [Test]
            public void TryGetValue_ShouldNotReturnValue() =>
                builder.TryGetValue("server", out _).Should().BeFalse();
        }
    }
}