//-----------------------------------------------------------------------------------------------
// <copyright file="MaxDBCommandTests.cs" company="2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG">
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

using FluentAssertions;
using MaxDB.Data;
using MaxDB.Data.Interfaces;
using NSubstitute;
using NUnit.Framework;

namespace MaxDB.UnitTests
{
    [TestFixture]
    public class MaxDBCommandTests
    {
        [Test]
        public void WhenCancelIsCalled_AndCommunicationIsCancelled()
        {
            var connection = new MaxDBConnection();

            var cmd = new MaxDBCommand(null, connection);

            var comm = Substitute.For<IMaxDBComm>();

            (connection as IMaxDBConnection).Comm = comm;

            cmd.Cancel();

            cmd.Canceled.Should().BeTrue();
            comm.Received().Cancel(Arg.Any<object>());
        }

        [Test]
        public void WhenCancelIsCalled_AndNoConnection_ThrowException()
        {
            var cmd = new MaxDBCommand();

            Assert.Throws<MaxDBException>(() => cmd.Cancel()).Message.Should().Be("Object is closed.");
        }
    }
}