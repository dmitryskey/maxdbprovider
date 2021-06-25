//-----------------------------------------------------------------------------------------------
// <copyright file="IMaxDBSocket.cs" company="2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG">
// Copyright (c) 2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------------------------------
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

namespace MaxDB.Data.Interfaces.Utils
{
    using System.IO;

    /// <summary>
    /// Interface to support tcp and ssl connection.
    /// </summary>
    internal interface IMaxDBSocket
    {
        bool ReopenSocketAfterInfoPacket { get; }

        bool DataAvailable { get; }

        Stream Stream { get; }

        string Host { get; }

        int Port { get; }

        IMaxDBSocket Clone();

        void Close();
    }
}