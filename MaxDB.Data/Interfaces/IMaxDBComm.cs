//-----------------------------------------------------------------------------------------------
// <copyright file="IMaxDBComm.cs" company="2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG">
// Copyright © 2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------------------------------
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General internal License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General internal License for more details.
//
// You should have received a copy of the GNU General internal License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using MaxDB.Data.Interfaces.MaxDBProtocol;
using MaxDB.Data.Interfaces.Utils;

namespace MaxDB.Data.Interfaces
{
    /// <summary>
    /// MaxDB communication interface.
    /// </summary>
    internal interface IMaxDBComm : IDisposable
    {
        /// <summary>
        /// Gets connection encoding.
        /// </summary>
        Encoding Encoding { get; }

        /// <summary>
        /// Gets a next cursor name.
        /// </summary>
        string NextCursorName { get; }

        /// <summary>
        /// Gets or sets a connection string builder.
        /// </summary>
        MaxDBConnectionStringBuilder ConnStrBuilder { get; set; }

        /// <summary>
        /// Gets a kernel version without patch level, e.g. 70402 or 70600.
        /// </summary>
        int KernelVersion { get; }

        /// <summary>
        /// Gets or sets a value indicating whether auto commit is used for the session.
        /// </summary>
        bool AutoCommit { get; set; }

        /// <summary>
        /// Gets or sets an isolation level.
        /// </summary>
        IsolationLevel IsolationLevel { get; set; }

        /// <summary>
        /// Gets a value indicating whether session in the middle of transaction.
        /// </summary>
        bool IsInTransaction { get; }

        /// <summary>
        /// Gets the session id.
        /// </summary>
        int SessionID { get; }

        /// <summary>
        /// Gets connection open time.
        /// </summary>
        DateTime OpenTime { get;}

        /// <summary>
        /// Gets a parsing cache.
        /// </summary>
        IParseInfoCache ParseCache { get; }

        /// <summary>
        /// Gets a network socket.
        /// </summary>
        IMaxDBSocket Socket { get;}

        /// <summary>
        /// Gets a request packets pool.
        /// </summary>
        Stack<IMaxDBRequestPacket> PacketPool { get; }

        /// <summary>
        /// Gets an Unicode request packets pool.
        /// </summary>
        Stack<IMaxDBRequestPacket> UnicodePacketPool { get; }

        /// <summary>
        /// Cancel request.
        /// </summary>
        /// <param name="reqObj">Request object.</param>
        void Cancel(object reqObj);

        /// <summary>
        /// Cancel session.
        /// </summary>
        void Cancel();

        /// <summary>
        /// Connect to the database.
        /// </summary>
        /// <param name="dbname">Database name.</param>
        /// <param name="port">Port number.</param>
        void Connect(string dbname, int port);

        /// <summary>
        /// Reconnect to the database.
        /// </summary>
        void Reconnect();

        /// <summary>
        /// Close session.
        /// </summary>
        /// <param name="closeSocket">Close underlaying socket.</param>
        /// <param name="release">Release work.</param>
        void Close(bool closeSocket, bool release);

        /// <summary>
        /// Execute user packet.
        /// </summary>
        /// <param name="userPacket">User packet.</param>
        /// <param name="len">Packet length.</param>
        /// <returns>Response packet.</returns>
        byte[] Execute(IMaxDBRequestPacket userPacket, int len);

        /// <summary>
        /// Set kernal feature request.
        /// </summary>
        /// <param name="feature">Feature number.</param>
        void SetKernelFeatureRequest(int feature);

        /// <summary>
        /// Checks if the given feature supported by kernel.
        /// </summary>
        /// <param name="feature">Feature number.</param>
        /// <returns>Returns <c>true</c> if feature is supported and <c>false</c> otherwise.</returns>
        bool IsKernelFeatureSupported(int feature);

        /// <summary>
        /// Open session.
        /// </summary>
        /// <param name="connArgs">Connection arguments.</param>
        /// <param name="initSocket">Parameter indicating whether network socket should be initialized.</param>
        void Open(ConnectArgs connArgs, bool initSocket = true);

        /// <summary>
        /// Try to reconnect.
        /// </summary>
        /// <param name="connArgs">Connection arguments.</param>
        void TryReconnect(ConnectArgs connArgs);

        /// <summary>
        /// Drop parsing ID.
        /// </summary>
        /// <param name="pid">Parsing ID.</param>
        void DropParseID(byte[] pid);

        /// <summary>
        /// Execute SQL statement.
        /// </summary>
        /// <param name="connArgs">Connection arguments.</param>
        /// <param name="cmd">Sql command.</param>
        /// <param name="gcFlags">GC flags.</param>
        void ExecuteSqlString(ConnectArgs connArgs, string cmd, int gcFlags);

        /// <summary>
        /// Commit command.
        /// </summary>
        /// <param name="connArgs">Connection arguments.</param>
        void Commit(ConnectArgs connArgs);

        /// <summary>
        /// Rollback command.
        /// </summary>
        /// <param name="connArgs">Connection arguments.</param>
        void Rollback(ConnectArgs connArgs);

        /// <summary>
        /// Gets request packet.
        /// </summary>
        /// <returns>Request packet.</returns>
        IMaxDBRequestPacket GetRequestPacket();

        /// <summary>
        /// Release request packet.
        /// </summary>
        /// <param name="requestPacket">Request packet.</param>
        void FreeRequestPacket(IMaxDBRequestPacket requestPacket);

        /// <summary>
        /// Execute DB object.
        /// </summary>
        /// <param name="connArgs">Connection arguments.</param>
        /// <param name="requestPacket">Request packet.</param>
        /// <param name="execObj">Object to execute.</param>
        /// <param name="gcFlags">GC flags.</param>
        /// <returns>Reply packet.</returns>
        IMaxDBReplyPacket Execute(ConnectArgs connArgs, IMaxDBRequestPacket requestPacket, object execObj, int gcFlags);

        /// <summary>
        /// Execute DB object.
        /// </summary>
        /// <param name="connArgs">Connection arguments.</param>
        /// <param name="requestPacket">Request packet.</param>
        /// <param name="ignoreErrors">Flag indicating whether errors should be ignored.</param>
        /// <param name="isParse">Flag indicating whether only parsing should be done.</param>
        /// <param name="execObj">Object to execute.</param>
        /// <param name="gcFlags">GC flags.</param>
        /// <returns>Reply packet.</returns>
        IMaxDBReplyPacket Execute(ConnectArgs connArgs, IMaxDBRequestPacket requestPacket, bool ignoreErrors, bool isParse, object execObj, int gcFlags);
    }
}