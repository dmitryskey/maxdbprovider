//-----------------------------------------------------------------------------------------------
// <copyright file="MaxDBComm.cs" company="2005-2019 Dmitry S. Kataev, 2002-2003 SAP AG">
// Copyright (c) 2005-2019 Dmitry S. Kataev, 2002-2003 SAP AG. All rights reserved.
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

namespace MaxDB.Data.MaxDBProtocol
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;
    using System.Runtime.CompilerServices;
    using System.Text;
    using MaxDB.Data.Utilities;

    /// <summary>
    /// MaxDB communication class.
    /// </summary>
    internal sealed class MaxDBComm : IDisposable
    {
        private static readonly object SyncObj = string.Empty;
        private static byte[] byDefFeatureSet = { 1, 0, 2, 0, 3, 0, 4, 0, 5, 0 };
        private readonly MaxDBLogger logger;
        private readonly TimeSpan ts = new TimeSpan(1);

        private object objExec;
        private int nonRecyclingExecutions;
        private bool inTransaction;
        private bool inReconnect;
        private int cursorId;

        private byte[] byKernelFeatures = new byte[byDefFeatureSet.Length];
        private string strDbName;
        private int iPort;
        private int iSender;
        private bool bIsAuthAllowed;
        private bool bIsServerLittleEndian;
        private int iMaxCmdSize;
        private bool bSession;
        private GarbageParseId garbageParseids;

        /// <summary>
        /// Initializes a new instance of the <see cref="MaxDBComm"/> class.
        /// </summary>
        /// <param name="logger">Logger object.</param>
        public MaxDBComm(MaxDBLogger logger) => this.logger = logger;

        /// <summary>
        /// Gets a next cursor name.
        /// </summary>
        public string NextCursorName => Consts.CursorPrefix + this.cursorId++;

        /// <summary>
        /// Gets or sets an isolation level.
        /// </summary>
        public IsolationLevel IsolationLevel { get; set; } = IsolationLevel.Unspecified;

        /// <summary>
        /// Gets or sets a connection string builder.
        /// </summary>
        public MaxDBConnectionStringBuilder ConnStrBuilder { get; set; }

        /// <summary>
        /// Gets connection open time.
        /// </summary>
        public DateTime OpenTime { get; private set; }

        /// <summary>
        /// Gets connection encoding.
        /// </summary>
        public Encoding Encoding { get; private set; } = Encoding.ASCII;

        /// <summary>
        /// Gets a kernel version without patch level, e.g. 70402 or 70600.
        /// </summary>
        public int KernelVersion { get; private set; }

        /// <summary>
        /// Gets or sets a value indicating whether auto commit is used for the session.
        /// </summary>
        public bool AutoCommit { get; set; }

        /// <summary>
        /// Gets a value indicating whether session in the middle of transaction.
        /// </summary>
        public bool IsInTransaction => !this.AutoCommit && this.inTransaction;

        /// <summary>
        /// Gets the session id.
        /// </summary>
        public int SessionID { get; private set; } = -1;

        /// <summary>
        /// Gets a parsing cache.
        /// </summary>
        internal MaxDBCache.ParseInfoCache ParseCache { get; private set; }

        /// <summary>
        /// Gets a network socket.
        /// </summary>
        internal IMaxDBSocket Socket { get; private set; }

        /// <summary>
        /// Gets a request packets pool.
        /// </summary>
        internal Stack<MaxDBRequestPacket> PacketPool { get; private set; } = new Stack<MaxDBRequestPacket>();

        /// <summary>
        /// Gets an Unicode request packets pool.
        /// </summary>
        internal Stack<MaxDBUnicodeRequestPacket> UnicodePacketPool { get; private set; } = new Stack<MaxDBUnicodeRequestPacket>();

        private string TermID => $"ado.net@{this.GetHashCode().ToString("x", CultureInfo.InvariantCulture)}".PadRight(18);

        /// <summary>
        /// Connect to the database.
        /// </summary>
        /// <param name="dbname">Database name.</param>
        /// <param name="port">Port number.</param>
        public void Connect(string dbname, int port)
        {
            try
            {
                var connData = new ConnectPacketData
                {
                    DBName = dbname,
                    Port = port,
                    MaxSegmentSize = 1024 * 32,
                };

                var request = new MaxDBConnectPacket(new byte[HeaderOffset.END + ConnectPacketOffset.END], connData);
                request.FillHeader(RSQLTypes.INFO_REQUEST, this.iSender);
                request.FillPacketLength();
                request.SetSendLength(request.PacketLength);
                this.Socket.Stream.Write(request.GetArrayData(), 0, request.PacketLength);

                var reply = this.GetConnectReply();
                int returnCode = reply.ReturnCode;
                if (returnCode != 0)
                {
                    this.Close(true, false);
                    throw new MaxDBCommunicationException(returnCode);
                }

                if (string.Compare(dbname.Trim(), reply.ClientDB.Trim(), true, CultureInfo.InvariantCulture) != 0)
                {
                    this.Close(true, false);
                    throw new MaxDBCommunicationException(RTEReturnCodes.SQLSERVER_DB_UNKNOWN);
                }

                if (this.Socket.ReopenSocketAfterInfoPacket)
                {
                    var new_socket = this.Socket.Clone();
                    this.Close(true, false);
                    this.Socket = new_socket;
                }

                this.bSession = true;
                this.strDbName = dbname;
                this.iPort = port;

                connData.DBName = dbname;
                connData.Port = port;
                connData.MaxSegmentSize = reply.PacketSize;
                connData.MaxDataLen = reply.MaxDataLength;
                connData.PacketSize = reply.PacketSize;
                connData.MinReplySize = reply.MinReplySize;

                var db_request = new MaxDBConnectPacket(new byte[HeaderOffset.END + reply.MaxDataLength], connData);
                db_request.FillHeader(RSQLTypes.USER_CONN_REQUEST, this.iSender);
                db_request.FillPacketLength();
                db_request.SetSendLength(db_request.PacketLength);
                this.Socket.Stream.Write(db_request.GetArrayData(), 0, db_request.PacketLength);

                reply = this.GetConnectReply();
                this.bIsAuthAllowed = reply.IsAuthAllowed;
                this.bIsServerLittleEndian = reply.IsLittleEndian;
                this.iMaxCmdSize = reply.MaxDataLength - reply.MinReplySize;
            }
            catch (Exception ex)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.HOST_CONNECT_FAILED, this.Socket.Host, this.Socket.Port), ex);
            }
        }

        /// <summary>
        /// Reconnect to the database.
        /// </summary>
        public void Reconnect()
        {
            var new_socket = this.Socket.Clone();
            this.Close(true, false);
            this.Socket = new_socket;
            if (this.bSession)
            {
                this.Connect(this.strDbName, this.iPort);
            }
            else
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.ADMIN_RECONNECT, CommError.ErrorText[RTEReturnCodes.SQLTIMEOUT]));
            }
        }

        /// <summary>
        /// Close session.
        /// </summary>
        /// <param name="closeSocket">Close underlaying socket.</param>
        /// <param name="release">Release work.</param>
        public void Close(bool closeSocket, bool release)
        {
            if (this.Socket != null && this.Socket.Stream != null)
            {
                this.SessionID = -1;

                if (release)
                {
                    try
                    {
                        if (this.garbageParseids != null)
                        {
                            this.garbageParseids.EmptyCan();
                        }

                        this.ExecuteSqlString(default(ConnectArgs), "ROLLBACK WORK RELEASE", GCMode.NONE);
                    }
                    catch (MaxDBException)
                    {
                    }
                }

                var request = new MaxDBConnectPacket(new byte[HeaderOffset.END]);
                request.FillHeader(RSQLTypes.USER_RELEASE_REQUEST, this.iSender);
                request.SetSendLength(HeaderOffset.END);
                this.Socket.Stream.Write(request.GetArrayData(), 0, request.Length);
                if (closeSocket)
                {
                    this.Socket.Close();
                }
            }
        }

        /// <summary>
        /// Cancel session.
        /// </summary>
        public void Cancel()
        {
            try
            {
                if (this.Socket != null && this.Socket.Stream != null)
                {
                    var cancel_socket = this.Socket.Clone();
                    var connData = new ConnectPacketData
                    {
                        DBName = this.strDbName,
                        Port = this.iPort,
                        MaxSegmentSize = 1024 * 32,
                    };

                    var request = new MaxDBConnectPacket(new byte[HeaderOffset.END + ConnectPacketOffset.END], connData);
                    request.FillHeader(RSQLTypes.USER_CANCEL_REQUEST, this.iSender);
                    request.WriteInt32(this.iSender, HeaderOffset.ReceiverRef);
                    request.SetSendLength(request.PacketLength);
                    request.Offset = HeaderOffset.END;
                    request.Close();
                    cancel_socket.Stream.Write(request.GetArrayData(), 0, request.PacketLength);
                    cancel_socket.Close();
                }
            }
            catch (Exception ex)
            {
                throw new MaxDBException(ex.Message);
            }
        }

        /// <summary>
        /// Execute user packet.
        /// </summary>
        /// <param name="userPacket">User packet.</param>
        /// <param name="len">Packet length.</param>
        /// <returns>Response packet.</returns>
        public byte[] Execute(MaxDBRequestPacket userPacket, int len)
        {
            var rawPacket = new MaxDBPacket(userPacket.GetArrayData(), 0, userPacket.Swapped);
            rawPacket.FillHeader(RSQLTypes.USER_DATA_REQUEST, this.iSender);
            rawPacket.SetSendLength(len + HeaderOffset.END);

            this.Socket.Stream.Write(rawPacket.GetArrayData(), 0, len + HeaderOffset.END);
            byte[] headerBuf = new byte[HeaderOffset.END];

            int headerLength = this.Socket.Stream.Read(headerBuf, 0, headerBuf.Length);

            if (headerLength != HeaderOffset.END)
            {
                throw new MaxDBCommunicationException(RTEReturnCodes.SQLRECEIVE_LINE_DOWN);
            }

            var header = new MaxDBConnectPacket(headerBuf, this.bIsServerLittleEndian);

            int returnCode = header.ReturnCode;
            if (returnCode != 0)
            {
                throw new MaxDBCommunicationException(returnCode);
            }

            byte[] packetBuf = new byte[header.MaxSendLength - HeaderOffset.END];
            int replyLen = HeaderOffset.END;
            int bytesRead;

            while (replyLen < header.ActSendLength)
            {
                bytesRead = this.Socket.Stream.Read(packetBuf, replyLen - HeaderOffset.END, header.ActSendLength - replyLen);
                if (bytesRead <= 0)
                {
                    break;
                }

                replyLen += bytesRead;
                if (!this.Socket.DataAvailable)
                {
                    System.Threading.Thread.Sleep(this.ts); // wait for the end of data
                }
            }

            if (replyLen < header.ActSendLength)
            {
                throw new MaxDBCommunicationException(RTEReturnCodes.SQLRECEIVE_LINE_DOWN);
            }

            if (replyLen > header.ActSendLength)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CHUNKOVERFLOW, header.ActSendLength, replyLen, packetBuf.Length + HeaderOffset.END));
            }

            return packetBuf;
        }

        /// <summary>
        /// Set kernal feature request.
        /// </summary>
        /// <param name="feature">Feature number.</param>
        public void SetKernelFeatureRequest(int feature) => this.byKernelFeatures[(2 * (feature - 1)) + 1] = 1;

        /// <summary>
        /// Checks if the given feature supported by kernel.
        /// </summary>
        /// <param name="feature">Feature number.</param>
        /// <returns>Returns <c>true</c> if feature is supported and <c>false</c> otherwise.</returns>
        public bool IsKernelFeatureSupported(int feature) => (this.byKernelFeatures[(2 * (feature - 1)) + 1] == 1) ? true : false;

        /// <summary>
        /// Open session.
        /// </summary>
        /// <param name="connArgs">Connection arguments.</param>
        /// <param name="initSocket">Parameter indicating whether network socket should be initialized.</param>
        public void Open(ConnectArgs connArgs, bool initSocket = true)
        {
            if (initSocket)
            {
                if (this.ConnStrBuilder.Encrypt)
                {
                    if (connArgs.port == 0)
                    {
                        connArgs.port = Ports.DefaultSecure;
                    }

                    this.Socket = new SslSocketClass(connArgs.host, connArgs.port, this.ConnStrBuilder.Timeout, true, this.ConnStrBuilder.SslCertificateName ?? connArgs.host);
                }
                else
                {
                    if (connArgs.port == 0)
                    {
                        connArgs.port = Ports.Default;
                    }

                    this.Socket = new SocketClass(connArgs.host, connArgs.port, this.ConnStrBuilder.Timeout, true);
                }
            }

            string username = connArgs.username;
            if (username == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.NOUSER));
            }

            username = StripString(username);

            string password = connArgs.password;
            if (password == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.NOPASSWORD));
            }

            password = StripString(password);

            byte[] passwordBytes = Encoding.ASCII.GetBytes(password);

            DateTime currentDt = DateTime.Now;

            //// >>> SQL TRACE
            if (this.logger.TraceSQL)
            {
                this.logger.SqlTrace(currentDt, "::CONNECT");
                this.logger.SqlTrace(
                    currentDt,
                    $"SERVERNODE: '{connArgs.host + (connArgs.port > 0 ? ":" + connArgs.port.ToString(CultureInfo.InvariantCulture) : string.Empty)}'");
                this.logger.SqlTrace(currentDt, $"SERVERDB  : '{connArgs.dbname}'");
                this.logger.SqlTrace(currentDt, $"USER  : '{connArgs.username}'");
            }
            //// <<< SQL TRACE

            this.Connect(connArgs.dbname, connArgs.port);

            string connectCmd;
            byte[] crypted;
            this.UnicodePacketPool.Clear();
            this.PacketPool.Clear();
            this.Encoding = Encoding.ASCII;
            var requestPacket = this.GetRequestPacket();
            Auth auth = null;
            bool isChallengeResponseSupported = false;
            if (this.bIsAuthAllowed)
            {
                try
                {
                    auth = new Auth();
                    isChallengeResponseSupported = this.InitiateChallengeResponse(connArgs, requestPacket, username, auth);
                    if (password.Length > auth.MaxPasswordLength && auth.MaxPasswordLength > 0)
                    {
                        password = password.Substring(0, auth.MaxPasswordLength);
                    }
                }
                catch (MaxDBException ex)
                {
                    isChallengeResponseSupported = false;
                    if (ex.ErrorCode == -5015)
                    {
                        try
                        {
                            this.Reconnect();
                        }
                        catch (MaxDBException exc)
                        {
                            throw new MaxDBConnectionException(exc);
                        }
                    }
                    else
                    {
                        throw;
                    }
                }
            }

            if (this.ConnStrBuilder.Encrypt && !isChallengeResponseSupported)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONNECTION_CHALLENGERESPONSENOTSUPPORTED));
            }

            // build connect statement
            connectCmd = $"CONNECT {connArgs.username} IDENTIFIED BY :PW SQLMODE {SqlModeName.Value[(byte)this.ConnStrBuilder.Mode]}";
            if (this.ConnStrBuilder.Timeout > 0)
            {
                connectCmd += $" TIMEOUT {this.ConnStrBuilder.Timeout}";
            }

            if (this.IsolationLevel != IsolationLevel.Unspecified)
            {
                connectCmd += $" ISOLATION LEVEL {MaxDBConnection.MapIsolationLevel(this.IsolationLevel).ToString(CultureInfo.InvariantCulture)}";
            }

            if (this.ConnStrBuilder.CacheLimit > 0)
            {
                connectCmd += $" CACHELIMIT {this.ConnStrBuilder.CacheLimit}";
            }

            if (this.ConnStrBuilder.SpaceOption)
            {
                connectCmd += $" SPACE OPTION ";
                this.SetKernelFeatureRequest(Feature.SpaceOption);
            }

            //// >>> SQL TRACE
            if (this.logger.TraceSQL)
            {
                this.logger.SqlTrace(currentDt, $"CONNECT COMMAND: {connectCmd}");
            }
            //// <<< SQL TRACE

            requestPacket.InitDbsCommand(false, connectCmd);

            if (!isChallengeResponseSupported)
            {
                try
                {
                    crypted = Crypt.Mangle(password, false);
                }
                catch
                {
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.INVALIDPASSWORD));
                }

                requestPacket.NewPart(PartKind.Data);
                requestPacket.AddData(crypted);
                requestPacket.AddDataString(this.TermID);
                requestPacket.PartArguments++;
            }
            else
            {
                requestPacket.AddClientProofPart(auth.GetClientProof(passwordBytes));
                requestPacket.AddClientIdPart(this.TermID);
            }

            byDefFeatureSet.CopyTo(this.byKernelFeatures, 0);

            this.SetKernelFeatureRequest(Feature.MultipleDropParseid);
            this.SetKernelFeatureRequest(Feature.CheckScrollableOption);
            requestPacket.AddFeatureRequestPart(this.byKernelFeatures);

            // execute
            var replyPacket = this.Execute(connArgs, requestPacket, this, GCMode.DELAYED);
            this.SessionID = replyPacket.SessionID;
            this.Encoding = replyPacket.IsUnicode ? Encoding.Unicode : Encoding.ASCII;

            this.KernelVersion = (10000 * replyPacket.KernelMajorVersion) + (100 * replyPacket.KernelMinorVersion) + replyPacket.KernelCorrectionLevel;
            byte[] featureReturn = replyPacket.Features;

            if (featureReturn != null)
            {
                this.byKernelFeatures = featureReturn;
            }
            else
            {
                byDefFeatureSet.CopyTo(this.byKernelFeatures, 0);
            }

            if (this.ConnStrBuilder.Cache != null && this.ConnStrBuilder.Cache.Length > 0)
            {
                this.ParseCache = new MaxDBCache.ParseInfoCache(this.ConnStrBuilder.Cache, this.ConnStrBuilder.CacheSize);
            }

            //// >>> SQL TRACE
            if (this.logger.TraceSQL)
            {
                this.logger.SqlTrace(DateTime.Now, $"SESSION ID: {this.SessionID}");
            }
            //// <<< SQL TRACE

            this.OpenTime = DateTime.Now;
        }

        /// <summary>
        /// Try to reconnect.
        /// </summary>
        /// <param name="connArgs">Connection arguments.</param>
        public void TryReconnect(ConnectArgs connArgs)
        {
            lock (SyncObj)
            {
                if (this.ParseCache != null)
                {
                    this.ParseCache.Clear();
                }

                this.PacketPool.Clear();
                this.UnicodePacketPool.Clear();
                this.inReconnect = true;
                try
                {
                    this.Reconnect();
                    this.Open(connArgs);
                }
                catch (MaxDBException ex)
                {
                    throw new MaxDBConnectionException(ex);
                }
                finally
                {
                    this.inReconnect = false;
                }

                throw new MaxDBTimeoutException();
            }
        }

        /// <summary>
        /// Cancel request.
        /// </summary>
        /// <param name="reqObj">Request object.</param>
        public void Cancel(object reqObj)
        {
            DateTime dt = DateTime.Now;

            //// >>> SQL TRACE
            if (this.logger.TraceSQL)
            {
                this.logger.SqlTrace(dt, "::CANCEL");
                this.logger.SqlTrace(dt, $"SESSION ID: {this.SessionID}");
            }
            //// <<< SQL TRACE

            if (this.objExec == reqObj)
            {
                this.Cancel();
            }
            else
            {
                //// >>> SQL TRACE
                if (this.logger.TraceSQL)
                {
                    this.logger.SqlTrace(dt, "RETURN     : 100");
                    this.logger.SqlTrace(dt, "MESSAGE    : No active command found.");
                }
                //// <<< SQL TRACE
            }
        }

        /// <summary>
        /// Drop parsing ID.
        /// </summary>
        /// <param name="pid">Parsing ID.</param>
        public void DropParseID(byte[] pid)
        {
            if (pid == null)
            {
                return;
            }

            if (this.garbageParseids == null)
            {
                this.garbageParseids = new GarbageParseId(this.IsKernelFeatureSupported(Feature.MultipleDropParseid));
            }

            this.garbageParseids.ThrowIntoGarbageCan(pid);
        }

        /// <summary>
        /// Execute SQL statement.
        /// </summary>
        /// <param name="connArgs">Connection arguments.</param>
        /// <param name="cmd">Sql command.</param>
        /// <param name="gcFlags">GC flags.</param>
        public void ExecuteSqlString(ConnectArgs connArgs, string cmd, int gcFlags)
        {
            var requestPacket = this.GetRequestPacket();
            requestPacket.InitDbs(this.AutoCommit);
            requestPacket.AddString(cmd);
            try
            {
                this.Execute(connArgs, requestPacket, this, gcFlags);
            }
            catch (MaxDBTimeoutException)
            {
                // ignore
            }
        }

        /// <summary>
        /// Commit command.
        /// </summary>
        /// <param name="connArgs">Connection arguments.</param>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Commit(ConnectArgs connArgs)
        {
            // send commit
            this.ExecuteSqlString(connArgs, "COMMIT WORK", GCMode.ALLOWED);
            this.inTransaction = false;
        }

        /// <summary>
        /// Rollback command.
        /// </summary>
        /// <param name="connArgs">Connection arguments.</param>
        public void Rollback(ConnectArgs connArgs)
        {
            // send rollback
            this.ExecuteSqlString(connArgs, "ROLLBACK WORK", GCMode.ALLOWED);
            this.inTransaction = false;
        }

        /// <summary>
        /// Dispose object.
        /// </summary>
        public void Dispose()
        {
            this.Close(true, false);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Get request packet.
        /// </summary>
        /// <returns>Request packet.</returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        internal MaxDBRequestPacket GetRequestPacket()
        {
            var packet = this.Encoding == Encoding.Unicode
                ? this.UnicodePacketPool.Count == 0
                    ? new MaxDBUnicodeRequestPacket(new byte[HeaderOffset.END + this.iMaxCmdSize], Consts.AppID, Consts.AppVersion)
                    : this.UnicodePacketPool.Pop()
                : this.PacketPool.Count == 0
                    ? new MaxDBRequestPacket(new byte[HeaderOffset.END + this.iMaxCmdSize], Consts.AppID, Consts.AppVersion)
                    : this.PacketPool.Pop();
            packet.SwitchSqlMode((byte)this.ConnStrBuilder.Mode);
            return packet;
        }

        /// <summary>
        /// Release request packet.
        /// </summary>
        /// <param name="requestPacket">Request packet.</param>
        internal void FreeRequestPacket(MaxDBRequestPacket requestPacket)
        {
            if (this.Encoding == Encoding.Unicode)
            {
                this.UnicodePacketPool.Push(requestPacket as MaxDBUnicodeRequestPacket);
            }
            else
            {
                this.PacketPool.Push(requestPacket);
            }
        }

        /// <summary>
        /// Execute DB object.
        /// </summary>
        /// <param name="connArgs">Connection arguments.</param>
        /// <param name="requestPacket">Request packet.</param>
        /// <param name="execObj">Object to execute.</param>
        /// <param name="gcFlags">GC flags.</param>
        /// <returns>Reply packet.</returns>
        internal MaxDBReplyPacket Execute(ConnectArgs connArgs, MaxDBRequestPacket requestPacket, object execObj, int gcFlags) =>
            this.Execute(connArgs, requestPacket, false, false, execObj, gcFlags);

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
        [MethodImpl(MethodImplOptions.Synchronized)]
        internal MaxDBReplyPacket Execute(ConnectArgs connArgs, MaxDBRequestPacket requestPacket, bool ignoreErrors, bool isParse, object execObj, int gcFlags)
        {
            int requestLen;
            MaxDBReplyPacket replyPacket = null;
            int localWeakReturnCode = 0;

            if (this.SessionID >= 0)
            {
                if (gcFlags == GCMode.ALLOWED)
                {
                    if (this.garbageParseids != null && this.garbageParseids.IsPending)
                    {
                        this.garbageParseids.EmptyCan(requestPacket);
                    }
                }
                else
                {
                    if (this.garbageParseids != null && this.garbageParseids.IsPending)
                    {
                        this.nonRecyclingExecutions++;
                    }
                }
            }

            requestPacket.Close();

            requestLen = requestPacket.PacketLength;

            try
            {
                DateTime dt = DateTime.Now;

                //// >>> PACKET TRACE
                if (this.logger.TraceFull)
                {
                    this.logger.SqlTrace(dt, $"<PACKET>{requestPacket.DumpPacket()}");

                    int segm = requestPacket.FirstSegment();
                    while (segm != -1)
                    {
                        this.logger.SqlTrace(dt, requestPacket.DumpSegment(dt));
                        segm = requestPacket.NextSegment();
                    }

                    this.logger.SqlTrace(dt, "</PACKET>");
                }
                //// <<< PACKET TRACE

                this.objExec = execObj;
                if (this.Encoding == Encoding.Unicode)
                {
                    replyPacket = new MaxDBUnicodeReplyPacket(this.Execute(requestPacket, requestLen));
                }
                else
                {
                    replyPacket = new MaxDBReplyPacket(this.Execute(requestPacket, requestLen));
                }

                //// >>> PACKET TRACE
                if (this.logger.TraceFull)
                {
                    dt = DateTime.Now;
                    this.logger.SqlTrace(dt, $"<PACKET>{replyPacket.DumpPacket()}");

                    int segm = replyPacket.FirstSegment();
                    while (segm != -1)
                    {
                        this.logger.SqlTrace(dt, replyPacket.DumpSegment(dt));
                        segm = replyPacket.NextSegment();
                    }

                    this.logger.SqlTrace(dt, "</PACKET>");
                }
                //// <<< PACKET TRACE

                // get return code
                localWeakReturnCode = replyPacket.WeakReturnCode;

                if (localWeakReturnCode != -8)
                {
                    this.FreeRequestPacket(requestPacket);
                }

                if (!this.AutoCommit && !isParse)
                {
                    this.inTransaction = true;
                }

                // if it is not completely forbidden, we will send the drop
                if (gcFlags != GCMode.NONE)
                {
                    if (this.nonRecyclingExecutions > 20 && localWeakReturnCode == 0)
                    {
                        this.nonRecyclingExecutions = 0;
                        if (this.garbageParseids != null && this.garbageParseids.IsPending)
                        {
                            this.garbageParseids.EmptyCan(this, connArgs);
                        }

                        this.nonRecyclingExecutions = 0;
                    }
                }
            }
            catch (MaxDBException)
            {
                // if a reconnect is forbidden or we are in the process of a
                // reconnect or we are in a (now rolled back) transaction
                if (this.inReconnect || this.inTransaction)
                {
                    throw;
                }
                else
                {
                    this.TryReconnect(connArgs);
                    this.inTransaction = false;
                }
            }
            finally
            {
                this.objExec = null;
            }

            if (!ignoreErrors && localWeakReturnCode != 0)
            {
                throw replyPacket.CreateException();
            }

            return replyPacket;
        }

        private static string StripString(string str) =>
           !(str.StartsWith("\"", StringComparison.InvariantCulture) && str.EndsWith("\"", StringComparison.InvariantCulture)) ? str.ToUpperInvariant() : str.Substring(1, str.Length - 2);

        private bool InitiateChallengeResponse(ConnectArgs connArgs, MaxDBRequestPacket requestPacket, string user, Auth auth)
        {
            if (requestPacket.InitChallengeResponse(user, auth.ClientChallenge))
            {
                var replyPacket = this.Execute(connArgs, requestPacket, this, GCMode.DELAYED);
                auth.ParseServerChallenge(replyPacket.VarDataPart);
                return true;
            }
            else
            {
                return false;
            }
        }

        private MaxDBConnectPacket GetConnectReply()
        {
            byte[] replyBuffer = new byte[HeaderOffset.END + ConnectPacketOffset.END];

            int len = this.Socket.Stream.Read(replyBuffer, 0, replyBuffer.Length);
            if (len <= HeaderOffset.END)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.RECV_CONNECT));
            }

            var replyPacket = new MaxDBConnectPacket(
                replyBuffer,
                replyBuffer[HeaderOffset.END + ConnectPacketOffset.MessCode + 1] == SwapMode.Swapped);

            int actLen = replyPacket.ActSendLength;
            if (actLen < 0 || actLen > 500 * 1024)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.REPLY_GARBLED));
            }

            int bytesRead;

            while (len < actLen)
            {
                bytesRead = this.Socket.Stream.Read(replyPacket.GetArrayData(), len, actLen - len);

                if (bytesRead <= 0)
                {
                    break;
                }

                len += bytesRead;

                if (!this.Socket.DataAvailable)
                {
                    // wait for end of data
                    System.Threading.Thread.Sleep(this.ts);
                }
            }

            if (len < actLen)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.REPLY_GARBLED));
            }

            if (len > actLen)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CHUNKOVERFLOW, actLen, len, replyBuffer.Length));
            }

            this.iSender = replyPacket.PacketSender;

            return replyPacket;
        }
    }
}
