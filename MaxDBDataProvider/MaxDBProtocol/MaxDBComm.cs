//	Copyright (C) 2005-2006 Dmitry S. Kataev
//	Copyright (C) 2002-2003 SAP AG
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
using System.Net.Sockets;
using MaxDB.Data.Utilities;
using System.Globalization;
using System.Text;
using System.Collections;
using System.Data;
using System.Runtime.CompilerServices;
#if NET20
using System.Collections.Generic;
#endif // NET20

namespace MaxDB.Data.MaxDBProtocol
{

	/// <summary>
	/// Summary description for MaxDBComm.
	/// </summary>
	internal sealed class MaxDBComm : IDisposable
	{
		public IsolationLevel mIsolationLevel = IsolationLevel.Unspecified;
		public MaxDBConnectionStringBuilder mConnStrBuilder;
		public DateTime openTime;
#if SAFE

		public IMaxDBSocket mSocket;
		private MaxDBLogger mLogger;
		private string strDbName;
		private int iPort;
		private int iSender;
		private bool bIsAuthAllowed;
		private bool bIsServerLittleEndian;
		private int iMaxCmdSize;
		private bool bSession;
		private TimeSpan ts = new TimeSpan(1);

#if NET20
		public Stack<MaxDBRequestPacket> mPacketPool = new Stack<MaxDBRequestPacket>();
		public Stack<MaxDBUnicodeRequestPacket> mUnicodePacketPool = new Stack<MaxDBUnicodeRequestPacket>();
#else
        public Stack mPacketPool = new Stack();
        public Stack mUnicodePacketPool = new Stack();
#endif // NET20
		public Encoding mEncoding = Encoding.ASCII;
		public int iKernelVersion; // Version without patch level, e.g. 70402 or 70600.

		public bool bAutoCommit;
		public GarbageParseId mGarbageParseids;
		public object objExec;
		private static string strSyncObj = string.Empty;
		public int bNonRecyclingExecutions;
		public bool bInTransaction;
		public bool bInReconnect;
		public int iCursorId;
		public ParseInfoCache mParseCache;

		public int iSessionID = -1;
		private static byte[] byDefFeatureSet = { 1, 0, 2, 0, 3, 0, 4, 0, 5, 0 };
		private byte[] byKernelFeatures = new byte[byDefFeatureSet.Length];

		public MaxDBComm(MaxDBLogger logger)
		{
			mLogger = logger;
		}

		public void Connect(string dbname, int port)
		{
			try
			{
				//for (int i = 0; i < 2; i++)
				//{
				//    byte[] array = System.Text.Encoding.ASCII.GetBytes("GET / HTTP/1.0\r\nAccept: */*\r\nUser-Agent: Mentalis.org SecureSocket\r\nHost: 192.168.22.220\r\n\r\n");
				//    mSocket.Stream.Write(array, 0, array.Length);
				//    byte[] answer = new byte[1024];
				//    mSocket.Stream.Read(answer, 0, 1024);
				//    string sss = System.Text.Encoding.ASCII.GetString(answer);
				//}

				ConnectPacketData connData = new ConnectPacketData();
				connData.DBName = dbname;
				connData.Port = port;
				connData.MaxSegmentSize = 1024 * 32;
				MaxDBConnectPacket request = new MaxDBConnectPacket(new byte[HeaderOffset.END + ConnectPacketOffset.END], connData);
				request.FillHeader(RSQLTypes.INFO_REQUEST, iSender);
				request.FillPacketLength();
				request.SetSendLength(request.PacketLength);
				mSocket.Stream.Write(request.GetArrayData(), 0, request.PacketLength);

				MaxDBConnectPacket reply = GetConnectReply();
				int returnCode = reply.ReturnCode;
				if (returnCode != 0)
				{
					Close(true, false);
					throw new MaxDBCommunicationException(returnCode);
				}

				if (string.Compare(dbname.Trim(), reply.ClientDB.Trim(), true, CultureInfo.InvariantCulture) != 0)
				{
					Close(true, false);
					throw new MaxDBCommunicationException(RTEReturnCodes.SQLSERVER_DB_UNKNOWN);
				}

				if (mSocket.ReopenSocketAfterInfoPacket)
				{
					IMaxDBSocket new_socket = mSocket.Clone();
					Close(true, false);
					mSocket = new_socket;
				}

				bSession = true;
				strDbName = dbname;
				iPort = port;

				connData.DBName = dbname;
				connData.Port = port;
				connData.MaxSegmentSize = reply.PacketSize;
				connData.MaxDataLen = reply.MaxDataLength;
				connData.PacketSize = reply.PacketSize;
				connData.MinReplySize = reply.MinReplySize;

				MaxDBConnectPacket db_request = new MaxDBConnectPacket(new byte[HeaderOffset.END + reply.MaxDataLength], connData);
				db_request.FillHeader(RSQLTypes.USER_CONN_REQUEST, iSender);
				db_request.FillPacketLength();
				db_request.SetSendLength(db_request.PacketLength);
				mSocket.Stream.Write(db_request.GetArrayData(), 0, db_request.PacketLength);

				reply = GetConnectReply();
				bIsAuthAllowed = reply.IsAuthAllowed;
				bIsServerLittleEndian = reply.IsLittleEndian;
				iMaxCmdSize = reply.MaxDataLength - reply.MinReplySize;
			}
			catch (Exception ex)
			{
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.HOST_CONNECT_FAILED, mSocket.Host, mSocket.Port), ex);
			}
		}

		public void Reconnect()
		{
			IMaxDBSocket new_socket = mSocket.Clone();
			Close(true, false);
			mSocket = new_socket;
			if (bSession)
				Connect(strDbName, iPort);
			else
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.ADMIN_RECONNECT, CommError.ErrorText[RTEReturnCodes.SQLTIMEOUT]));
		}

		private MaxDBConnectPacket GetConnectReply()
		{
			byte[] replyBuffer = new byte[HeaderOffset.END + ConnectPacketOffset.END];

			int len = mSocket.Stream.Read(replyBuffer, 0, replyBuffer.Length);
			if (len <= HeaderOffset.END)
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.RECV_CONNECT));

			MaxDBConnectPacket replyPacket = new MaxDBConnectPacket(replyBuffer,
				replyBuffer[HeaderOffset.END + ConnectPacketOffset.MessCode + 1] == SwapMode.Swapped);

			int actLen = replyPacket.ActSendLength;
			if (actLen < 0 || actLen > 500 * 1024)
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.REPLY_GARBLED));

			int bytesRead;

			while (len < actLen)
			{
				bytesRead = mSocket.Stream.Read(replyPacket.GetArrayData(), len, actLen - len);

				if (bytesRead <= 0)
					break;

				len += bytesRead;

				if (!mSocket.DataAvailable) System.Threading.Thread.Sleep(ts); //wait for end of data
			};

			if (len < actLen)
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.REPLY_GARBLED));

			if (len > actLen)
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CHUNKOVERFLOW, actLen, len, replyBuffer.Length));

			iSender = replyPacket.PacketSender;

			return replyPacket;
		}

		public void Close(bool closeSocket, bool release)
		{
			if (mSocket != null && mSocket.Stream != null)
			{
				iSessionID = -1;

				if (release)
				{
					try
					{
						if (mGarbageParseids != null)
							mGarbageParseids.EmptyCan();
						ExecuteSqlString(new ConnectArgs(), "ROLLBACK WORK RELEASE", GCMode.GC_NONE);
					}
					catch (MaxDBException)
					{
					}
				}

				MaxDBConnectPacket request = new MaxDBConnectPacket(new byte[HeaderOffset.END]);
				request.FillHeader(RSQLTypes.USER_RELEASE_REQUEST, iSender);
				request.SetSendLength(HeaderOffset.END);
				mSocket.Stream.Write(request.GetArrayData(), 0, request.Length);
				if (closeSocket)
					mSocket.Close();
			}
		}

		public void Cancel()
		{
			try
			{
				if (mSocket != null && mSocket.Stream != null)
				{
					IMaxDBSocket cancel_socket = mSocket.Clone();
					ConnectPacketData connData = new ConnectPacketData();
					connData.DBName = strDbName;
					connData.Port = iPort;
					connData.MaxSegmentSize = 1024 * 32;
					MaxDBConnectPacket request = new MaxDBConnectPacket(new byte[HeaderOffset.END + ConnectPacketOffset.END], connData);
					request.FillHeader(RSQLTypes.USER_CANCEL_REQUEST, iSender);
					request.WriteInt32(iSender, HeaderOffset.ReceiverRef);
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

		public byte[] Execute(MaxDBRequestPacket userPacket, int len)
		{
			MaxDBPacket rawPacket = new MaxDBPacket(userPacket.GetArrayData(), 0, userPacket.Swapped);
			rawPacket.FillHeader(RSQLTypes.USER_DATA_REQUEST, iSender);
			rawPacket.SetSendLength(len + HeaderOffset.END);

			mSocket.Stream.Write(rawPacket.GetArrayData(), 0, len + HeaderOffset.END);
			byte[] headerBuf = new byte[HeaderOffset.END];

			int headerLength = mSocket.Stream.Read(headerBuf, 0, headerBuf.Length);

			if (headerLength != HeaderOffset.END)
				throw new MaxDBCommunicationException(RTEReturnCodes.SQLRECEIVE_LINE_DOWN);

			// auto-detect header byte-order
			//ulong littleEndianValue = headerBuf[0] + headerBuf[1] * 0x100UL +
			//                headerBuf[2] * 0x10000UL + headerBuf[3] * 0x1000000UL;
			//ulong bigEndianValue = headerBuf[0] * 0x1000000UL + headerBuf[1] * 0x10000UL +
			//                headerBuf[2] * 0x100UL + headerBuf[3];
			//MaxDBConnectPacket header = new MaxDBConnectPacket(headerBuf, littleEndianValue < bigEndianValue);
			MaxDBConnectPacket header = new MaxDBConnectPacket(headerBuf, bIsServerLittleEndian);

			int returnCode = header.ReturnCode;
			if (returnCode != 0)
				throw new MaxDBCommunicationException(returnCode);

			byte[] packetBuf = new byte[header.MaxSendLength - HeaderOffset.END];
			int replyLen = HeaderOffset.END;
			int bytesRead;

			while (replyLen < header.ActSendLength)
			{
				bytesRead = mSocket.Stream.Read(packetBuf, replyLen - HeaderOffset.END, header.ActSendLength - replyLen);
				if (bytesRead <= 0)
					break;
				replyLen += bytesRead;
				if (!mSocket.DataAvailable) System.Threading.Thread.Sleep(ts); //wait for the end of data
			};

			if (replyLen < header.ActSendLength)
				throw new MaxDBCommunicationException(RTEReturnCodes.SQLRECEIVE_LINE_DOWN);

			if (replyLen > header.ActSendLength)
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CHUNKOVERFLOW,
					header.ActSendLength, replyLen, packetBuf.Length + HeaderOffset.END));

			return packetBuf;
		}

		public void SetKernelFeatureRequest(int feature)
		{
			byKernelFeatures[2 * (feature - 1) + 1] = 1;
		}

		public bool IsKernelFeatureSupported(int feature)
		{
			return (byKernelFeatures[2 * (feature - 1) + 1] == 1) ? true : false;
		}

		private static string StripString(string str)
		{
			if (!(str.StartsWith("\"") && str.EndsWith("\"")))
				return str.ToUpper(CultureInfo.InvariantCulture);
			else
				return str.Substring(1, str.Length - 2);
		}

		public void Open(ConnectArgs connArgs)
		{
			Open(connArgs, true);
		}

		public void Open(ConnectArgs connArgs, bool initSocket)
		{
			if (initSocket)
			{
				if (mConnStrBuilder.Encrypt)
				{
					if (connArgs.port == 0) connArgs.port = Ports.DefaultSecure;
					mSocket = new SslSocketClass(connArgs.host, connArgs.port, mConnStrBuilder.Timeout, true,
						mConnStrBuilder.SslCertificateName != null ? mConnStrBuilder.SslCertificateName : connArgs.host);
				}
				else
				{
					if (connArgs.port == 0) connArgs.port = Ports.Default;
					mSocket = new SocketClass(connArgs.host, connArgs.port, mConnStrBuilder.Timeout, true);
				}
			}

			string username = connArgs.username;
			if (username == null)
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.NOUSER));

			username = StripString(username);

			string password = connArgs.password;
			if (password == null)
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.NOPASSWORD));

			password = StripString(password);

			byte[] passwordBytes = Encoding.ASCII.GetBytes(password);

			DateTime currentDt = DateTime.Now;

			//>>> SQL TRACE
			if (mLogger.TraceSQL)
			{
				mLogger.SqlTrace(currentDt, "::CONNECT");
				mLogger.SqlTrace(currentDt, "SERVERNODE: '" + connArgs.host + (connArgs.port > 0 ? ":" +
					connArgs.port.ToString(CultureInfo.InvariantCulture) : string.Empty) + "'");
				mLogger.SqlTrace(currentDt, "SERVERDB  : '" + connArgs.dbname + "'");
				mLogger.SqlTrace(currentDt, "USER  : '" + connArgs.username + "'");
			}
			//<<< SQL TRACE

			Connect(connArgs.dbname, connArgs.port);

			string connectCmd;
			byte[] crypted;
			mUnicodePacketPool.Clear();
			mPacketPool.Clear();
			mEncoding = Encoding.ASCII;
			MaxDBRequestPacket requestPacket = GetRequestPacket();
			Auth auth = null;
			bool isChallengeResponseSupported = false;
			if (bIsAuthAllowed)
			{
				try
				{
					auth = new Auth();
					isChallengeResponseSupported = InitiateChallengeResponse(connArgs, requestPacket, username, auth);
					if (password.Length > auth.MaxPasswordLength && auth.MaxPasswordLength > 0)
						password = password.Substring(0, auth.MaxPasswordLength);
				}
				catch (MaxDBException ex)
				{
					isChallengeResponseSupported = false;
					if (ex.ErrorCode == -5015)
					{
						try
						{
							Reconnect();
						}
						catch (MaxDBException exc)
						{
							throw new MaxDBConnectionException(exc);
						}
					}
					else
						throw;
				}
			}

#if (NET20 || MONO) && SAFE
			if (mConnStrBuilder.Encrypt && !isChallengeResponseSupported)
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONNECTION_CHALLENGERESPONSENOTSUPPORTED));
#endif // (NET20 || MONO) && SAFE

			/*
            * build connect statement
            */
			connectCmd = "CONNECT " + connArgs.username + " IDENTIFIED BY :PW SQLMODE " + SqlModeName.Value[(byte)mConnStrBuilder.Mode];
			if (mConnStrBuilder.Timeout > 0)
				connectCmd += " TIMEOUT " + mConnStrBuilder.Timeout;
			if (mIsolationLevel != IsolationLevel.Unspecified)
				connectCmd += " ISOLATION LEVEL " + MaxDBConnection.MapIsolationLevel(mIsolationLevel).ToString(CultureInfo.InvariantCulture);
			if (mConnStrBuilder.CacheLimit > 0)
				connectCmd += " CACHELIMIT " + mConnStrBuilder.CacheLimit;
			if (mConnStrBuilder.SpaceOption)
			{
				connectCmd += " SPACE OPTION ";
				SetKernelFeatureRequest(Feature.SpaceOption);
			}

			//>>> SQL TRACE
			if (mLogger.TraceSQL)
				mLogger.SqlTrace(currentDt, "CONNECT COMMAND: " + connectCmd);
			//<<< SQL TRACE

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
				requestPacket.AddDataString(TermID);
				requestPacket.PartArguments++;
			}
			else
			{
				requestPacket.AddClientProofPart(auth.GetClientProof(passwordBytes));
				requestPacket.AddClientIdPart(TermID);
			}

			byDefFeatureSet.CopyTo(byKernelFeatures, 0);

			SetKernelFeatureRequest(Feature.MultipleDropParseid);
			SetKernelFeatureRequest(Feature.CheckScrollableOption);
			requestPacket.AddFeatureRequestPart(byKernelFeatures);

			// execute
			MaxDBReplyPacket replyPacket = Execute(connArgs, requestPacket, this, GCMode.GC_DELAYED);
			iSessionID = replyPacket.SessionID;
			mEncoding = replyPacket.IsUnicode ? Encoding.Unicode : Encoding.ASCII;

			iKernelVersion = 10000 * replyPacket.KernelMajorVersion + 100 * replyPacket.KernelMinorVersion + replyPacket.KernelCorrectionLevel;
			byte[] featureReturn = replyPacket.Features;

			if (featureReturn != null)
				byKernelFeatures = featureReturn;
			else
				byDefFeatureSet.CopyTo(byKernelFeatures, 0);

			if (mConnStrBuilder.Cache != null && mConnStrBuilder.Cache.Length > 0)
				mParseCache = new ParseInfoCache(mConnStrBuilder.Cache, mConnStrBuilder.CacheSize);

			//>>> SQL TRACE
			if (mLogger.TraceSQL)
				mLogger.SqlTrace(DateTime.Now, "SESSION ID: " + iSessionID);
			//<<< SQL TRACE

			openTime = DateTime.Now;
		}

		public void TryReconnect(ConnectArgs connArgs)
		{
			lock (strSyncObj)
			{
				if (mParseCache != null)
					mParseCache.Clear();
				mPacketPool.Clear();
				mUnicodePacketPool.Clear();
				bInReconnect = true;
				try
				{
					Reconnect();
					Open(connArgs);
				}
				catch (MaxDBException ex)
				{
					throw new MaxDBConnectionException(ex);
				}
				finally
				{
					bInReconnect = false;
				}
				throw new MaxDBTimeoutException();
			}
		}

		private string TermID
		{
			get
			{
				return ("ado.net@" + this.GetHashCode().ToString("x", CultureInfo.InvariantCulture)).PadRight(18);
			}
		}

		private bool InitiateChallengeResponse(ConnectArgs connArgs, MaxDBRequestPacket requestPacket, string user, Auth auth)
		{
			if (requestPacket.InitChallengeResponse(user, auth.ClientChallenge))
			{
				MaxDBReplyPacket replyPacket = Execute(connArgs, requestPacket, this, GCMode.GC_DELAYED);
				auth.ParseServerChallenge(replyPacket.VarDataPart);
				return true;
			}
			else
				return false;
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		internal MaxDBRequestPacket GetRequestPacket()
		{
			MaxDBRequestPacket packet;

			if (mEncoding == Encoding.Unicode)
			{
				if (mUnicodePacketPool.Count == 0)
					packet = new MaxDBUnicodeRequestPacket(new byte[HeaderOffset.END + iMaxCmdSize], Consts.AppID, Consts.AppVersion);
				else
					packet =
#if NET20
 mUnicodePacketPool.Pop();
#else
                        (MaxDBUnicodeRequestPacket)mUnicodePacketPool.Pop();
#endif // NET20
			}
			else
			{
				if (mPacketPool.Count == 0)
					packet = new MaxDBRequestPacket(new byte[HeaderOffset.END + iMaxCmdSize], Consts.AppID, Consts.AppVersion);
				else
					packet =
#if NET20
 mPacketPool.Pop();
#else
                        (MaxDBRequestPacket)mPacketPool.Pop();
#endif // NET20
			}

			packet.SwitchSqlMode((byte)mConnStrBuilder.Mode);
			return packet;
		}

		internal void FreeRequestPacket(MaxDBRequestPacket requestPacket)
		{
			if (mEncoding == Encoding.Unicode)
				mUnicodePacketPool.Push(requestPacket as MaxDBUnicodeRequestPacket);
			else
				mPacketPool.Push(requestPacket);
		}

		internal MaxDBReplyPacket Execute(ConnectArgs connArgs, MaxDBRequestPacket requestPacket, object execObj, int gcFlags)
		{
			return Execute(connArgs, requestPacket, false, false, execObj, gcFlags);
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		internal MaxDBReplyPacket Execute(ConnectArgs connArgs, MaxDBRequestPacket requestPacket, bool ignoreErrors, bool isParse, object execObj, int gcFlags)
		{
			int requestLen;
			MaxDBReplyPacket replyPacket = null;
			int localWeakReturnCode = 0;

			if (iSessionID >= 0)
				if (gcFlags == GCMode.GC_ALLOWED)
				{
					if (mGarbageParseids != null && mGarbageParseids.IsPending)
						mGarbageParseids.EmptyCan(requestPacket);
				}
				else
				{
					if (mGarbageParseids != null && mGarbageParseids.IsPending)
						bNonRecyclingExecutions++;
				}

			requestPacket.Close();

			requestLen = requestPacket.PacketLength;

			try
			{
				DateTime dt = DateTime.Now;

				//>>> PACKET TRACE
				if (mLogger.TraceFull)
				{
					mLogger.SqlTrace(dt, "<PACKET>" + requestPacket.DumpPacket());

					int segm = requestPacket.FirstSegment();
					while (segm != -1)
					{
						mLogger.SqlTrace(dt, requestPacket.DumpSegment(dt));
						segm = requestPacket.NextSegment();
					}

					mLogger.SqlTrace(dt, "</PACKET>");
				}
				//<<< PACKET TRACE

				objExec = execObj;
				if (mEncoding == Encoding.Unicode)
					replyPacket = new MaxDBUnicodeReplyPacket(Execute(requestPacket, requestLen));
				else
					replyPacket = new MaxDBReplyPacket(Execute(requestPacket, requestLen));

				//>>> PACKET TRACE
				if (mLogger.TraceFull)
				{
					dt = DateTime.Now;
					mLogger.SqlTrace(dt, "<PACKET>" + replyPacket.DumpPacket());

					int segm = replyPacket.FirstSegment();
					while (segm != -1)
					{
						mLogger.SqlTrace(dt, replyPacket.DumpSegment(dt));
						segm = replyPacket.NextSegment();
					}
					mLogger.SqlTrace(dt, "</PACKET>");
				}
				//<<< PACKET TRACE

				// get return code
				localWeakReturnCode = replyPacket.WeakReturnCode;

				if (localWeakReturnCode != -8)
					FreeRequestPacket(requestPacket);

				if (!bAutoCommit && !isParse)
					bInTransaction = true;

				// if it is not completely forbidden, we will send the drop
				if (gcFlags != GCMode.GC_NONE)
				{
					if (bNonRecyclingExecutions > 20 && localWeakReturnCode == 0)
					{
						bNonRecyclingExecutions = 0;
						if (mGarbageParseids != null && mGarbageParseids.IsPending)
							mGarbageParseids.EmptyCan(this, connArgs);
						bNonRecyclingExecutions = 0;
					}
				}
			}
			catch (MaxDBException)
			{
				// if a reconnect is forbidden or we are in the process of a
				// reconnect or we are in a (now rolled back) transaction
				if (bInReconnect || bInTransaction)
					throw;
				else
				{
					TryReconnect(connArgs);
					bInTransaction = false;
				}
			}
			finally
			{
				objExec = null;
			}
			if (!ignoreErrors && localWeakReturnCode != 0)
				throw replyPacket.CreateException();
			return replyPacket;
		}

		public void Cancel(object reqObj)
		{
			DateTime dt = DateTime.Now;
			//>>> SQL TRACE
			if (mLogger.TraceSQL)
			{
				mLogger.SqlTrace(dt, "::CANCEL");
				mLogger.SqlTrace(dt, "SESSION ID: " + iSessionID);
			}
			//<<< SQL TRACE
			if (objExec == reqObj)
				Cancel();
			else
			{
				//>>> SQL TRACE
				if (mLogger.TraceSQL)
				{
					mLogger.SqlTrace(dt, "RETURN     : 100");
					mLogger.SqlTrace(dt, "MESSAGE    : No active command found.");
				}
				//<<< SQL TRACE
			}
		}

		public void DropParseID(byte[] pid)
		{
			if (pid == null)
				return;
			if (mGarbageParseids == null)
				mGarbageParseids = new GarbageParseId(IsKernelFeatureSupported(Feature.MultipleDropParseid));
			mGarbageParseids.ThrowIntoGarbageCan(pid);
		}

		public void ExecuteSqlString(ConnectArgs connArgs, string cmd, int gcFlags)
		{
			MaxDBRequestPacket requestPacket = GetRequestPacket();
			requestPacket.InitDbs(bAutoCommit);
			requestPacket.AddString(cmd);
			try
			{
				Execute(connArgs, requestPacket, this, gcFlags);
			}
			catch (MaxDBTimeoutException)
			{
				//ignore
			}
		}

		public bool IsInTransaction
		{
			get
			{
				return !bAutoCommit && bInTransaction;
			}
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public void Commit(ConnectArgs connArgs)
		{
			// send commit
			ExecuteSqlString(connArgs, "COMMIT WORK", GCMode.GC_ALLOWED);
			bInTransaction = false;
		}

		public void Rollback(ConnectArgs connArgs)
		{
			// send rollback
			ExecuteSqlString(connArgs, "ROLLBACK WORK", GCMode.GC_ALLOWED);
			bInTransaction = false;
		}

		public string NextCursorName
		{
			get
			{
				return Consts.CursorPrefix + iCursorId++;
			}
		}
#else
		public IntPtr mEnviromentHandler = IntPtr.Zero;
		public IntPtr mConnectionPropertiesHandler = IntPtr.Zero;
		public IntPtr mConnectionHandler = IntPtr.Zero;

		private unsafe IntPtr GetRuntimeHandler(ref byte[] errorText)
		{
			IntPtr mRuntimeHandler = IntPtr.Zero;

			fixed (byte* errorPtr = errorText)
				mRuntimeHandler = UnsafeNativeMethods.ClientRuntime_GetClientRuntime(errorPtr, errorText.Length);

			return mRuntimeHandler;
		}

		public MaxDBComm()
		{
			byte[] errorText = new byte[256];
			IntPtr mRuntimeHandler = GetRuntimeHandler(ref errorText);

			if (mRuntimeHandler == IntPtr.Zero)
				throw new MaxDBException(Encoding.ASCII.GetString(errorText));

			mEnviromentHandler = UnsafeNativeMethods.SQLDBC_Environment_new_SQLDBC_Environment(mRuntimeHandler);
		}

		public void Open(ConnectArgs connArgs)
		{
			mConnectionHandler = UnsafeNativeMethods.SQLDBC_Environment_createConnection(mEnviromentHandler);

			mConnectionPropertiesHandler = UnsafeNativeMethods.SQLDBC_ConnectProperties_new_SQLDBC_ConnectProperties();

			if (mConnStrBuilder.Timeout > 0)
				UnsafeNativeMethods.SQLDBC_ConnectProperties_setProperty(mConnectionPropertiesHandler, "TIMEOUT",
					mConnStrBuilder.Timeout.ToString(CultureInfo.InvariantCulture));
			if (mIsolationLevel != IsolationLevel.Unspecified)
				UnsafeNativeMethods.SQLDBC_ConnectProperties_setProperty(mConnectionPropertiesHandler, "ISOLATIONLEVEL",
					MaxDBConnection.MapIsolationLevel(mIsolationLevel).ToString(CultureInfo.InvariantCulture));
			if (mConnStrBuilder.SpaceOption)
				UnsafeNativeMethods.SQLDBC_ConnectProperties_setProperty(mConnectionPropertiesHandler, "SPACEOPTION", "1");
			if (mConnStrBuilder.CacheSize > 0)
				UnsafeNativeMethods.SQLDBC_ConnectProperties_setProperty(mConnectionPropertiesHandler, "STATEMENTCACHESIZE",
					mConnStrBuilder.CacheSize.ToString(CultureInfo.InvariantCulture));
			if (mConnStrBuilder.CacheLimit > 0)
				UnsafeNativeMethods.SQLDBC_ConnectProperties_setProperty(mConnectionPropertiesHandler, "CACHELIMIT",
					mConnStrBuilder.CacheLimit.ToString(CultureInfo.InvariantCulture));

			UnsafeNativeMethods.SQLDBC_Connection_setSQLMode(mConnectionHandler, (byte)mConnStrBuilder.Mode);

			if (UnsafeNativeMethods.SQLDBC_Connection_connectASCII(mConnectionHandler,
				"maxdb:remote://" + connArgs.host + "/database/" + connArgs.dbname, connArgs.dbname,
				connArgs.username, connArgs.password, mConnectionPropertiesHandler) != SQLDBC_Retcode.SQLDBC_OK)
				MaxDBException.ThrowException(MaxDBMessages.Extract(MaxDBError.HOST_CONNECT_FAILED, connArgs.host, connArgs.port),
					UnsafeNativeMethods.SQLDBC_Connection_getError(mConnectionHandler));

			openTime = DateTime.Now;
		}

		public void Close()
		{
			if (mConnectionHandler != IntPtr.Zero)
			{
				UnsafeNativeMethods.SQLDBC_ConnectProperties_delete_SQLDBC_ConnectProperties(mConnectionPropertiesHandler);
				mConnectionPropertiesHandler = IntPtr.Zero;

				UnsafeNativeMethods.SQLDBC_Connection_close(mConnectionHandler);

				UnsafeNativeMethods.SQLDBC_Environment_releaseConnection(mEnviromentHandler, mConnectionHandler);
				mConnectionHandler = IntPtr.Zero;
			}
		}
#endif // SAFE

		#region IDisposable Members

		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing)
		{
			if (disposing)
			{
#if SAFE
				Close(true, false);
#else
				Close();
				UnsafeNativeMethods.SQLDBC_Environment_delete_SQLDBC_Environment(mEnviromentHandler);
				mEnviromentHandler = IntPtr.Zero;
#endif // !SAFE
			}
		}

		#endregion

	}
}
