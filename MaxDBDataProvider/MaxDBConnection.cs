//	Copyright (C) 2005-2006 Dmitry S. Kataev
//	Copyright (C) 2002-2003 SAP AG
//
//	This program is free software; you can redistribute it and/or
//	modify it under the terms of the GNU General Public License
//	as published by the Free Software Foundation; either version 2
//  of the License, or (at your option) any later version.
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
using System.Data;
using System.Data.Common;
using System.Text;
using System.Collections;
#if NET20
using System.Collections.Generic;
#endif
using System.Runtime.CompilerServices;
using MaxDB.Data.MaxDBProtocol;
using MaxDB.Data.Utilities;
using System.Globalization;

namespace MaxDB.Data
{
    internal struct ConnectArgs
    {
        public string username;
        public string password;
        public string dbname;
        public string host;
        public int port;
    };

    /// <summary>
    /// SQL Mode
    /// </summary>
    /// <remarks>
    /// copy of vsp001::tsp1_sqlmode
    /// </remarks>
    public enum SqlMode
    {
        Nil = 0,
        SessionSqlMode = 1,
        Internal = 2,
        Ansi = 3,
        Db2 = 4,
        Oracle = 5,
        SapR3 = 6,
    }

    public class MaxDBConnection :
#if NET20
        DbConnection
#else
        IDbConnection, IDisposable
#endif // NET20
        
    {
        private MaxDBConnectionStringBuilder mConnStrBuilder;
        private string strConnection;

        private ConnectArgs mConnArgs;

#if SAFE
        #region "Native implementation parameters"

        internal MaxDBComm mComm;
#if NET20
        private Stack<MaxDBRequestPacket> mPacketPool = new Stack<MaxDBRequestPacket>();
        private Stack<MaxDBUnicodeRequestPacket> mUnicodePacketPool = new Stack<MaxDBUnicodeRequestPacket>();
#else
        private Stack mPacketPool = new Stack();
        private Stack mUnicodePacketPool = new Stack();
#endif // NET20
        internal bool bAutoCommit;
        private GarbageParseId mGarbageParseids;
        private object objExec;
        private static string strSyncObj = string.Empty;
        private int bNonRecyclingExecutions;
        private bool bInTransaction;
        private bool bInReconnect;
        private string strCache;
        private int iCursorId;
        internal ParseInfoCache mParseCache;

        internal int iSessionID = -1;
        private static byte[] byDefFeatureSet = { 1, 0, 2, 0, 3, 0, 4, 0, 5, 0 };
        private byte[] byKernelFeatures = new byte[byDefFeatureSet.Length];

#if NET20 || MONO
        private string strSslCertificateName;
        private bool bEncrypt;
#endif // NET20

        #endregion
#else
        #region "SQLDBC Wrapper parameters"

        private IntPtr mRuntimeHandler = IntPtr.Zero;
        internal IntPtr mEnviromentHandler = IntPtr.Zero;
        private IntPtr mConnectionPropertiesHandler = IntPtr.Zero;
		internal IntPtr mConnectionHandler = IntPtr.Zero;

		//we cache table names extracted from SELECT ... FOR UPDATE statement
		//SQLDBC library does not store it in its command cache!!!
		//hash algorithm is equal to the SQLDBC counterpart

#if NET20
        private class TableNameHashCodeProvider : IEqualityComparer
        {
            bool IEqualityComparer.Equals(object x, object y)
            {
                return (string)x == (string)y;
            }

            int IEqualityComparer.GetHashCode(object obj)
#else
		private class TableNameHashCodeProvider : IHashCodeProvider
		{
			int IHashCodeProvider.GetHashCode(object obj)
#endif // NET20
            {
				// the X31 hash formula is hash = (hash<<5) - hash + char(i) for i in 1 ... string length
				// as it degenerates when the input are 0's, a little bit decoration is added
				// to hash UTF8 data and UCS2 data equally. 
				// also chars >= 128 are completely skipped.

				string str = (string)obj;
				if (str.Length > 0)
				{
					int result = 0;
					
					foreach(char c in str)
					{
						byte[] b = BitConverter.GetBytes(c);
						if (b[0] < 128 && b[1] == 0)
							result = (result << 5) - result + b[0];
					}
					return result;
				}
				else
					return 0;
			}
		}

		internal Hashtable mTableNames = new Hashtable(new TableNameHashCodeProvider()
#if !NET20
			, new Comparer(System.Globalization.CultureInfo.InvariantCulture)
#endif // !NET20
            );

        #endregion
#endif // SAFE

        internal MaxDBLogger mLogger;
        internal IsolationLevel mIsolationLevel = IsolationLevel.Unspecified;
        internal bool bSpaceOption;
        private int iTimeout;
        private Encoding mEncoding = Encoding.ASCII;
        private SqlMode iMode = SqlMode.Internal;
        private int iKernelVer; // Version without patch level, e.g. 70402 or 70600.
        private int iCacheSize, iCacheLimit;

        // Always have a default constructor.
        public MaxDBConnection()
        {
        }

        // Have a constructor that takes a connection string.
        public MaxDBConnection(string connectionString)
        {
            strConnection = connectionString;
            mConnStrBuilder = new MaxDBConnectionStringBuilder(connectionString);
            SetConnectionParameters();
        }

        private void SetConnectionParameters()
        {
            if (mConnStrBuilder.DataSource != null)
            {
                string[] hostPort = mConnStrBuilder.DataSource.Split(':');
                mConnArgs.host = hostPort[0];
                mConnArgs.port = 0;
                try
                {
                    mConnArgs.port = int.Parse(hostPort[1], CultureInfo.InvariantCulture);
                }
                catch (IndexOutOfRangeException)
                {
                }
                catch (ArgumentNullException)
                {
                }
                catch (FormatException)
                {
                }
                catch (OverflowException)
                {
                }
            }

            mConnArgs.dbname = mConnStrBuilder.InitialCatalog;

            mConnArgs.username = mConnStrBuilder.UserId;

            mConnArgs.password = mConnStrBuilder.Password;

            iTimeout = mConnStrBuilder.Timeout;

            iMode = mConnStrBuilder.Mode;

            bSpaceOption = mConnStrBuilder.SpaceOption;

            iCacheSize = mConnStrBuilder.CacheSize;

            iCacheLimit = mConnStrBuilder.CacheLimit;

#if SAFE
            strCache = mConnStrBuilder.Cache;
#endif // SAFE

#if NET20 && SAFE
            if (mConnStrBuilder.SslCertificateName != null)
                strSslCertificateName = mConnStrBuilder.SslCertificateName;

            bEncrypt = mConnStrBuilder.Encrypt;
#endif // NET && SAFE
        }

        public Encoding DatabaseEncoding
        {
            get
            {
                return mEncoding;
            }
        }

        public SqlMode SqlMode
        {
            get
            {
                return iMode;
            }
            set
            {
                iMode = value;
            }
        }

        public bool AutoCommit
        {
            get
            {
                if (State != ConnectionState.Open)
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONNECTION_NOTOPENED));

#if SAFE
                return bAutoCommit;
#else
				return UnsafeNativeMethods.SQLDBC_Connection_getAutoCommit(mConnectionHandler) == SQLDBC_BOOL.SQLDBC_TRUE;
#endif // SAFE
            }
            set
            {
                if (State != ConnectionState.Open)
                    throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONNECTION_NOTOPENED));

                //>>> SQL TRACE
                if (mLogger.TraceSQL)
                    mLogger.SqlTrace(DateTime.Now, "::SET AUTOCOMMIT " + (value ? "ON" : "OFF"));
                //<<< SQL TRACE				

#if SAFE
                bAutoCommit = value;
#else
				UnsafeNativeMethods.SQLDBC_Connection_setAutoCommit(mConnectionHandler, value ? SQLDBC_BOOL.SQLDBC_TRUE : SQLDBC_BOOL.SQLDBC_FALSE);
#endif // SAFE
            }
        }

#if NET20
        public override string ServerVersion
#else
        public string ServerVersion
#endif // NET20
        {
            get
            {
                int correction_level = iKernelVer % 100;
                int minor_release = ((iKernelVer - correction_level) % 10000) / 100;
                int mayor_release = (iKernelVer - minor_release * 100 - correction_level) / 10000;
                return mayor_release.ToString(CultureInfo.InvariantCulture) + "." + 
                    minor_release.ToString(CultureInfo.InvariantCulture) + "." + 
                    correction_level.ToString(CultureInfo.InvariantCulture);
            }
        }

        private static int MapIsolationLevel(IsolationLevel level)
        {
            switch (level)
            {
                case IsolationLevel.ReadUncommitted:
                    return 0;
                case IsolationLevel.ReadCommitted:
                    return 1;
                case IsolationLevel.RepeatableRead:
                    return 2;
                case IsolationLevel.Serializable:
                    return 3;
                default:
                    return 1;
            }
        }

        private void SetIsolationLevel(IsolationLevel level)
        {
#if SAFE
            if (mIsolationLevel != level)
            {
                AssertOpen();
                string cmd = "SET ISOLATION LEVEL " + MapIsolationLevel(level).ToString(CultureInfo.InvariantCulture);
                MaxDBRequestPacket requestPacket = GetRequestPacket();
                byte oldMode = requestPacket.SwitchSqlMode((byte)SqlMode.Internal);
                requestPacket.InitDbsCommand(bAutoCommit, cmd);
                try
                {
                    Execute(requestPacket, this, GCMode.GC_ALLOWED);
                }
                catch (MaxDBTimeoutException)
                {
                    requestPacket.SwitchSqlMode(oldMode);
                }

                mIsolationLevel = level;
            }
#else
			mIsolationLevel = level;

			if(UnsafeNativeMethods.SQLDBC_Connection_setTransactionIsolation(mConnectionHandler, MapIsolationLevel(level)) != SQLDBC_Retcode.SQLDBC_OK) 
				MaxDBException.ThrowException(MaxDBMessages.Extract(MaxDBError.CONNECTION_ISOLATIONLEVEL), 
					UnsafeNativeMethods.SQLDBC_Connection_getError(mConnectionHandler));
#endif // SAFE
        }

        internal void AssertOpen()
        {
            if (State == ConnectionState.Closed)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.OBJECTISCLOSED));
        }

#if SAFE
        #region "Methods to support native protocol"

        private string TermID
        {
            get
            {
                return ("ado.net@" + this.GetHashCode().ToString("x", CultureInfo.InvariantCulture)).PadRight(18);
            }
        }

        private bool InitiateChallengeResponse(MaxDBRequestPacket requestPacket, string user, Auth auth)
        {
            if (requestPacket.InitChallengeResponse(user, auth.ClientChallenge))
            {
                MaxDBReplyPacket replyPacket = Execute(requestPacket, this, GCMode.GC_DELAYED);
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

            if (DatabaseEncoding == Encoding.Unicode)
            {
                if (mUnicodePacketPool.Count == 0)
                     packet = new MaxDBUnicodeRequestPacket(new byte[HeaderOffset.END + mComm.MaxCmdSize], Consts.AppID, Consts.AppVersion, iMode);
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
                    packet = new MaxDBRequestPacket(new byte[HeaderOffset.END + mComm.MaxCmdSize], Consts.AppID, Consts.AppVersion, iMode);
                else
                    packet =
#if NET20
                        mPacketPool.Pop();
#else
                        (MaxDBRequestPacket)mPacketPool.Pop();
#endif // NET20
            }

            return packet;
        }

        internal void FreeRequestPacket(MaxDBRequestPacket requestPacket)
        {
            if (DatabaseEncoding == Encoding.Unicode)
                mUnicodePacketPool.Push(requestPacket as MaxDBUnicodeRequestPacket);
            else
                mPacketPool.Push(requestPacket);
        }

        internal MaxDBReplyPacket Execute(MaxDBRequestPacket requestPacket, object execObj, int gcFlags)
        {
            return Execute(requestPacket, false, false, execObj, gcFlags);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        internal MaxDBReplyPacket Execute(MaxDBRequestPacket requestPacket, bool ignoreErrors, bool isParse, object execObj, int gcFlags)
        {
            int requestLen;
            MaxDBReplyPacket replyPacket = null;
            int localWeakReturnCode = 0;

            if (State != ConnectionState.Open)
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
                if (DatabaseEncoding == Encoding.Unicode)
                    replyPacket = new MaxDBUnicodeReplyPacket(mComm.Execute(requestPacket, requestLen));
                else
                    replyPacket = new MaxDBReplyPacket(mComm.Execute(requestPacket, requestLen));

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
                            mGarbageParseids.EmptyCan(this);
                        bNonRecyclingExecutions = 0;
                    }
                }
            }
            catch(MaxDBException)
            {
                // if a reconnect is forbidden or we are in the process of a
                // reconnect or we are in a (now rolled back) transaction
                if (bInReconnect || bInTransaction)
                    throw;
                else
                {
                    TryReconnect();
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

        private static string StripString(string str)
        {
            if (!(str.StartsWith("\"") && str.EndsWith("\"")))
                return str.ToUpper(CultureInfo.InvariantCulture);
            else
                return str.Substring(1, str.Length - 2);
        }

        private void DoConnect()
        {
            string username = mConnArgs.username;
            if (username == null)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.NOUSER));

            username = StripString(username);

            string password = mConnArgs.password;
            if (password == null)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.NOPASSWORD));

            password = StripString(password);

            byte[] passwordBytes = Encoding.ASCII.GetBytes(password);

            DateTime currentDt = DateTime.Now;

            //>>> SQL TRACE
            if (mLogger.TraceSQL)
            {
                mLogger.SqlTrace(currentDt, "::CONNECT");
                mLogger.SqlTrace(currentDt, "SERVERNODE: '" + mConnArgs.host + (mConnArgs.port > 0 ? ":" + 
                    mConnArgs.port.ToString(CultureInfo.InvariantCulture) : string.Empty) + "'");
                mLogger.SqlTrace(currentDt, "SERVERDB  : '" + mConnArgs.dbname + "'");
                mLogger.SqlTrace(currentDt, "USER  : '" + mConnArgs.username + "'");
            }
            //<<< SQL TRACE

            mComm.Connect(mConnArgs.dbname, mConnArgs.port);

            string connectCmd;
            byte[] crypted;
            mUnicodePacketPool.Clear();
            mPacketPool.Clear();
            mEncoding = Encoding.ASCII;
            MaxDBRequestPacket requestPacket = GetRequestPacket();
            Auth auth = null;
            bool isChallengeResponseSupported = false;
            if (mComm.IsAuthAllowed)
            {
                try
                {
                    auth = new Auth();
                    isChallengeResponseSupported = InitiateChallengeResponse(requestPacket, username, auth);
                    if (password.Length > auth.MaxPasswordLength && auth.MaxPasswordLength > 0)
                        password = password.Substring(0, auth.MaxPasswordLength);
                }
                catch(MaxDBException ex)
                {
                    isChallengeResponseSupported = false;
                    if (ex.ErrorCode == -5015)
                    {
                        try
                        {
                            mComm.Reconnect();
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
            if (bEncrypt && !isChallengeResponseSupported)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.CONNECTION_CHALLENGERESPONSENOTSUPPORTED));
#endif // (NET20 || MONO) && SAFE

            /*
            * build connect statement
            */
            connectCmd = "CONNECT " + mConnArgs.username + " IDENTIFIED BY :PW SQLMODE " + SqlModeName.Value[(byte)iMode];
            if (iTimeout > 0)
                connectCmd += " TIMEOUT " + iTimeout;
            if (mIsolationLevel != IsolationLevel.Unspecified)
                connectCmd += " ISOLATION LEVEL " + MapIsolationLevel(mIsolationLevel).ToString(CultureInfo.InvariantCulture);
            if (iCacheLimit > 0)
                connectCmd += " CACHELIMIT " + iCacheLimit;
            if (bSpaceOption)
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
            MaxDBReplyPacket replyPacket = Execute(requestPacket, this, GCMode.GC_DELAYED);
            iSessionID = replyPacket.SessionID;
            mEncoding = replyPacket.IsUnicode ? Encoding.Unicode : Encoding.ASCII;

            iKernelVer = 10000 * replyPacket.KernelMajorVersion + 100 * replyPacket.KernelMinorVersion + replyPacket.KernelCorrectionLevel;
            byte[] featureReturn = replyPacket.Features;

            if (featureReturn != null)
                byKernelFeatures = featureReturn;
            else
                byDefFeatureSet.CopyTo(byKernelFeatures, 0);

            if (strCache != null && strCache.Length > 0 && iCacheSize > 0)
                mParseCache = new ParseInfoCache(strCache, iCacheSize);

            //>>> SQL TRACE
            if (mLogger.TraceSQL)
                mLogger.SqlTrace(DateTime.Now, "SESSION ID: " + iSessionID);
            //<<< SQL TRACE
        }

        private void SetKernelFeatureRequest(int feature)
        {
            byKernelFeatures[2 * (feature - 1) + 1] = 1;
        }

        internal void Cancel(object reqObj)
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
                mComm.Cancel();
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

        private bool IsKernelFeatureSupported(int feature)
        {
            return (byKernelFeatures[2 * (feature - 1) + 1] == 1) ? true : false;
        }

        private void TryReconnect()
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
                    mComm.Reconnect();
                    DoConnect();
                }
                catch (MaxDBException conn_ex)
                {
                    throw new MaxDBConnectionException(conn_ex);
                }
                finally
                {
                    bInReconnect = false;
                }
                throw new MaxDBTimeoutException();
            }
        }

        internal void DropParseID(byte[] pid)
        {
            if (pid == null)
                return;
            if (mGarbageParseids == null)
                mGarbageParseids = new GarbageParseId(IsKernelFeatureSupported(Feature.MultipleDropParseid));
            mGarbageParseids.ThrowIntoGarbageCan(pid);
        }

        private void ExecSQLString(string cmd, int gcFlags)
        {
            MaxDBRequestPacket requestPacket = GetRequestPacket();
            requestPacket.InitDbs(bAutoCommit);
            requestPacket.AddString(cmd);
            try
            {
                Execute(requestPacket, this, gcFlags);
            }
            catch (MaxDBTimeoutException)
            {
                //ignore
            }
        }

        internal bool IsInTransaction
        {
            get
            {
                return (!bAutoCommit && bInTransaction);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        internal void Commit()
        {
            AssertOpen();

            // send commit
            ExecSQLString("COMMIT WORK", GCMode.GC_ALLOWED);
            bInTransaction = false;
        }

        internal void Rollback()
        {
            AssertOpen();

            // send rollback
            ExecSQLString("ROLLBACK WORK", GCMode.GC_ALLOWED);
            bInTransaction = false;
        }

        internal string NextCursorName
        {
            get
            {
                return Consts.CursorPrefix + iCursorId++;
            }
        }

        #endregion
#else
        #region "Unsafe methods"

		private unsafe void OpenConnection()
		{
			byte[] errorText = new byte[256];
			
			fixed(byte* errorPtr = errorText)
				mRuntimeHandler = UnsafeNativeMethods.ClientRuntime_GetClientRuntime(errorPtr, errorText.Length);

            if (mRuntimeHandler == IntPtr.Zero)
				throw new MaxDBException(Encoding.ASCII.GetString(errorText));

			mEnviromentHandler = UnsafeNativeMethods.SQLDBC_Environment_new_SQLDBC_Environment(mRuntimeHandler);

			mConnectionHandler = UnsafeNativeMethods.SQLDBC_Environment_createConnection(mEnviromentHandler);

			mConnectionPropertiesHandler = UnsafeNativeMethods.SQLDBC_ConnectProperties_new_SQLDBC_ConnectProperties();
 
			if (iTimeout > 0) 
				UnsafeNativeMethods.SQLDBC_ConnectProperties_setProperty(mConnectionPropertiesHandler, "TIMEOUT", 
                    iTimeout.ToString(CultureInfo.InvariantCulture));
			if (mIsolationLevel != IsolationLevel.Unspecified)
				UnsafeNativeMethods.SQLDBC_ConnectProperties_setProperty(mConnectionPropertiesHandler, "ISOLATIONLEVEL",
                    MapIsolationLevel(mIsolationLevel).ToString(CultureInfo.InvariantCulture));
			if (bSpaceOption) 
				UnsafeNativeMethods.SQLDBC_ConnectProperties_setProperty(mConnectionPropertiesHandler, "SPACEOPTION", "1");
            if (iCacheSize > 0)
                UnsafeNativeMethods.SQLDBC_ConnectProperties_setProperty(mConnectionPropertiesHandler, "STATEMENTCACHESIZE", 
                    iCacheSize.ToString(CultureInfo.InvariantCulture));
            if (iCacheLimit > 0)
                UnsafeNativeMethods.SQLDBC_ConnectProperties_setProperty(mConnectionPropertiesHandler, "CACHELIMIT", 
                    iCacheLimit.ToString(CultureInfo.InvariantCulture));
		
			UnsafeNativeMethods.SQLDBC_Connection_setSQLMode(mConnectionHandler, (byte)iMode);

			mLogger = new MaxDBLogger(this);

			if (UnsafeNativeMethods.SQLDBC_Connection_connectASCII(mConnectionHandler, mConnArgs.host, mConnArgs.dbname, mConnArgs.username, 
				mConnArgs.password, mConnectionPropertiesHandler) != SQLDBC_Retcode.SQLDBC_OK) 
				MaxDBException.ThrowException(MaxDBMessages.Extract(MaxDBError.HOST_CONNECT_FAILED, mConnArgs.host, mConnArgs.port),  
					UnsafeNativeMethods.SQLDBC_Connection_getError(mConnectionHandler));
		}

        #endregion
#endif // SAFE

        #region IDbConnection Members

#if NET20
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
#else
		IDbTransaction IDbConnection.BeginTransaction(IsolationLevel isolationLevel)
#endif // NET20
        {
            return BeginTransaction(isolationLevel);
		}

#if NET20
        public new MaxDBTransaction BeginTransaction(IsolationLevel isolationLevel)
#else
        public MaxDBTransaction BeginTransaction(IsolationLevel isolationLevel)
#endif // NET20
        {
            SetIsolationLevel(isolationLevel);
            return new MaxDBTransaction(this);
        }

#if !NET20
		IDbTransaction IDbConnection.BeginTransaction()
		{
			return BeginTransaction();
		}
#endif

#if NET20
        public new MaxDBTransaction BeginTransaction()
#else
        public MaxDBTransaction BeginTransaction()
#endif // NET20
        {
            return new MaxDBTransaction(this);
        }

#if NET20
        public override void ChangeDatabase(string databaseName)
#else
        public void ChangeDatabase(string databaseName)
#endif // NET20
        {
            // Change the database setting on the back-end. Note that it is a method
            // and not a property because the operation requires an expensive round trip.
            mConnArgs.dbname = databaseName;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
#if NET20
        public override void Close()
#else
        public void Close()
#endif // NET20
        {
            /*
             * Close the database connection and set the ConnectionState
             * property. If the underlying connection to the server is
             * being pooled, Close() will release it back to the pool.
             */
            if (State == ConnectionState.Open)
            {
                //>>> SQL TRACE
                if (mLogger.TraceSQL)
                    mLogger.SqlTrace(DateTime.Now, "::CLOSE CONNECTION");
                //<<< SQL TRACE

                mLogger.Flush();
#if SAFE
                iSessionID = -1;
                if (mComm != null)
                {
                    try
                    {
                        if (mGarbageParseids != null)
                            mGarbageParseids.EmptyCan();
                        ExecSQLString("ROLLBACK WORK RELEASE", GCMode.GC_NONE);
                    }
                    catch (MaxDBException)
                    {
                    }
                    finally
                    {
                        mComm.Close();
                        mComm = null;
                    }
                }
#else
				UnsafeNativeMethods.SQLDBC_ConnectProperties_delete_SQLDBC_ConnectProperties(mConnectionPropertiesHandler);
				mConnectionPropertiesHandler = IntPtr.Zero;

				UnsafeNativeMethods.SQLDBC_Connection_close(mConnectionHandler);
#endif // SAFE
            }
        }

#if NET20
        public override string ConnectionString
#else
        public string ConnectionString
#endif // NET20
        {
            get
            {
                // Always return exactly what the user set. Security-sensitive information may be removed.
                return strConnection;
            }
            set
            {
                strConnection = value;
                mConnStrBuilder = new MaxDBConnectionStringBuilder(value);
                SetConnectionParameters();
            }
        }

#if NET20
        public override int ConnectionTimeout
#else
        public int ConnectionTimeout
#endif // NET20
        {
            get
            {
                // Returns the connection time-out value set in the connection
                // string. Zero indicates an indefinite time-out period.
                return iTimeout;
            }
        }

#if NET20
        protected override DbCommand CreateDbCommand()
        {
            return CreateCommand();
        }
#else
		IDbCommand IDbConnection.CreateCommand()
		{
			return CreateCommand();
		}
#endif // NET20

#if NET20
        public static new MaxDBCommand CreateCommand()
#else
        public static MaxDBCommand CreateCommand()
#endif // NET20
        {
            // Return a new instance of a command object.
            return new MaxDBCommand();
        }

#if NET20
        public override string Database
#else
        public string Database
#endif // NET20
        {
            get
            {
                 return mConnArgs.dbname;
            }
        }

#if NET20
        public override string DataSource
#else
        public string DataSource
#endif // NET20
        {
            get
            {
                return mConnArgs.host;
            }
        }

#if NET20
        public override void Open()
#else
        public void Open()
#endif // NET20
        {
#if SAFE
#if NET20 || MONO
            if (bEncrypt)
            {
                if (mConnArgs.port == 0) mConnArgs.port = Ports.DefaultSecure;
                mComm = new MaxDBComm(new SslSocketClass(mConnArgs.host, mConnArgs.port, iTimeout, true,
                    strSslCertificateName != null ? strSslCertificateName : mConnArgs.host));
            }
            else
            {
                if (mConnArgs.port == 0) mConnArgs.port = Ports.Default;
                mComm = new MaxDBComm(new SocketClass(mConnArgs.host, mConnArgs.port, iTimeout, true));
            }
#else
            if (mConnArgs.port == 0) mConnArgs.port = Ports.Default;
			mComm = new MaxDBComm(new SocketClass(mConnArgs.host, mConnArgs.port, iTimeout, true));
#endif // NET20 || MONO

            mLogger = new MaxDBLogger();
            DoConnect();
#else
            OpenConnection();
			mEncoding = UnsafeNativeMethods.SQLDBC_Connection_isUnicodeDatabase(mConnectionHandler) == 1 ? Encoding.Unicode : Encoding.ASCII;
			iKernelVer = UnsafeNativeMethods.SQLDBC_Connection_getKernelVersion(mConnectionHandler);
#endif // SAFE
        }

#if NET20
        public override ConnectionState State
#else
        public ConnectionState State
#endif // NET20
        {
            get
            {
#if SAFE
                return iSessionID >= 0 ? ConnectionState.Open : ConnectionState.Closed;
#else
				if (mConnectionHandler != IntPtr.Zero && UnsafeNativeMethods.SQLDBC_Connection_isConnected(mConnectionHandler) == SQLDBC_BOOL.SQLDBC_TRUE)
					return ConnectionState.Open;
				else
					return ConnectionState.Closed;
#endif // SAFE
            }
        }

        #endregion

        #region IDisposable Members

#if !NET20
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
		}
#endif // NET20

#if NET20
        protected override void Dispose(bool disposing)
#else
        private void Dispose(bool disposing)
#endif // NET20
        {
#if NET20
            base.Dispose(disposing);
#endif // NET20
            if (disposing)
            {
                if (State == ConnectionState.Open)
                    Close();
                
                if (mLogger != null)
                    ((IDisposable)mLogger).Dispose();

#if !SAFE
                UnsafeNativeMethods.SQLDBC_Environment_releaseConnection(mEnviromentHandler, mConnectionHandler);
                mConnectionHandler = IntPtr.Zero;
                UnsafeNativeMethods.SQLDBC_Environment_delete_SQLDBC_Environment(mEnviromentHandler);
                mEnviromentHandler = IntPtr.Zero;
#endif // !SAFE
            }
        }

        #endregion
    }
}

