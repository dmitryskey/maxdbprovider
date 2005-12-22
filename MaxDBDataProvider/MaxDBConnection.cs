using System;
using System.Data;
using System.Text;
using System.Collections;
using System.Runtime.CompilerServices;
using MaxDBDataProvider.MaxDBProtocol;

namespace MaxDBDataProvider
{
	struct ConnectArgs 
	{
		public string username;
		public string password;
		public string dbname;
		public string host;
		public int port;
	};

	public class MaxDBConnection : IDbConnection
	{
		private string m_sConnString;

		private ConnectArgs m_ConnArgs;

		#region "SQLDBC Wrapper parameters"
		
#if !NATIVE
		private IntPtr m_runtimeHandler;
		private IntPtr m_envHandler;
		private IntPtr m_connPropHandler;
#endif

		internal IntPtr m_connHandler = IntPtr.Zero;

		#endregion

		#region "Native implementation parameters"
		
#if NATIVE

		private MaxDBComm m_comm = null;
		private Stack m_packetPool = new Stack();
		private bool m_autocommit = false;
		private GarbageParseid m_garbageParseids = null;
		private GarbageCursor  m_garbageCursors  = null;
		private object m_execObj;
		private static string syncObj = string.Empty;
		private int m_nonRecyclingExecutions = 0;
		private bool m_inTransaction = false;
		private bool m_inReconnect = false;
		private string m_cache;
		private int m_timeout, m_cacheLimit, m_cacheSize;
		private ParseInfoCache m_parseCache = null;
		private bool m_auth = false;
		private bool m_spaceoption = false;
		int m_sessionID = -1;
		private static byte[] defaultFeatureSet = {1, 0, 2, 0, 3, 0, 4, 0, 5, 0};
		private byte[] kernelFeatures = new byte[defaultFeatureSet.Length];

#endif

		#endregion
		
		private Encoding m_enc = Encoding.ASCII;
		private int m_mode = SqlMode.Internal;
		private int m_level = -1;
		private int m_kernelVersion; // Version without patch level, e.g. 70402 or 70600.

		// Always have a default constructor.
		public MaxDBConnection()
		{
		}
    
		// Have a constructor that takes a connection string.
		public MaxDBConnection(string sConnString) 
		{
			ParseConnectionString(sConnString);
		}

		private void ParseConnectionString(string sConnString)
		{
			string[] paramArr = sConnString.Split(';');
			foreach (string param in paramArr)
			{
				if (param.Split('=').Length > 1)
					switch (param.Split('=')[0].Trim().ToUpper())
					{
						case "DATA SOURCE":
							string[] hostPort = param.Split('=')[1].Trim().Split(':');
							m_ConnArgs.host = hostPort[0];
							try
							{
								m_ConnArgs.port = int.Parse(hostPort[1]);
							}
							catch
							{
								m_ConnArgs.port = Ports.Default;
							}
							break;
						case "INITIAL CATALOG":
							m_ConnArgs.dbname = param.Split('=')[1].Trim();
							break;
						case "USER ID":
							m_ConnArgs.username = param.Split('=')[1].Trim();
							break;
						case "PASSWORD":
							m_ConnArgs.password = param.Split('=')[1].Trim();
							break;
#if NATIVE
						case "TIMEOUT":
							try
							{
								m_timeout = int.Parse(param.Split('=')[1]);
							}
							catch
							{
								m_timeout = 0;
							}
							break;
						case "CACHE":
							m_cache = param.Split('=')[1].Trim();
							break;
						case "CACHELIMIT":
							try
							{
								m_cacheLimit = int.Parse(param.Split('=')[1]);
							}
							catch
							{
								m_cacheLimit = 0;
							}
							break;
						case "CACHESIZE":
							try
							{
								m_cacheSize = int.Parse(param.Split('=')[1]);
							}
							catch
							{
								m_cacheSize = 0;
							}
							break;
						case "AUTH":
							if (param.Split('=')[1].Trim().ToUpper() == "TRUE")
								m_auth = true;
							break;
						case "SPACE":
							if (param.Split('=')[1].Trim().ToUpper() == "TRUE")
								m_spaceoption = true;
							break;
#endif
						case "MODE":
							string mode = param.Split('=')[1].Trim().ToUpper();
							if(mode == SqlModeName.Value[SqlMode.Ansi])
							{
								m_mode = SqlMode.Ansi;
								break;
							}
							if(mode == SqlModeName.Value[SqlMode.Db2])
							{
								m_mode = SqlMode.Db2;
								break;
							}
							if(mode == SqlModeName.Value[SqlMode.Oracle])
							{
								m_mode = SqlMode.Oracle;
								break;
							}
							if(mode == SqlModeName.Value[SqlMode.SAPR3])
							{
								m_mode = SqlMode.SAPR3;
								break;
							}
							m_mode = SqlMode.Internal;
							break;
					}
			}
		}

		/****
		 * IMPLEMENT THE REQUIRED PROPERTIES.
		 ****/
		public string ConnectionString
		{
			get
			{
				// Always return exactly what the user set.
				// Security-sensitive information may be removed.
				return m_sConnString;
			}
			set
			{
				m_sConnString = value;
				ParseConnectionString(value);
			}
		}

		public int ConnectionTimeout
		{
			get
			{
				// Returns the connection time-out value set in the connection
				// string. Zero indicates an indefinite time-out period.
				// execute query 'SELECT timeout FROM DOMAIN.CONNECTPARAMETERS'
				try
				{
					if (State != ConnectionState.Closed)
						Open();

					IntPtr stmt = SQLDBC.SQLDBC_Connection_createStatement(m_connHandler);

					if(SQLDBC.SQLDBC_Statement_executeASCII(stmt, "SELECT timeout FROM DOMAIN.CONNECTPARAMETERS") != 0) 
						return 0;

					/*
					* Check if the SQL command return a resultset and get a result set object.
					*/
  
					IntPtr result = SQLDBC.SQLDBC_Statement_getResultSet(stmt);
					if (result == IntPtr.Zero) 
						return 0;

					/*
					 * Position the cursor within the resultset by doing a fetch next call.
					 */

					if(SQLDBC.SQLDBC_ResultSet_next(result) != 0) 
						return 0;

					int timeout = getTimeout(result);

					SQLDBC.SQLDBC_ResultSet_close(result);

					SQLDBC.SQLDBC_Connection_releaseStatement(m_connHandler, stmt);

					if (State == ConnectionState.Closed)
						Close();

					return timeout;
				}
				catch(Exception)
				{
					return 0;
				}
			}
		}

		private unsafe int getTimeout(IntPtr result)
		{
			/*
			 * Get an integer value from the column.
			*/
			byte[] timeout = new byte[sizeof(Int32)];
			Int32 ind = 0;

			fixed(byte *buffer = timeout)
			{
				if(SQLDBC.SQLDBC_ResultSet_getObject(result, 1, SQLDBC_HostType.SQLDBC_HOSTTYPE_INT4, 
					new IntPtr(buffer), ref ind, sizeof(Int32), 0) != 0) 
					return 0;
			}

			return BitConverter.ToInt32(timeout, 0);
		}

		public string Database
		{
			get
			{
				// Returns an initial database as set in the connection string.
				// An empty string indicates not set - do not return a null reference.
				return m_ConnArgs.dbname;
			}
		}

		public Encoding DatabaseEncoding
		{
			get{return m_enc;}
		}

		public ConnectionState State
		{
			get 
			{
#if NATIVE
				return m_sessionID >= 0 ? ConnectionState.Open : ConnectionState.Closed;
#else
				if (m_connHandler != IntPtr.Zero && SQLDBC.SQLDBC_Connection_isConnected(m_connHandler) == SQLDBC_BOOL.SQLDBC_TRUE)
					return ConnectionState.Open;
				else
					return ConnectionState.Closed;
#endif
			}
		}

		public int SQLMode
		{
			get{return m_mode;}
			set{m_mode = value;}
		}

		public bool AutoCommit
		{
			get
			{
				if (State != ConnectionState.Open)
					throw new MaxDBException(MessageTranslator.Translate(MessageKey.ERROR_CONNECTIONNOTOPENED));

#if NATIVE
				return m_autocommit;
#else
				return SQLDBC.SQLDBC_Connection_getAutoCommit(m_connHandler) == SQLDBC_BOOL.SQLDBC_TRUE;
#endif
			}
			set
			{
				if (State != ConnectionState.Open)
					throw new MaxDBException(MessageTranslator.Translate(MessageKey.ERROR_CONNECTIONNOTOPENED));

#if NATIVE
				m_autocommit = value;
#else
				SQLDBC.SQLDBC_Connection_setAutoCommit(m_connHandler, value ? SQLDBC_BOOL.SQLDBC_TRUE : SQLDBC_BOOL.SQLDBC_FALSE);
#endif
			}
		}

		public string ServerVersion 
		{
			get
			{
				int correction_level = m_kernelVersion % 100; 
				int minor_release  = ((m_kernelVersion - correction_level) % 10000)/ 100;
				int mayor_release = (m_kernelVersion - minor_release * 100 - correction_level) / 10000;
				return mayor_release.ToString() + "." + minor_release.ToString() + "." + correction_level.ToString();
			}
		}

		/****
		 * IMPLEMENT THE REQUIRED METHODS.
		 ****/

		IDbTransaction IDbConnection.BeginTransaction()
		{
			return new MaxDBTransaction(this);
		}

		public MaxDBTransaction BeginTransaction()
		{
			return new MaxDBTransaction(this);
		}

		public MaxDBTransaction BeginTransaction(IsolationLevel level)
		{
			SetIsolationLevel(level);
			return new MaxDBTransaction(this);
		}

		IDbTransaction IDbConnection.BeginTransaction(IsolationLevel level)
		{
			SetIsolationLevel(level);
			return new MaxDBTransaction(this);
		}

		private int MapIsolationLevel(IsolationLevel level)
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
					return 0;
			}
		}

		private void SetIsolationLevel(IsolationLevel level)
		{
			m_level = MapIsolationLevel(level);

			if(SQLDBC.SQLDBC_Connection_setTransactionIsolation(m_connHandler, m_level) != SQLDBC_Retcode.SQLDBC_OK) 
				throw new MaxDBException("Can't set isolation level: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
					SQLDBC.SQLDBC_Connection_getError(m_connHandler)));

			// "SET ISOLATION LEVEL " + m_level.ToString()
		}

		public void ChangeDatabase(string dbName)
		{
			/*
			 * Change the database setting on the back-end. Note that it is a method
			 * and not a property because the operation requires an expensive
			 * round trip.
			 */
			m_ConnArgs.dbname = dbName;
		}

		public void Open()
		{
			/*
			 * Open the database connection and set the ConnectionState
			 * property. If the underlying connection to the server is 
			 * expensive to obtain, the implementation should provide
			 * implicit pooling of that connection.
			 * 
			 * If the provider also supports automatic enlistment in 
			 * distributed transactions, it should enlist during Open().
			 */
#if NATIVE
			m_comm = new MaxDBComm(new SocketClass(m_ConnArgs.host, m_ConnArgs.port, m_timeout));
			DoConnect();
#else
			OpenConnection();
			m_enc = SQLDBC.SQLDBC_Connection_isUnicodeDatabase(m_connHandler) == 1?Encoding.Unicode:Encoding.ASCII;
			m_kernelVersion = SQLDBC.SQLDBC_Connection_getKernelVersion(m_connHandler);
#endif
		}

#if !NATIVE
		private unsafe void OpenConnection()
		{
			byte[] errorText = new byte[256];
			
			fixed(byte* errorPtr = errorText)
			{
				m_runtimeHandler = SQLDBC.ClientRuntime_GetClientRuntime(new IntPtr(errorPtr), errorText.Length);
			}
			if (m_runtimeHandler == IntPtr.Zero)
				throw new MaxDBException(Encoding.ASCII.GetString(errorText));

			m_envHandler = SQLDBC.SQLDBC_Environment_new_SQLDBC_Environment(m_runtimeHandler);

			m_connHandler = SQLDBC.SQLDBC_Environment_createConnection(m_envHandler);

			m_connPropHandler = SQLDBC.SQLDBC_ConnectProperties_new_SQLDBC_ConnectProperties();
			SQLDBC.SQLDBC_Connection_setSQLMode(m_connHandler, m_mode);

			if (SQLDBC.SQLDBC_Connection_connectASCII(m_connHandler, m_ConnArgs.host, m_ConnArgs.dbname, m_ConnArgs.username, 
				m_ConnArgs.password, m_connPropHandler) != SQLDBC_Retcode.SQLDBC_OK) 
				throw new MaxDBException("Connecting to the database failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
					SQLDBC.SQLDBC_Connection_getError(m_connHandler)));
		}
#endif

		public void Close()
		{
			/*
			 * Close the database connection and set the ConnectionState
			 * property. If the underlying connection to the server is
			 * being pooled, Close() will release it back to the pool.
			 */
#if NATIVE
			m_sessionID = -1;
			m_comm.Close();
			m_comm = null;
#else
			SQLDBC.SQLDBC_ConnectProperties_delete_SQLDBC_ConnectProperties(m_connPropHandler);
			m_connPropHandler = IntPtr.Zero;

			SQLDBC.SQLDBC_Connection_close(m_connHandler);

			SQLDBC.SQLDBC_Environment_releaseConnection(m_envHandler, m_connHandler);
			m_connHandler = IntPtr.Zero;
			SQLDBC.SQLDBC_Environment_delete_SQLDBC_Environment(m_envHandler);
			m_envHandler = IntPtr.Zero;
#endif
		}

		public MaxDBCommand CreateCommand()
		{
			// Return a new instance of a command object.
			return new MaxDBCommand();
		}

		IDbCommand IDbConnection.CreateCommand()
		{
			// Return a new instance of a command object.
			return new MaxDBCommand();
		}

		public void Dispose() 
		{
			if (State == ConnectionState.Open)
				Close();

			System.GC.SuppressFinalize(this);
		}

		#region "Methods to support native protocol"

#if NATIVE
		private string TermID
		{
			get
			{
				return ("ado.net@" + this.GetHashCode().ToString("x")).PadRight(18);
			}
		}

		private bool initiateChallengeResponse(MaxDBRequestPacket requestPacket, string user, Auth auth)
		{
			if (requestPacket.initChallengeResponse(user, auth.ClientChallenge))
			{
				MaxDBReplyPacket replyPacket = Exec(requestPacket, this, GCMode.GC_DELAYED); 
				auth.parseServerChallenge(replyPacket.VarDataPart);
				return true;
			}  
			else 
				return false;
		}

		public MaxDBRequestPacket CreateRequestPacket()
		{
			MaxDBRequestPacket packet;
			
			if (m_packetPool.Count == 0)
				packet = new MaxDBRequestPacket(new byte[HeaderOffset.END + m_comm.MaxCmdSize], Consts.AppID, Consts.ApplVers);
			else
				packet = (MaxDBRequestPacket)m_packetPool.Pop();

			packet.IsAvailable = true;
			return packet;
		}

		private void FreeRequestPacket(MaxDBRequestPacket requestPacket) 
		{
			requestPacket.IsAvailable = false;
			m_packetPool.Push(requestPacket);
		}

		public MaxDBReplyPacket	Exec(MaxDBRequestPacket requestPacket, object execObj, int gcFlags)
		{
			return Exec(requestPacket, false, false, execObj, gcFlags);
		}

		[MethodImpl(MethodImplOptions.Synchronized)]
		public MaxDBReplyPacket Exec(MaxDBRequestPacket requestPacket, bool ignoreErrors, bool isParse, object execObj, int gcFlags)
		{
			int requestLen;
			MaxDBReplyPacket replyPacket = null;
			int localWeakReturnCode = 0;

			if (State != ConnectionState.Open)	
				if (gcFlags == GCMode.GC_ALLOWED) 
				{
					bool spaceleft = true;
					if (m_garbageCursors != null && m_garbageCursors.isPending) 
						spaceleft = m_garbageCursors.emptyCan(requestPacket);

					if (spaceleft && m_garbageParseids != null && m_garbageParseids.isPending) 
						m_garbageParseids.emptyCan(requestPacket);
				} 
				else 
				{
					if(m_garbageParseids != null && m_garbageParseids.isPending) 
						m_nonRecyclingExecutions++;
				}

			requestPacket.Close();

			requestLen = requestPacket.PacketLength;

			try 
			{
				m_execObj = execObj;
				replyPacket = m_comm.Exec(requestPacket, requestLen);

				/*get Returncode*/
				int firstSegm = replyPacket.firstSegment;
				localWeakReturnCode = replyPacket.weakReturnCode;

				if(localWeakReturnCode != -8) 
					FreeRequestPacket(requestPacket);

				if (!m_autocommit && ! isParse) 
					m_inTransaction = true;

				// if it is not completely forbidden, we will send the drop
				if(gcFlags != GCMode.GC_NONE) 
				{
					if (m_garbageCursors != null && m_garbageCursors.isPending && localWeakReturnCode == 0) 
						m_garbageCursors.emptyCan(this);

					if(m_nonRecyclingExecutions > 20 && localWeakReturnCode == 0) 
					{
						m_nonRecyclingExecutions = 0;
						if (m_garbageParseids != null && m_garbageParseids.isPending) 
							m_garbageParseids.emptyCan(this);
						m_nonRecyclingExecutions = 0;
					}
				}

			}
			catch (MaxDBException ex) 
			{
				// if a reconnect is forbidden or we are in the process of a
				// reconnect or we are in a (now rolled back) transaction
				if (m_inReconnect || m_inTransaction) 
					throw;
				else 
				{
					TryReconnect(ex);
					m_inTransaction = false;
				}
			}
			finally 
			{
				m_execObj = null;
			}
			if (!ignoreErrors && localWeakReturnCode != 0) 
				throw replyPacket.createException();
			return replyPacket;
		}

		private string stripString(string str)
		{
			if (!(str.StartsWith("\"") && str.EndsWith("\"")))
				return str.ToUpper();
			else
				return str.Substring(1, str.Length - 2);
		}

		private void DoConnect()
		{
			string username = m_ConnArgs.username;
			if (username == null)
				throw new MaxDBException(MessageTranslator.Translate(MessageKey.ERROR_NOUSER));

			username = stripString(username);

			string password = m_ConnArgs.password;
			if (password == null)
				throw new MaxDBException(MessageTranslator.Translate(MessageKey.ERROR_NOPASSWORD));

			password = stripString(password);

			byte[] passwordBytes = m_enc.GetBytes(password);
 
			m_comm.Connect(m_ConnArgs.dbname, m_ConnArgs.port);

			string connectCmd;
			byte [] crypted;
			MaxDBRequestPacket requestPacket = CreateRequestPacket();
			Auth auth = null;
			bool isChallengeResponseSupported = false;
			if (m_comm.IsAuthAllowed)
			{
				try
				{
					auth = new Auth();
					isChallengeResponseSupported = initiateChallengeResponse(requestPacket, username, auth);
					if (password.Length > auth.MaxPasswordLength && auth.MaxPasswordLength > 0)
						password = password.Substring(0, auth.MaxPasswordLength);
				}
				catch(MaxDBSQLException ex)
				{
					isChallengeResponseSupported = false;
					if (ex.VendorCode == -5015)
						try
						{
							m_comm.Reconnect();
						}
						catch(MaxDBException exc)
						{
							throw new ConnectionException(exc);
						}
					else
						throw;
				}
				catch
				{
					isChallengeResponseSupported = false;
				}
			}
			if (m_auth && !isChallengeResponseSupported)
				throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_CONNECTION_CHALLENGERESPONSENOTSUPPORTED));
		
			/*
			* build connect statement
			*/
			connectCmd = "CONNECT " + m_ConnArgs.username + " IDENTIFIED BY :PW SQLMODE " + SqlModeName.Value[m_mode];
			if (m_timeout > 0) 
				connectCmd += " TIMEOUT " + m_timeout;
			if (m_level >= 0)
				connectCmd += " ISOLATION LEVEL " + m_level;
			if (m_cacheLimit > 0)
				connectCmd += " CACHELIMIT " + m_cacheLimit;
			if (m_spaceoption) 
			{
				connectCmd += " SPACE OPTION ";
				setKernelFeatureRequest(Feature.SpaceOption);
			}

			requestPacket.initDbsCommand (false, connectCmd);

			if (!isChallengeResponseSupported)
			{
				try 
				{
					crypted = Crypt.Mangle(password, false);
				}
				catch 
				{
					throw new MaxDBSQLException(MessageTranslator.Translate(MessageKey.ERROR_INVALIDPASSWORD));
				}
				requestPacket.newPart(PartKind.Data);
				requestPacket.addData(crypted);
				requestPacket.addASCII(TermID);
				requestPacket.incrPartArguments();
			} 
			else 
			{
				requestPacket.addClientProofPart(auth.getClientProof(passwordBytes)); 
				requestPacket.addClientIDPart(TermID);
			}

			defaultFeatureSet.CopyTo(kernelFeatures, 0);

			setKernelFeatureRequest(Feature.MultipleDropParseid);
			setKernelFeatureRequest(Feature.CheckScrollableOption);
			requestPacket.addFeatureRequestPart(kernelFeatures);

			// execute
			MaxDBReplyPacket replyPacket = Exec(requestPacket, this, GCMode.GC_DELAYED);
			m_sessionID = replyPacket.SessionID;
			m_enc = replyPacket.isUnicode?Encoding.Unicode:Encoding.ASCII;
			
			m_kernelVersion = 10000 * replyPacket.KernelMajorVersion + 100 * replyPacket.KernelMinorVersion + replyPacket.KernelCorrectionLevel;
			byte[] featureReturn = replyPacket.Features;

			if (featureReturn != null)
				kernelFeatures = featureReturn;
			else 
				defaultFeatureSet.CopyTo(kernelFeatures, 0);

			if (m_cache != null && m_cache.Length > 0 && m_cacheSize > 0)
				m_parseCache = new ParseInfoCache(m_cache, m_cacheSize);
		}

		private void setKernelFeatureRequest(int feature)
		{
			kernelFeatures[2 * (feature - 1) + 1] = 1;
		}

		private void TryReconnect(MaxDBException ex)
		{
			lock (syncObj)
			{
				if (m_parseCache != null) 
					m_parseCache.Clear();
				m_packetPool.Clear();
				m_inReconnect = true;
				try 
				{
					m_comm.Reconnect();
					DoConnect();
				}
				catch (MaxDBException conn_ex) 
				{
					throw new ConnectionException(conn_ex);
				}
				finally 
				{
					m_inReconnect = false;
				}
				throw new TimeoutException();
			}
		}

#endif

		#endregion
	}
}

