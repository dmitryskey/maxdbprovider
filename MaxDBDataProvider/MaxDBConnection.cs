using System;
using System.Data;
using System.Text;
using MaxDBDataProvider.MaxDBProtocol;

namespace MaxDBDataProvider
{
	struct ConnectArgs 
	{
		public string username;
		public string password;
		public string dbname;
		public string host;
	};

	public class MaxDBConnection : IDbConnection
	{
		private string      m_sConnString;

		private ConnectArgs m_ConnArgs;

		private IntPtr runtimeHandler;
		private IntPtr envHandler;
		internal IntPtr connHandler = IntPtr.Zero;
		private IntPtr connPropHandler;

		private MaxDBComm comm;

		private Encoding enc = Encoding.ASCII;
		private SQLDBC_SQLMode mode = SQLDBC_SQLMode.SQLDBC_INTERNAL;

		// Always have a default constructor.
		public MaxDBConnection()
		{
		}
    
		// Have a constructor that takes a connection string.
		public MaxDBConnection(string sConnString) 
		{
			ParseConnectionString(sConnString);
			comm = new MaxDBComm(new SocketClass(m_ConnArgs.host, Ports.Default));
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
							m_ConnArgs.host = param.Split('=')[1].Trim();
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
						case "MODE":
							switch(param.Split('=')[1].Trim().ToUpper())
							{
								case "ANSI":
									mode = SQLDBC_SQLMode.SQLDBC_ANSI;
									break;
								case "DB2":
									mode = SQLDBC_SQLMode.SQLDBC_DB2;
									break;
								case "ORACLE":
									mode = SQLDBC_SQLMode.SQLDBC_ORACLE;
									break;
								case "SAPR3":
									mode = SQLDBC_SQLMode.SQLDBC_SAPR3;
									break;
								default:
									mode = SQLDBC_SQLMode.SQLDBC_INTERNAL;
									break;
							}
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

					IntPtr stmt = SQLDBC.SQLDBC_Connection_createStatement(connHandler);

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

					SQLDBC.SQLDBC_Connection_releaseStatement(connHandler, stmt);

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
			get{return enc;}
		}

		public ConnectionState State
		{
			get 
			{ 
				if (connHandler != IntPtr.Zero && SQLDBC.SQLDBC_Connection_isConnected(connHandler) == SQLDBC_BOOL.SQLDBC_TRUE)
					return ConnectionState.Open;
				else
					return ConnectionState.Closed;
			}
		}

		public SQLDBC_SQLMode SQLMode
		{
			get{return mode;}
			set{mode = value;}
		}

		public bool AutoCommit
		{
			get
			{
				if (State != ConnectionState.Open)
					throw new MaxDBException("Connection is not opened.");

				return SQLDBC.SQLDBC_Connection_getAutoCommit(connHandler) == SQLDBC_BOOL.SQLDBC_TRUE;
			}
			set
			{
				if (State != ConnectionState.Open)
					throw new MaxDBException("Connection is not opened.");

				SQLDBC.SQLDBC_Connection_setAutoCommit(connHandler, value ? SQLDBC_BOOL.SQLDBC_TRUE : SQLDBC_BOOL.SQLDBC_FALSE);
			}
		}

		public string ServerVersion 
		{
			get
			{
				if (State != ConnectionState.Open)
					throw new MaxDBException("Connection is not opened.");

				int version = SQLDBC.SQLDBC_Connection_getKernelVersion(connHandler);
				int correction_level = version % 100; 
				int minor_release  = ((version - correction_level) % 10000)/ 100;
				int mayor_release = (version - minor_release * 100 - correction_level) / 10000;
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

		private void SetIsolationLevel(IsolationLevel level)
		{
			int MaxDBLevel;

			switch (level)
			{
				case IsolationLevel.ReadUncommitted:
					MaxDBLevel = 0;
					break;
				case IsolationLevel.ReadCommitted:
					MaxDBLevel = 1;
					break;
				case IsolationLevel.RepeatableRead:
					MaxDBLevel = 2;
					break;
				case IsolationLevel.Serializable:
					MaxDBLevel = 3;
					break;
				default:
					MaxDBLevel = 0;
					break;
			}

			if(SQLDBC.SQLDBC_Connection_setTransactionIsolation(connHandler, MaxDBLevel) != SQLDBC_Retcode.SQLDBC_OK) 
				throw new MaxDBException("Can't set isolation level: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
					SQLDBC.SQLDBC_Connection_getError(connHandler)));

			// "SET ISOLATION LEVEL " + MaxDBLevel.ToString()
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
			OpenConnection();

			if (SQLDBC.SQLDBC_Connection_isUnicodeDatabase(connHandler) == 1)
				enc = Encoding.Unicode;//little-endian unicode
			else
				enc = Encoding.ASCII;
		}

		private string TermID
		{
			get
			{
				return ("ado.net@" + this.GetHashCode().ToString("x")).PadRight(18);
			}
		}

		private unsafe void OpenConnection()
		{
			comm.Connect(m_ConnArgs.dbname, Ports.Default);
			byte[] requestBuf = new byte[HeaderOffset.END + comm.MaxCmdSize];


			byte[] errorText = new byte[256];
			
			fixed(byte* errorPtr = errorText)
			{
				runtimeHandler = SQLDBC.ClientRuntime_GetClientRuntime(new IntPtr(errorPtr), errorText.Length);
			}
			if (runtimeHandler == IntPtr.Zero)
				throw new MaxDBException(Encoding.ASCII.GetString(errorText));

			envHandler = SQLDBC.SQLDBC_Environment_new_SQLDBC_Environment(runtimeHandler);

			connHandler = SQLDBC.SQLDBC_Environment_createConnection(envHandler);

			connPropHandler = SQLDBC.SQLDBC_ConnectProperties_new_SQLDBC_ConnectProperties();
			SQLDBC.SQLDBC_Connection_setSQLMode(connHandler, mode);

			if (SQLDBC.SQLDBC_Connection_connectASCII(connHandler, m_ConnArgs.host, m_ConnArgs.dbname, m_ConnArgs.username, 
				m_ConnArgs.password, connPropHandler) != SQLDBC_Retcode.SQLDBC_OK) 
				throw new MaxDBException("Connecting to the database failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
					SQLDBC.SQLDBC_Connection_getError(connHandler)));
		}

		public void Close()
		{
			/*
			 * Close the database connection and set the ConnectionState
			 * property. If the underlying connection to the server is
			 * being pooled, Close() will release it back to the pool.
			 */
			comm.Close();

			SQLDBC.SQLDBC_ConnectProperties_delete_SQLDBC_ConnectProperties(connPropHandler);
			connPropHandler = IntPtr.Zero;

			SQLDBC.SQLDBC_Connection_close(connHandler);

			SQLDBC.SQLDBC_Environment_releaseConnection(envHandler, connHandler);
			connHandler = IntPtr.Zero;
			SQLDBC.SQLDBC_Environment_delete_SQLDBC_Environment(envHandler);
			envHandler = IntPtr.Zero;
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
	}
}

