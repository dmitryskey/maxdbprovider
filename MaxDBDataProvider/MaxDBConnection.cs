using System;
using System.Data;
using System.Text;

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
		private ConnectionState m_state;
		private string      m_sConnString;

		private ConnectArgs m_ConnArgs;

		private IntPtr runtimeHandler;
		private IntPtr envHandler;
		internal IntPtr connHandler;
		private IntPtr connPropHandler;
		private Encoding enc;

		// Always have a default constructor.
		public MaxDBConnection()
		{
			// Initialize the connection object into the closed state.
			m_state = ConnectionState.Closed;
		}
    
		// Have a constructor that takes a connection string.
		public MaxDBConnection(string sConnString)
		{
			// Initialize the connection object into a closed state.
			m_state = ConnectionState.Closed;
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
					ConnectionState status = m_state;

					if (status != ConnectionState.Closed)
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

					if (status == ConnectionState.Closed)
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
			get
			{
				return enc;
			}
		}

		public ConnectionState State
		{
			get { return m_state; }
		}

		/****
		 * IMPLEMENT THE REQUIRED METHODS.
		 ****/

		public IDbTransaction BeginTransaction()
		{
			return new MaxDBTransaction(this);
		}

		public IDbTransaction BeginTransaction(IsolationLevel level)
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

			return new MaxDBTransaction(this);
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
			byte[] errorText = new byte[256];

			runtimeHandler = SQLDBC.ClientRuntime_GetClientRuntime(errorText, 256);
			if (runtimeHandler != IntPtr.Zero)
				throw new MaxDBException(Encoding.ASCII.GetString(errorText));

			envHandler = SQLDBC.SQLDBC_Environment_new_SQLDBC_Environment(runtimeHandler);

			connHandler = SQLDBC.SQLDBC_Environment_createConnection(envHandler);
			connPropHandler = SQLDBC.SQLDBC_ConnectProperties_new_SQLDBC_ConnectProperties();

			if(SQLDBC.SQLDBC_Connection_connectASCII(connHandler, m_ConnArgs.host, m_ConnArgs.dbname, m_ConnArgs.username, 
				m_ConnArgs.password, connPropHandler) != SQLDBC_Retcode.SQLDBC_OK) 
				throw new MaxDBException("Connecting to the database failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
					SQLDBC.SQLDBC_Connection_getError(connHandler)));

			if (SQLDBC.SQLDBC_Connection_isUnicodeDatabase(connHandler) == 1)
				enc = Encoding.Unicode;//little-endian unicode
			else
				enc = Encoding.ASCII;

			m_state = ConnectionState.Open;
		}

		public void Close()
		{
			/*
			 * Close the database connection and set the ConnectionState
			 * property. If the underlying connection to the server is
			 * being pooled, Close() will release it back to the pool.
			 */
			m_state = ConnectionState.Closed;

			SQLDBC.SQLDBC_ConnectProperties_delete_SQLDBC_ConnectProperties(connPropHandler);
			connPropHandler = IntPtr.Zero;

			SQLDBC.SQLDBC_Connection_close(connHandler);
			connHandler = IntPtr.Zero;

			SQLDBC.SQLDBC_Environment_delete_SQLDBC_Environment(envHandler);
			envHandler = IntPtr.Zero;
		}

		public IDbCommand CreateCommand()
		{
			// Return a new instance of a command object.
			return new MaxDBCommand();
		}

		void IDisposable.Dispose() 
		{
			this.Dispose(true);
			System.GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) 
		{
			/*
			 * Dispose of the object and perform any cleanup.
			 */

			if (m_state == ConnectionState.Open)
				this.Close();
		}

	}
}

