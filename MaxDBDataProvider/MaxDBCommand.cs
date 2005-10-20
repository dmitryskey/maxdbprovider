using System;
using System.Data;
using System.Text;

namespace MaxDBDataProvider
{
	public class MaxDBCommand : IDbCommand
	{
		MaxDBConnection  m_connection;
		MaxDBTransaction  m_txn;
		string      m_sCmdText;
		UpdateRowSource m_updatedRowSource = UpdateRowSource.None;
		MaxDBParameterCollection m_parameters = new MaxDBParameterCollection();
		CommandType m_sCmdType = CommandType.Text;
		IntPtr stmt = IntPtr.Zero;

		// Implement the default constructor here.
		public MaxDBCommand()
		{
			stmt = SQLDBC.SQLDBC_Connection_createPreparedStatement(m_connection.connHandler);
		}

		// Implement other constructors here.
		public MaxDBCommand(string cmdText)
		{
			m_sCmdText = cmdText;
		}

		public MaxDBCommand(string cmdText, MaxDBConnection connection)
		{
			m_sCmdText    = cmdText;
			m_connection  = connection;
			stmt = SQLDBC.SQLDBC_Connection_createPreparedStatement(m_connection.connHandler);
		}

		public MaxDBCommand(string cmdText, MaxDBConnection connection, MaxDBTransaction txn)
		{
			m_sCmdText    = cmdText;
			m_connection  = connection;
			m_txn      = txn;
			stmt = SQLDBC.SQLDBC_Connection_createPreparedStatement(m_connection.connHandler);
		}

		/****
		 * IMPLEMENT THE REQUIRED PROPERTIES.
		 ****/
		public string CommandText
		{
			get { return m_sCmdText;  }
			set  { m_sCmdText = value;  }
		}

		public int CommandTimeout
		{
			/*
			 * The sample does not support a command time-out. As a result,
			 * for the get, zero is returned because zero indicates an indefinite
			 * time-out period. For the set, throw an exception.
			 */
			get  { return 0; }
			set  { if (value != 0) throw new NotSupportedException(); }
		}

		public CommandType CommandType
		{
			/*
			 * The sample only supports CommandType.Text.
			 */
			get { return m_sCmdType; }
			set { m_sCmdType = value; }
		}

		public IDbConnection Connection
		{
			/*
			 * The user should be able to set or change the connection at 
			 * any time.
			 */
			get { return m_connection;  }
			set
			{
				/*
				 * The connection is associated with the transaction
				 * so set the transaction object to return a null reference if the connection 
				 * is reset.
				 */
				if (m_connection != value)
					this.Transaction = null;

				m_connection = (MaxDBConnection)value;
			}
		}

		public MaxDBParameterCollection Parameters
		{
			get  { return m_parameters; }
		}

		IDataParameterCollection IDbCommand.Parameters
		{
			get  { return m_parameters; }
		}

		public IDbTransaction Transaction
		{
			/*
			 * Set the transaction. Consider additional steps to ensure that the transaction
			 * is compatible with the connection, because the two are usually linked.
			 */
			get { return m_txn; }
			set { m_txn = (MaxDBTransaction)value; }
		}

		public UpdateRowSource UpdatedRowSource
		{
			get { return m_updatedRowSource;  }
			set { m_updatedRowSource = value; }
		}

		/****
		 * IMPLEMENT THE REQUIRED METHODS.
		 ****/
		public void Cancel()
		{
			SQLDBC.SQLDBC_Connection_cancel(m_connection.connHandler);			
		}

		public IDbDataParameter CreateParameter()
		{
			return (IDbDataParameter)(new MaxDBParameter());
		}

		public int ExecuteNonQuery()
		{
			/*
			 * ExecuteNonQuery is intended for commands that do
			 * not return results, instead returning only the number
			 * of records affected.
			 */
      
			// There must be a valid and open connection.
			if (m_connection == null || m_connection.State != ConnectionState.Open)
				throw new MaxDBException("Connection must valid and open.");

			// Execute the command.

			SQLDBC_Retcode rc;

			try
			{
				if (m_connection.DatabaseEncoding is UnicodeEncoding)
					rc = SQLDBC.SQLDBC_PreparedStatement_prepareNTS(stmt, m_connection.DatabaseEncoding.GetBytes(m_sCmdText), StringEncodingType.UCS2Swapped);
				else
					rc = SQLDBC.SQLDBC_PreparedStatement_prepareASCII(stmt, m_sCmdText);
			
				if(rc != SQLDBC_Retcode.SQLDBC_OK) 
					throw new MaxDBException("Execution failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
						SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));

				BindAndExecute(stmt, m_parameters);
			}
			catch
			{
				throw;
			}
			finally
			{
				SQLDBC.SQLDBC_Connection_releasePreparedStatement(m_connection.connHandler, stmt);
			}

			// use SQLDBC_PreparedStatement_getRowsAffected???
			return 0;
		}

		IDataReader IDbCommand.ExecuteReader()
		{
			return ExecuteReader(CommandBehavior.Default);
		}

		public MaxDBDataReader ExecuteReader()
		{
			return (MaxDBDataReader)ExecuteReader(CommandBehavior.Default);
		}

		public IDataReader ExecuteReader(CommandBehavior behavior)
		{
			/*
			 * ExecuteReader should retrieve results from the data source
			 * and return a DataReader that allows the user to process 
			 * the results.
			 */

			// There must be a valid and open connection.
			if (m_connection == null || m_connection.State != ConnectionState.Open)
				throw new InvalidOperationException("Connection must valid and open");

			// Execute the command.

			SQLDBC_Retcode rc;

			IntPtr stmt = SQLDBC.SQLDBC_Connection_createPreparedStatement(m_connection.connHandler);

			try
			{
				if (m_connection.DatabaseEncoding is UnicodeEncoding)
					rc = SQLDBC.SQLDBC_PreparedStatement_prepareNTS(stmt, m_connection.DatabaseEncoding.GetBytes(m_sCmdText), StringEncodingType.UCS2Swapped);
				else
					rc = SQLDBC.SQLDBC_PreparedStatement_prepareASCII(stmt, m_sCmdText);
			
				if(rc != SQLDBC_Retcode.SQLDBC_OK) 
					throw new MaxDBException("Execution failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
						SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));

				BindAndExecute(stmt, m_parameters);

				/*
				* Check if the SQL command return a resultset and get a result set object.
				*/  
				IntPtr result = SQLDBC.SQLDBC_PreparedStatement_getResultSet(stmt);
				if(result == IntPtr.Zero) 
					throw new Exception("SQL command doesn't return a result set " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
						SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));

				return new MaxDBDataReader(result);				
			}
			catch
			{
				throw;
			}
		}

		public object ExecuteScalar()
		{
			IDataReader result = ExecuteReader(CommandBehavior.SingleResult);
			if (result.FieldCount > 0 && result.Read())
				return result.GetValue(0);
			else
				return null;
		}

		private unsafe void BindAndExecute(IntPtr stmt, MaxDBParameterCollection parameters)
		{
			int buffer_length = 0;
			int buffer_offset = 0;

			for(ushort i = 0; i < parameters.Count; i++)
			{
				MaxDBParameter param = parameters[i];
				switch(param.m_dbType)
				{
					case MaxDBType.Boolean:
						buffer_length += sizeof(byte);
						break;
					case MaxDBType.CharA: case MaxDBType.StrA: case MaxDBType.VarCharA: 
						buffer_length += (((string)param.Value).Length + 1)* sizeof(byte);
						break;
					case MaxDBType.CharB: case MaxDBType.StrB: case MaxDBType.VarCharB:
						buffer_length += ((byte[])param.Value).Length * sizeof(byte);
						break;
					case MaxDBType.Date:
						buffer_length += sizeof(ODBCDATE);
						break;
					case MaxDBType.Time:
						buffer_length += sizeof(ODBCTIME);
						break;
					case MaxDBType.Fixed: case MaxDBType.Float: case MaxDBType.VFloat:
						buffer_length += sizeof(double);
						break;
					case MaxDBType.Integer:
						buffer_length += sizeof(int);
						break;
					case MaxDBType.SmallInt:
						buffer_length += sizeof(short);
						break;
					case MaxDBType.TimeStamp:
						buffer_length += sizeof(ODBCTIMESTAMP);
						break;
					case MaxDBType.Unicode: case MaxDBType.StrUni: case MaxDBType.VarCharUni:
						buffer_length += (((string)param.Value).Length + 1) * sizeof(char);
						break;
				}
			}

			byte[] param_buffer = new byte[buffer_length];

			fixed(byte *buffer_ptr = param_buffer)
			{
				int[] val_size = new int[parameters.Count];
				for(ushort i = 1; i <= parameters.Count; i++)
				{
					MaxDBParameter param = parameters[i - 1];
					int val_length;
					
					switch(param.m_dbType)
					{
						case MaxDBType.Boolean:
							param_buffer[buffer_offset] = (byte)((bool)param.Value ? 1 : 0);
							val_length = sizeof(byte);

							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_UINT1, 
								new IntPtr(buffer_ptr + buffer_offset), ref val_size[i - 1], sizeof(byte), SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException("Execution failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
									SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							buffer_offset += val_length;
							break;
						case MaxDBType.CharA: case MaxDBType.StrA: case MaxDBType.VarCharA: 
							val_length = (((string)param.Value).Length + 1)* sizeof(byte);
							Array.Copy(Encoding.ASCII.GetBytes((string)param.Value), 0 , param_buffer, buffer_offset, val_length - sizeof(byte)); 
							
							val_size[i - 1] = SQLDBC.SQLDBC_NTS;
							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_ASCII, 
								new IntPtr(buffer_ptr + buffer_offset),	ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_TRUE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException("Execution failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
									SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							buffer_offset += val_length;
							break;
						case MaxDBType.CharB: case MaxDBType.StrB: case MaxDBType.VarCharB:
							val_length = ((byte[])param.Value).Length * sizeof(byte);
							Array.Copy((byte[])param.Value, 0 , param_buffer, buffer_offset, val_length); 
							
							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_BINARY, 
								new IntPtr(buffer_ptr + buffer_offset),	ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException("Execution failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
									SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							buffer_offset += val_length;
							break;
						case MaxDBType.Date:
							DateTime dt = (DateTime)param.Value;
							//ODBC date format
							ODBCDATE dt_odbc;
							dt_odbc.year = (short)(dt.Year % 0x10000);
							dt_odbc.month = (ushort)(dt.Month % 0x10000);
							dt_odbc.day = (ushort)(dt.Day % 0x10000);

							val_length = sizeof(ODBCDATE);
							Array.Copy(ODBCConverter.GetBytes(dt_odbc), 0, param_buffer, buffer_offset, val_length);

							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_ODBCDATE, 
								new IntPtr(buffer_ptr + buffer_offset), ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException("Execution failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
									SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							
							buffer_offset += val_length;
							break;
						case MaxDBType.Fixed: case MaxDBType.Float: case MaxDBType.VFloat:
							Array.Copy(BitConverter.GetBytes((double)param.Value), 0, param_buffer, buffer_offset, sizeof(double));
							val_length = sizeof(double);

							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_DOUBLE, 
								new IntPtr(buffer_ptr + buffer_offset), ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException("Execution failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
									SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							buffer_offset += val_length;
							break;
						case MaxDBType.Integer:
							Array.Copy(BitConverter.GetBytes((int)param.Value), 0, param_buffer, buffer_offset, sizeof(int));
							val_length = sizeof(int);

							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_INT4, 
								new IntPtr(buffer_ptr + buffer_offset), ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException("Execution failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
									SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							buffer_offset += val_length;
							break;
						case MaxDBType.SmallInt:
							Array.Copy(BitConverter.GetBytes((short)param.Value), 0, param_buffer, buffer_offset, sizeof(short));
							val_length = sizeof(short);

							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_INT2, 
								new IntPtr(buffer_ptr + buffer_offset), ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException("Execution failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
									SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							buffer_offset += val_length;
							break;
						case MaxDBType.Time:
							DateTime tm = (DateTime)param.Value;
							//ODBC time format
							ODBCTIME tm_odbc;
							tm_odbc.hour = (ushort)(tm.Hour % 0x10000);
							tm_odbc.minute = (ushort)(tm.Minute % 0x10000);
							tm_odbc.second = (ushort)(tm.Second % 0x10000);

							val_length = sizeof(ODBCTIME);
							
							Array.Copy(ODBCConverter.GetBytes(tm_odbc), 0, param_buffer, buffer_offset, val_length);

							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_ODBCTIME, 
								new IntPtr(buffer_ptr + buffer_offset), ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException("Execution failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
									SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							buffer_offset += val_length;
							break;
						case MaxDBType.TimeStamp:
							DateTime ts = (DateTime)param.Value;
							//ODBC timestamp format
							ODBCTIMESTAMP ts_odbc;
							ts_odbc.year = (short)(ts.Year % 0x10000);
							ts_odbc.month = (ushort)(ts.Month % 0x10000);
							ts_odbc.day = (ushort)(ts.Day % 0x10000);
							ts_odbc.hour = (ushort)(ts.Hour % 0x10000);
							ts_odbc.minute = (ushort)(ts.Minute % 0x10000);
							ts_odbc.second = (ushort)(ts.Second % 0x10000);
							ts_odbc.fraction = (uint) ts.Millisecond * 1000000;

							val_length = sizeof(ODBCTIMESTAMP);

							Array.Copy(ODBCConverter.GetBytes(ts_odbc), 0, param_buffer, buffer_offset, val_length);

							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_ODBCTIMESTAMP, 
								new IntPtr(buffer_ptr + buffer_offset),	ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_FALSE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException("Execution failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
									SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							buffer_offset += val_length;
							break;
						case MaxDBType.Unicode: case MaxDBType.StrUni: case MaxDBType.VarCharUni:
							val_length = (((string)param.Value).Length + 1) * sizeof(char);
							val_size[i - 1] = SQLDBC.SQLDBC_NTS;
							Array.Copy(Encoding.Unicode.GetBytes((string)param.Value), 0, param_buffer, buffer_offset, val_length - sizeof(char));
							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2_SWAPPED, 
								new IntPtr(buffer_ptr + buffer_offset), ref val_size[i - 1], val_length, SQLDBC_BOOL.SQLDBC_TRUE) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException("Execution failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
									SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							buffer_offset += val_length;
							break;
					}
				}

				SQLDBC_Retcode rc = SQLDBC.SQLDBC_PreparedStatement_executeASCII(stmt);

				if(rc != SQLDBC_Retcode.SQLDBC_OK && rc != SQLDBC_Retcode.SQLDBC_NO_DATA_FOUND) 
					throw new MaxDBException("Execution failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
			}
		}

		void IDbCommand.Prepare()
		{
			// The Prepare is a no-op since parameter preparing and query execution
			// has to be in the single "fixed" block of code
		}

		void IDisposable.Dispose() 
		{
			if (stmt != IntPtr.Zero)
				SQLDBC.SQLDBC_Connection_releasePreparedStatement(m_connection.connHandler, stmt);
			System.GC.SuppressFinalize(this);
		}
	}
}

