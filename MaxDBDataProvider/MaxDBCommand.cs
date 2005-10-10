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

		// Implement the default constructor here.
		public MaxDBCommand()
		{
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
		}

		public MaxDBCommand(string cmdText, MaxDBConnection connection, MaxDBTransaction txn)
		{
			m_sCmdText    = cmdText;
			m_connection  = connection;
			m_txn      = txn;
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
			// The sample does not support canceling a command
			// once it has been initiated.
			throw new NotSupportedException();
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
			IntPtr stmt = SQLDBC.SQLDBC_Connection_createPreparedStatement(m_connection.connHandler);

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

		private unsafe void BindAndExecute(IntPtr stmt, MaxDBParameterCollection parameters)
		{
			int buffer_length = 0;
			int buffer_offset = 0;

			for(ushort i = 1; i <= parameters.Count; i++)
			{
				MaxDBParameter param = (MaxDBParameter)parameters[i];
				switch(param.m_dbType)
				{
					case MaxDBType.Boolean:
						buffer_length += sizeof(byte);
						break;
					case MaxDBType.CharA: case MaxDBType.CharB: case MaxDBType.StrA: case MaxDBType.StrB: case MaxDBType.VarCharA: case MaxDBType.VarCharB:
						buffer_length += ((string)param.Value).Length * sizeof(byte);
						break;
					case MaxDBType.Date: case MaxDBType.Time:
						buffer_length += 3 * sizeof(ushort);
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
						buffer_length += (6 + sizeof(uint) / sizeof(ushort)) * sizeof(ushort);
						break;
					case MaxDBType.Unicode: case MaxDBType.StrUni: case MaxDBType.VarCharUni:
						buffer_length += ((string)param.Value).Length * sizeof(char);
						break;
				}
			}

			byte[] param_buffer = new byte[buffer_length];

			fixed(byte *buffer_ptr = param_buffer)
			{

				for(ushort i = 1; i <= parameters.Count; i++)
				{
					MaxDBParameter param = (MaxDBParameter)parameters[i];
					int val_length;
					switch(param.m_dbType)
					{
						case MaxDBType.Boolean:
							param_buffer[buffer_offset] = (byte)((bool)param.Value ? 1 : 0);
							val_length = sizeof(byte);
							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_UINT1, 
									new IntPtr(buffer_ptr + buffer_offset), ref val_length, sizeof(byte), 0) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException("Execution failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
									SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							buffer_offset += val_length;
							break;
						case MaxDBType.CharA: case MaxDBType.CharB: case MaxDBType.StrA: case MaxDBType.StrB: case MaxDBType.VarCharA: case MaxDBType.VarCharB:
							val_length = ((string)param.Value).Length * sizeof(byte);
							Array.Copy(Encoding.ASCII.GetBytes((string)param.Value), 0 , param_buffer, buffer_offset, val_length); 
							
							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_ASCII, 
									new IntPtr(buffer_ptr + buffer_offset),	ref val_length, val_length, 0) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException("Execution failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
									SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							buffer_offset += val_length;
							break;
						case MaxDBType.Date:
							DateTime dt = (DateTime)param.Value;
							//ODBC date format
							short[] dt_array = new short[3]{(short)(dt.Year % 0x10000), (short)(dt.Month % 0x10000), (short)(dt.Day % 0x10000)};
							val_length = dt_array.Length * sizeof(short);
							
							for(int idx = 0; idx < dt_array.Length; idx++)
								Array.Copy(BitConverter.GetBytes(dt_array[idx]), 0, param_buffer, buffer_offset, sizeof(short));
	
							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_ODBCDATE, 
									new IntPtr(buffer_ptr + buffer_offset), ref val_length, val_length, 0) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException("Execution failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
									SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							
							buffer_offset += val_length;
							break;
						case MaxDBType.Fixed: case MaxDBType.Float: case MaxDBType.VFloat:
							Array.Copy(BitConverter.GetBytes((double)param.Value), 0, param_buffer, buffer_offset, sizeof(double));
							val_length = sizeof(double);
							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_DOUBLE, 
									new IntPtr(buffer_ptr + buffer_offset), ref val_length, val_length, 0) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException("Execution failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
										SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							buffer_offset += val_length;
							break;
						case MaxDBType.Integer:
							Array.Copy(BitConverter.GetBytes((int)param.Value), 0, param_buffer, buffer_offset, sizeof(int));
							val_length = sizeof(int);
							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_INT4, 
									new IntPtr(buffer_ptr + buffer_offset), ref val_length, val_length, 0) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException("Execution failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
										SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							buffer_offset += val_length;
							break;
						case MaxDBType.SmallInt:
							Array.Copy(BitConverter.GetBytes((short)param.Value), 0, param_buffer, buffer_offset, sizeof(short));
							val_length = sizeof(short);
							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_INT2, 
									new IntPtr(buffer_ptr + buffer_offset), ref val_length, val_length, 0) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException("Execution failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
										SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							buffer_offset += val_length;
							break;
						case MaxDBType.Time:
							DateTime tm = (DateTime)param.Value;
							//ODBC time format
							short[] tm_array = new short[3]{(short)(tm.Hour % 0x10000), (short)(tm.Minute % 0x10000), (short)(tm.Second % 0x10000)};
							val_length = tm_array.Length * sizeof(short);

							for(int idx = 0; idx < tm_array.Length; idx++)
								Array.Copy(BitConverter.GetBytes(tm_array[idx]), 0, param_buffer, buffer_offset, sizeof(short));

							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_ODBCTIME, 
									new IntPtr(buffer_ptr + buffer_offset), ref val_length, val_length, 0) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException("Execution failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
										SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							buffer_offset += val_length;
							break;
						case MaxDBType.TimeStamp:
							DateTime ts = (DateTime)param.Value;
							//ODBC timestamp format
							ushort[] ts_array = new ushort[6 + sizeof(uint) / sizeof(ushort)];
							ts_array[0] = (ushort)(ts.Year % 0x10000);
							ts_array[1] = (ushort)(ts.Month % 0x10000);
							ts_array[2] = (ushort)(ts.Day % 0x10000);
							ts_array[3] = (ushort)(ts.Hour % 0x10000);
							ts_array[4] = (ushort)(ts.Minute % 0x10000);
							ts_array[5] = (ushort)(ts.Second % 0x10000);
							uint fraction = (uint) ts.Millisecond * 1000000;
							if (BitConverter.IsLittleEndian)
							{
								ts_array[6] = (ushort)(fraction % 0x10000);
								ts_array[7] = (ushort)(fraction / 0x10000);
							}
							else
							{
								ts_array[6] = (ushort)(fraction / 0x10000);
								ts_array[7] = (ushort)(fraction % 0x10000);
							}
							val_length = ts_array.Length * sizeof(ushort);

							for(int idx = 0; idx < ts_array.Length; idx++)
								Array.Copy(BitConverter.GetBytes(ts_array[idx]), 0, param_buffer, buffer_offset, sizeof(ushort));

							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_ODBCTIMESTAMP, 
									new IntPtr(buffer_ptr + buffer_offset),	ref val_length, val_length, 0) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException("Execution failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
										SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
							buffer_offset += val_length;
							break;
						case MaxDBType.Unicode: case MaxDBType.StrUni: case MaxDBType.VarCharUni:
							val_length = ((string)param.Value).Length * sizeof(char);
							Array.Copy(Encoding.Unicode.GetBytes((string)param.Value), 0, param_buffer, buffer_offset, val_length);
							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_UCS2_SWAPPED, 
									new IntPtr(buffer_ptr + buffer_offset), ref val_length, val_length, 0) != SQLDBC_Retcode.SQLDBC_OK) 
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

		public IDataReader ExecuteReader()
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
//			SampleDb.SampleDbResultSet resultset;
//			m_connection.SampleDb.Execute(m_sCmdText, out resultset);
//
//			return new TemplateDataReader(resultset);
			return null;
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
//			SampleDb.SampleDbResultSet resultset;
//			m_connection.SampleDb.Execute(m_sCmdText, out resultset);

			/*
			 * The only CommandBehavior option supported by this
			 * sample is the automatic closing of the connection
			 * when the user is done with the reader.
			 */
//			if (behavior == CommandBehavior.CloseConnection)
//				return new TemplateDataReader(resultset, m_connection);
//			else
//				return new TemplateDataReader(resultset);
			return null;
		}

		public object ExecuteScalar()
		{
			/*
			 * ExecuteScalar assumes that the command will return a single
			 * row with a single column, or if more rows/columns are returned
			 * it will return the first column of the first row.
			 */

			// There must be a valid and open connection.
			if (m_connection == null || m_connection.State != ConnectionState.Open)
				throw new InvalidOperationException("Connection must valid and open");

			// Execute the command.
//			SampleDb.SampleDbResultSet resultset;
//			m_connection.SampleDb.Execute(m_sCmdText, out resultset);
//
//			// Return the first column of the first row.
//			// Return a null reference if there is no data.
//			if (resultset.data.Length == 0)
//				return null;
//
//			return resultset.data[0, 0];

			return null;
		}

		public void Prepare()
		{
			// The sample Prepare is a no-op.
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
		}

	}
}

