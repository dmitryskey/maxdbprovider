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

			if (m_connection.DatabaseEncoding is UnicodeEncoding)
				rc = SQLDBC.SQLDBC_PreparedStatement_prepareNTS(stmt, m_connection.DatabaseEncoding.GetBytes(m_sCmdText), StringEncodingType.UCS2Swapped);
			else
				rc = SQLDBC.SQLDBC_PreparedStatement_prepareASCII(stmt, m_sCmdText);
			
			if(rc != SQLDBC_Retcode.SQLDBC_OK) 
				throw new MaxDBException("Execution failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
					SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));

			BindParameters(stmt, m_parameters);

			rc = SQLDBC.SQLDBC_PreparedStatement_executeASCII(stmt);

			if(rc != SQLDBC_Retcode.SQLDBC_OK && rc != SQLDBC_Retcode.SQLDBC_NO_DATA_FOUND) 
				throw new MaxDBException("Execution failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));

			SQLDBC.SQLDBC_Connection_releasePreparedStatement(m_connection.connHandler, stmt);

			// use SQLDBC_PreparedStatement_getRowsAffected???
			return 0;
		}

		private unsafe void BindParameters(IntPtr stmt, MaxDBParameterCollection parameters)
		{
			for(ushort i = 1; i <= parameters.Count; i++)
			{
				MaxDBParameter param = (MaxDBParameter)parameters[i];
				int val_length;
				switch(param.m_dbType)
				{
					case MaxDBType.Boolean:
						byte byte_val = (byte)((bool)param.Value ? 1 : 0);
						val_length = sizeof(byte);
						if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_UINT1, new IntPtr(&byte_val), 
							ref val_length, sizeof(byte), 0) != SQLDBC_Retcode.SQLDBC_OK) 
							throw new MaxDBException("Execution failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
								SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
						break;
					case MaxDBType.CharA:case MaxDBType.CharB:
						fixed (byte* byte_ref = Encoding.ASCII.GetBytes((string)param.Value))
						{
							val_length = ((string)param.Value).Length;
							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_ASCII, new IntPtr(byte_ref), 
									ref val_length, val_length, 0) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException("Execution failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
									SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
						}
						break;
					case MaxDBType.Date:
						DateTime dt = (DateTime)param.Value;
						byte[] b = new byte[6];//ODBC date format
						b[0] = (byte)(dt.Year % 0x100);
						b[1] = (byte)(dt.Year / 0x100);
						b[2] = (byte)(dt.Month % 0x100);
						b[3] = (byte)(dt.Month / 0x100);
						b[4] = (byte)(dt.Day % 0x100);
						b[5] = (byte)(dt.Day / 0x100);
						val_length = b.Length;
						fixed (byte* byte_ref = b)
						{
							if(SQLDBC.SQLDBC_PreparedStatement_bindParameter(stmt, i, SQLDBC_HostType.SQLDBC_HOSTTYPE_ODBCDATE, new IntPtr(byte_ref), 
								ref val_length, val_length, 0) != SQLDBC_Retcode.SQLDBC_OK) 
								throw new MaxDBException("Execution failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
									SQLDBC.SQLDBC_PreparedStatement_getError(stmt)));
						}
						break;
					
				}
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

