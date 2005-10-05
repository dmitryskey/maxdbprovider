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
				throw new MaxDBException("Connection must valid and open");

			// Execute the command.
			IntPtr stmt = SQLDBC.SQLDBC_Connection_createPreparedStatement(m_connection.connHandler);

			int rc;

			if (m_connection.DatabaseEncoding is UnicodeEncoding)
				rc = SQLDBC.SQLDBC_PreparedStatement_prepareNTS(stmt, m_connection.DatabaseEncoding.GetBytes(m_sCmdText), StringEncodingType.UCS2Swapped);
			else
				rc = SQLDBC.SQLDBC_PreparedStatement_prepareASCII(stmt, m_sCmdText);
			
			if(0 != rc) 
			{
				IntPtr herror = SQLDBC.SQLDBC_PreparedStatement_getError(stmt);
				throw new MaxDBException("Execution failed " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(herror));
			}

			foreach (MaxDBParameter param in m_parameters)
			{
				switch(param.m_dbType)
				{
					case MaxDBType.Boolean:
						break;//???
				}
			}

			if(SQLDBC.SQLDBC_PreparedStatement_executeASCII(stmt) != 0) 
			{
				IntPtr herror = SQLDBC.SQLDBC_PreparedStatement_getError(stmt);
				throw new MaxDBException("Execution failed " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(herror));
			}

			SQLDBC.SQLDBC_Connection_releasePreparedStatement(m_connection.connHandler, stmt);

			// use SQLDBC_PreparedStatement_getRowsAffected???
			return 0;
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

