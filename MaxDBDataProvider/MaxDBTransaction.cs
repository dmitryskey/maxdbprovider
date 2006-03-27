using System;
using System.Data;
using MaxDBDataProvider.Utils;

namespace MaxDBDataProvider
{
	public class MaxDBTransaction : IDbTransaction
	{
		private MaxDBConnection m_connection;

		internal MaxDBTransaction(MaxDBConnection conn)
		{
			m_connection = conn;
		}

		public IsolationLevel IsolationLevel 
		{
			get 
			{
#if SAFE
				return m_connection.m_isolationLevel;
#else
				switch (SQLDBC.SQLDBC_Connection_getTransactionIsolation(m_connection.m_connHandler))
				{
					case 0:
						return IsolationLevel.ReadUncommitted;
					case 1: case 10:
						return IsolationLevel.ReadCommitted;
					case 2: case 20:
						return IsolationLevel.RepeatableRead;
					case 3: case 30:
						return IsolationLevel.Serializable;
					default:
						return IsolationLevel.Unspecified;
				}
#endif
			}
		}

		public void Commit()
		{
			m_connection.AssertOpen();
#if SAFE
			m_connection.Commit();
#else
			if(SQLDBC.SQLDBC_Connection_commit(m_connection.m_connHandler) != SQLDBC_Retcode.SQLDBC_OK) 
				throw new MaxDBException("COMMIT failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
					SQLDBC.SQLDBC_Connection_getError(m_connection.m_connHandler)));
#endif
		}

		public void Rollback()
		{
			m_connection.AssertOpen();
#if SAFE
			m_connection.Rollback();
#else
			if(SQLDBC.SQLDBC_Connection_rollback(m_connection.m_connHandler) != SQLDBC_Retcode.SQLDBC_OK) 
				throw new MaxDBException("ROLLBACK failed: " + SQLDBC.SQLDBC_ErrorHndl_getErrorText(
					SQLDBC.SQLDBC_Connection_getError(m_connection.m_connHandler)));
#endif
		}

		public IDbConnection Connection
		{
			/*
			 * Return the connection for the current transaction.
			 */

			get  
			{ 
				return m_connection; 
			}
		}    

		public void Dispose() 
		{
			this.Dispose(true);
			System.GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) 
		{
			if (disposing) 
				if (null != m_connection) 
					// implicitly rollback if transaction still valid
					Rollback();
		}

	}
}

