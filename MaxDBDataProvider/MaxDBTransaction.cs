//	Copyright (C) 2005-2006 Dmitry S. Kataev
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
using System.Data;
using MaxDBDataProvider.Utils;

namespace MaxDBDataProvider
{
	public class MaxDBTransaction : IDbTransaction, IDisposable
	{
		private MaxDBConnection m_connection;

		internal MaxDBTransaction(MaxDBConnection conn)
		{
			m_connection = conn;
		}

		#region IDbTransaction Members

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

		void IDbTransaction.Commit()
		{
			Commit();
		}

		public MaxDBConnection Connection
		{
			get  
			{ 
				return m_connection; 
			}
		}
 
		IDbConnection IDbTransaction.Connection
		{
			get
			{
				return Connection;
			}
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

		IsolationLevel IDbTransaction.IsolationLevel
		{
			get
			{
				return IsolationLevel;
			}
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

		void IDbTransaction.Rollback()
		{
			Rollback();
		}

		#endregion   

		#region IDisposable Members

		public void Dispose()
		{
			if (null != m_connection) 
				// implicitly rollback if transaction still valid
				Rollback();
			GC.SuppressFinalize(this);
		}

		#endregion
	}
}

