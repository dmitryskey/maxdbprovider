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
using System.Data.Common;
using MaxDB.Data.Utilities;

namespace MaxDB.Data
{
	public class MaxDBTransaction :
#if NET20
        DbTransaction
#else
        IDbTransaction, IDisposable
#endif // NET20
    {
		private MaxDBConnection dbConnection;

		internal MaxDBTransaction(MaxDBConnection conn)
		{
			dbConnection = conn;
		}

		#region IDbTransaction Members

#if NET20
        public override void Commit()
#else
		public void Commit()
#endif // NET20
        {
			dbConnection.AssertOpen();

			//>>> SQL TRACE
			dbConnection.mLogger.SqlTrace(DateTime.Now, "::COMMIT");
			//<<< SQL TRACE
#if SAFE
			dbConnection.Commit();
#else
			if(UnsafeNativeMethods.SQLDBC_Connection_commit(dbConnection.mConnectionHandler) != SQLDBC_Retcode.SQLDBC_OK) 
				MaxDBException.ThrowException("COMMIT", UnsafeNativeMethods.SQLDBC_Connection_getError(dbConnection.mConnectionHandler));
#endif // SAFE
        }

#if NET20
        protected override DbConnection DbConnection
#else
		IDbConnection IDbTransaction.Connection
#endif // NET20
        {
			get 
			{
				return this.Connection;
			}
		}

#if NET20
		public new MaxDBConnection Connection
#else
        public MaxDBConnection Connection
#endif // NET20
        {
			get  
			{ 
				return dbConnection; 
			}
		}
 
#if NET20
        public override IsolationLevel IsolationLevel
#else
		public IsolationLevel IsolationLevel
#endif // NET20
        {
			get 
			{
#if SAFE
				return dbConnection.mIsolationLevel;
#else
				switch (UnsafeNativeMethods.SQLDBC_Connection_getTransactionIsolation(dbConnection.mConnectionHandler))
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
#endif // SAFE
            }
		}

#if NET20
        public override void Rollback()
#else
        public void Rollback()
#endif // NET20
        {
			dbConnection.AssertOpen();

			//>>> SQL TRACE
			dbConnection.mLogger.SqlTrace(DateTime.Now, "::ROLLBACK");
			//<<< SQL TRACE
#if SAFE
			dbConnection.Rollback();
#else
			if(UnsafeNativeMethods.SQLDBC_Connection_rollback(dbConnection.mConnectionHandler) != SQLDBC_Retcode.SQLDBC_OK) 
				throw new MaxDBException("ROLLBACK" + UnsafeNativeMethods.SQLDBC_Connection_getError(dbConnection.mConnectionHandler));
#endif // SAFE
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
            if (disposing && null != dbConnection)
                Rollback();// implicitly rollback if transaction still valid
        }

		#endregion
	}
}

