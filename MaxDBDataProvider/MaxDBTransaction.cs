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
	/// <summary>
	/// Represents a SQL transaction to be made in a MaxDB database. This class cannot be inherited.
	/// </summary>
	/// <remarks>
	/// The application creates a <B>MaxDBTransaction</B> object by calling <see cref="MaxDBConnection.BeginTransaction()"/>
	/// on the <see cref="MaxDBConnection"/> object. All subsequent operations associated with the 
	/// transaction (for example, committing or aborting the transaction), are performed on the  <B>MaxDBTransaction</B> object.
	/// </remarks>
	public sealed class MaxDBTransaction :
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

		/// <summary>
		/// Commits the database transaction.
		/// </summary>
		/// <remarks>
		/// The <b>Commit</b> method is equivalent to the MaxDB SQL statement COMMIT [WORK].
		/// </remarks>
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
			dbConnection.mComm.Commit(dbConnection.mConnArgs);
#else
			if (UnsafeNativeMethods.SQLDBC_Connection_commit(dbConnection.mComm.mConnectionHandler) != SQLDBC_Retcode.SQLDBC_OK)
				MaxDBException.ThrowException("COMMIT", UnsafeNativeMethods.SQLDBC_Connection_getError(dbConnection.mComm.mConnectionHandler));
#endif // SAFE
		}

		/// <summary>
		/// This property is intended for internal use and can not to be used directly from your code.
		/// </summary>
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

		/// <summary>
		/// Gets the <see cref="MaxDBConnection"/> object associated with the transaction, or a null reference (Nothing in Visual Basic) 
		/// if the transaction is no longer valid.
		/// </summary>
		/// <value>The <see cref="MaxDBConnection"/> object associated with this transaction.</value>
		/// <remarks>
		/// A single application may have multiple database connections, each 
		/// with zero or more transactions. This property enables you to 
		/// determine the connection object associated with a particular 
		/// transaction created by <see cref="MaxDBConnection.BeginTransaction()"/>.
		/// </remarks>
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

		/// <summary>
		/// Specifies the <see cref="IsolationLevel"/> for this transaction.
		/// </summary>
		/// <value>
		/// The <see cref="IsolationLevel"/> for this transaction. The default is <b>ReadCommitted</b>.
		/// </value>
		/// <remarks>
		/// Parallel transactions are not supported. Therefore, the IsolationLevel applies to the entire transaction.
		/// </remarks>
#if NET20
		public override IsolationLevel IsolationLevel
#else
		public IsolationLevel IsolationLevel
#endif // NET20
		{
			get
			{
#if SAFE
				return dbConnection.mComm.mIsolationLevel;
#else
				switch (UnsafeNativeMethods.SQLDBC_Connection_getTransactionIsolation(dbConnection.mComm.mConnectionHandler))
				{
					case 0:
						return IsolationLevel.ReadUncommitted;
					case 1:
					case 10:
						return IsolationLevel.ReadCommitted;
					case 2:
					case 20:
						return IsolationLevel.RepeatableRead;
					case 3:
					case 30:
						return IsolationLevel.Serializable;
					default:
						return IsolationLevel.Unspecified;
				}
#endif // SAFE
			}
		}

		/// <summary>
		/// Rollbacks the database transaction.
		/// </summary>
		/// <remarks>
		/// The <b>Rollback</b> method is equivalent to the MaxDB SQL statement ROLLBACK [WORK].
		/// </remarks>
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
			dbConnection.mComm.Rollback(dbConnection.mConnArgs);
#else
			if (UnsafeNativeMethods.SQLDBC_Connection_rollback(dbConnection.mComm.mConnectionHandler) != SQLDBC_Retcode.SQLDBC_OK)
				throw new MaxDBException("ROLLBACK" + UnsafeNativeMethods.SQLDBC_Connection_getError(dbConnection.mComm.mConnectionHandler));
#endif // SAFE
		}

		#endregion

		#region IDisposable Members

#if !NET20
		void IDisposable.Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}
#endif // NET20

		/// <summary>
		/// This method is intended for internal use and can not to be called directly from your code.
		/// </summary>
		/// <param name="disposing">The disposing flag.</param>
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

