// Copyright © 2005-2018 Dmitry S. Kataev
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

namespace MaxDB.Data
{
    using System;
    using System.Data;
    using System.Data.Common;

    /// <summary>
    /// Represents a SQL transaction to be made in a MaxDB database. This class cannot be inherited.
    /// </summary>
    /// <remarks>
    /// The application creates a <B>MaxDBTransaction</B> object by calling <see cref="MaxDBConnection.BeginTransaction()"/>
    /// on the <see cref="MaxDBConnection"/> object. All subsequent operations associated with the 
    /// transaction (for example, committing or aborting the transaction), are performed on the  <B>MaxDBTransaction</B> object.
    /// </remarks>
    public sealed class MaxDBTransaction : DbTransaction
    {
        internal MaxDBTransaction(MaxDBConnection conn) => this.Connection = conn;

        #region IDbTransaction Members

        /// <summary>
        /// Commits the database transaction.
        /// </summary>
        /// <remarks>
        /// The <b>Commit</b> method is equivalent to the MaxDB SQL statement COMMIT [WORK].
        /// </remarks>
        public override void Commit()
        {
            this.Connection.AssertOpen();

            // >>> SQL TRACE
            this.Connection.mLogger.SqlTrace(DateTime.Now, "::COMMIT");
            // <<< SQL TRACE

            this.Connection.mComm.Commit(this.Connection.mConnArgs);
        }

        /// <summary>
        /// This property is intended for internal use and can not to be used directly from your code.
        /// </summary>
        protected override DbConnection DbConnection => this.Connection;

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
        public new MaxDBConnection Connection { get; }

        /// <summary>
        /// Specifies the <see cref="IsolationLevel"/> for this transaction.
        /// </summary>
        /// <value>
        /// The <see cref="IsolationLevel"/> for this transaction. The default is <b>ReadCommitted</b>.
        /// </value>
        /// <remarks>
        /// Parallel transactions are not supported. Therefore, the IsolationLevel applies to the entire transaction.
        /// </remarks>
        public override IsolationLevel IsolationLevel => this.Connection.mComm.IsolationLevel;

        /// <summary>
        /// Rollbacks the database transaction.
        /// </summary>
        /// <remarks>
        /// The <b>Rollback</b> method is equivalent to the MaxDB SQL statement ROLLBACK [WORK].
        /// </remarks>
        public override void Rollback()
        {
            this.Connection.AssertOpen();

            // >>> SQL TRACE
            this.Connection.mLogger.SqlTrace(DateTime.Now, "::ROLLBACK");
            // <<< SQL TRACE

            this.Connection.mComm.Rollback(this.Connection.mConnArgs);
        }

        #endregion

        #region IDisposable Members

        /// <summary>
        /// This method is intended for internal use and can not to be called directly from your code.
        /// </summary>
        /// <param name="disposing">The disposing flag.</param>
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing && null != this.Connection)
            {
                this.Rollback(); // implicitly rollback if transaction still valid
            }
        }

        #endregion
    }
}