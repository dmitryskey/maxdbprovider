//-----------------------------------------------------------------------------------------------
// <copyright file="MaxDBException.cs" company="2005-2019 Dmitry S. Kataev, 2002-2003 SAP AG">
// Copyright (c) 2005-2019 Dmitry S. Kataev, 2002-2003 SAP AG. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------------------------------
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
    using System.Globalization;
    using System.IO;
    using System.Runtime.Serialization;
    using System.Security.Permissions;
    using MaxDB.Data.MaxDBProtocol;

    /// <summary>
    /// Summary description for MaxDBException.
    /// </summary>
    [Serializable]
    internal class PartNotFoundException : Exception
    {
        public PartNotFoundException()
        {
        }

        public PartNotFoundException(string msg)
            : base(msg)
        {
        }

        public PartNotFoundException(string msg, Exception ex)
            : base(msg, ex)
        {
        }

        protected PartNotFoundException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }

    [Serializable]
    internal class MaxDBCommunicationException : DataException
    {
        public MaxDBCommunicationException()
        {
        }

        public MaxDBCommunicationException(int code)
            : base(CommError.ErrorText[code])
        {
        }

        public MaxDBCommunicationException(string msg)
            : base(msg)
        {
        }

        public MaxDBCommunicationException(string msg, Exception ex)
            : base(msg, ex)
        {
        }

        protected MaxDBCommunicationException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }

    /// <summary>
    /// The exception that is thrown when MaxDB returns an error.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This class is created whenever the MaxDB Data Provider encounters an error generated from the server.
    /// </para>
    /// <para>
    /// Any open connections are not automatically closed when an exception is thrown.  If
    /// the client application determines that the exception is fatal, it should close any open
    /// <see cref="MaxDBDataReader"/> objects or <see cref="MaxDBConnection"/> objects.
    /// </para>
    /// </remarks>
    /// <example>
    /// The following example generates a <B>MaxDBException</B> due to a missing server,
    /// and then displays the exception.
    ///
    /// <code lang="Visual Basic">
    /// Public Sub ShowException()
    ///     Dim mySelectQuery As String = "SELECT column1 FROM table1"
    ///     Dim myConnection As New MaxDBConnection ("Data Source=localhost;Database=Sample;")
    ///     Dim myCommand As New MaxDBCommand(mySelectQuery, myConnection)
    ///
    ///     Try
    ///         myCommand.Connection.Open()
    ///     Catch e As MaxDBException
    ///         MessageBox.Show( e.Message )
    ///     End Try
    /// End Sub
    /// </code>
    /// <code lang="C#">
    /// public void ShowException()
    /// {
    ///     string mySelectQuery = "SELECT column1 FROM table1";
    ///     MaxDBConnection myConnection =
    ///         new MaxDBConnection("Data Source=localhost;Database=Sample;");
    ///     MaxDBCommand myCommand = new MaxDBCommand(mySelectQuery,myConnection);
    ///
    ///     try
    ///     {
    ///         myCommand.Connection.Open();
    ///     }
    ///     catch (MaxDBException e)
    ///     {
    ///         MessageBox.Show( e.Message );
    ///     }
    /// }
    /// </code>
    /// </example>
    [Serializable]
    public class MaxDBException : DbException
    {
        private readonly int iErrorCode;

        internal MaxDBException()
        {
        }

        internal MaxDBException(string message)
            : base(message)
        {
        }

        internal MaxDBException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        /// <summary>
        /// This constructor is intended for internal use and can not to be called directly from your code.
        /// </summary>
        protected MaxDBException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        internal MaxDBException(string message, string sqlState)
            : base(message) => this.SqlState = sqlState;

        internal MaxDBException(string message, string sqlState, Exception innerException)
            : base(message, innerException) => this.SqlState = sqlState;

        internal MaxDBException(string message, string sqlState, int vendorCode)
            : base(message)
        {
            this.SqlState = sqlState;
            this.iErrorCode = vendorCode;
        }

        internal MaxDBException(string message, string sqlState, int vendorCode, Exception innerException)
            : base(message, innerException)
        {
            this.SqlState = sqlState;
            this.iErrorCode = vendorCode;
        }

        internal MaxDBException(string message, string sqlState, int vendorCode, int errorPosition)
            : base(message)
        {
            this.SqlState = sqlState;
            this.iErrorCode = vendorCode;
            this.ErrorPos = errorPosition;
        }

        internal MaxDBException(string message, string sqlState, int vendorCode, int errorPosition, Exception innerException)
            : base(message, innerException)
        {
            this.SqlState = sqlState;
            this.iErrorCode = vendorCode;
            this.ErrorPos = errorPosition;
        }

        /// <summary>
        /// This member overrides <see cref="Exception.GetObjectData"/> method
        /// </summary>
        /// <param name="info">Serialization info</param>
        /// <param name="context">Streaming context</param>
        [SecurityPermissionAttribute(SecurityAction.Demand, SerializationFormatter = true)]
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "info"));
            }

            base.GetObjectData(info, context);
            info.AddValue("VendorCode", this.ErrorCode);
            info.AddValue("ErrorPos", this.ErrorPos);
            info.AddValue("SqlState", this.SqlState);
        }

        /// <summary>
        /// Error code
        /// </summary>
        public override int ErrorCode => this.iErrorCode;

        /// <summary>
        /// Error position
        /// </summary>
        public int ErrorPos { get; } = -10899;

        /// <summary>
        /// SQL state
        /// </summary>
        public string SqlState { get; }

        /// <summary>
        /// Check whether connection is releasing
        /// </summary>
        public virtual bool IsConnectionReleasing
        {
            get
            {
                switch (this.iErrorCode)
                {
                    case -904:  // Space for result tables exhausted
                    case -708:  // SERVERDB system not available
                    case +700:  // Session inactivity timeout (work rolled back)
                    case -70:   // Session inactivity timeout (work rolled back)
                    case +710:  // Session terminated by shutdown (work rolled back)
                    case -71:   // Session terminated by shutdown (work rolled back)
                    case +750:  // Too many SQL statements (work rolled back)
                    case -75:   // Too many SQL statements (work rolled back)
                        return true;
                }

                return false;
            }
        }
    }

    [Serializable]
    internal class DatabaseException : MaxDBException
    {
        public DatabaseException()
        {
        }

        public DatabaseException(string message)
            : base(message)
        {
        }

        public DatabaseException(string message, Exception innerException)
            :
            base(message, innerException)
        {
        }

        public DatabaseException(string message, string sqlState, int vendorCode, int errorPosition)
            : base((errorPosition > 1)
            ? MaxDBMessages.Extract(MaxDBError.DATABASEEXCEPTION,
                vendorCode.ToString(CultureInfo.InvariantCulture), errorPosition.ToString(CultureInfo.InvariantCulture), message)
            : MaxDBMessages.Extract(MaxDBError.DATABASEEXCEPTION_WOERRPOS, vendorCode.ToString(CultureInfo.InvariantCulture), message),
            sqlState, vendorCode, errorPosition)
        {
        }

        protected DatabaseException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }

    [Serializable]
    internal class MaxDBConnectionException : MaxDBException
    {
        public MaxDBConnectionException()
        {
        }

        public MaxDBConnectionException(string message)
            : base(message)
        {
        }

        public MaxDBConnectionException(string message, Exception innerException)
            :
            base(message, innerException)
        {
        }

        public MaxDBConnectionException(MaxDBException ex)
            : base(ex == null ? string.Empty : ex.Message, "08000", ex == null ? -1 : ex.ErrorCode)
        {
        }

        protected MaxDBConnectionException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public override bool IsConnectionReleasing => true;
    }

    [Serializable]
    internal class MaxDBTimeoutException : DatabaseException
    {
        public MaxDBTimeoutException()
            : base(MaxDBMessages.Extract(MaxDBError.TIMEOUT), "08000", 700, 0)
        {
        }

        public MaxDBTimeoutException(string message)
            : base(message)
        {
        }

        public MaxDBTimeoutException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        protected MaxDBTimeoutException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public override bool IsConnectionReleasing => true;
    }

    [Serializable]
    internal class MaxDBConversionException : MaxDBException
    {
        public MaxDBConversionException()
        {
        }

        public MaxDBConversionException(string msg)
            : base(msg)
        {
        }

        public MaxDBConversionException(string msg, Exception ex)
            : base(msg, ex)
        {
        }

        protected MaxDBConversionException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }

    [Serializable]
    internal class MaxDBValueOverflowException : MaxDBException
    {
        public MaxDBValueOverflowException()
        {
        }

        public MaxDBValueOverflowException(string msg)
            : base(msg)
        {
        }

        public MaxDBValueOverflowException(string msg, Exception ex)
            : base(msg, ex)
        {
        }

        public MaxDBValueOverflowException(int colIndex)
            : base(MaxDBMessages.Extract(MaxDBError.VALUEOVERFLOW, colIndex.ToString(CultureInfo.InvariantCulture)))
        {
        }

        public MaxDBValueOverflowException(string type, int colIndex)
            : base(MaxDBMessages.Extract(MaxDBError.VALUEOVERFLOW, type + " " + colIndex.ToString(CultureInfo.InvariantCulture)))
        {
        }

        protected MaxDBValueOverflowException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }

    [Serializable]
    internal class StreamIOException : IOException
    {
        public StreamIOException()
        {
        }

        public StreamIOException(string msg)
            : base(msg)
        {
        }

        public StreamIOException(string msg, Exception ex)
            : base(msg, ex)
        {
        }

        public StreamIOException(DataException sqlEx)
        {
            this.SqlException = sqlEx;
        }

        protected StreamIOException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public DataException SqlException { get; }

        [SecurityPermissionAttribute(SecurityAction.Demand, SerializationFormatter = true)]
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
            {
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "info"));
            }

            base.GetObjectData(info, context);
            info.AddValue("mSqlException", this.SqlException);
        }

    }

    [Serializable]
    internal class InvalidColumnException : DataException
    {
        public InvalidColumnException()
        {
        }

        public InvalidColumnException(int columnIndex)
            : base(MaxDBMessages.Extract(MaxDBError.INVALID_COLUMNINDEX, columnIndex))
        {
        }

        public InvalidColumnException(string columnName)
            : base(MaxDBMessages.Extract(MaxDBError.INVALID_COLUMNNAME, columnName))
        {
        }

        public InvalidColumnException(string columnName, Exception ex)
            : base(MaxDBMessages.Extract(MaxDBError.INVALID_COLUMNNAME, columnName), ex)
        {
        }

        protected InvalidColumnException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
