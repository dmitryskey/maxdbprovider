//	Copyright (C) 2005-2006 Dmitry S. Kataev
//	Copyright (C) 2002-2003 SAP AG
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
using System.IO;
using MaxDB.Data.MaxDBProtocol;
using MaxDB.Data.Utilities;
using System.Runtime.Serialization;
using System.Security.Permissions;
using System.Globalization;
using System.Data.Common;

namespace MaxDB.Data
{
    /// <summary>
    /// Summary description for MaxDBException.
    /// </summary>

    [Serializable]
    public class PartNotFoundException : Exception
    {
        public PartNotFoundException()
            : base()
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
    public class MaxDBCommunicationException : DataException
    {
        public MaxDBCommunicationException()
            : base()
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

    [Serializable]
    public class MaxDBException : 
#if NET20
        DbException
#else
        DataException
#endif // NET20
    {
        private int iErrorPosition = -10899;
        private string strSqlState;
        private int iErrorCode;

        public MaxDBException()
            : base()
        {
        }

        public MaxDBException(string message)
            : base(message)
        {
        }

        public MaxDBException(string message, Exception innerException)
            :
            base(message, innerException)
        {
        }

        protected MaxDBException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public MaxDBException(string message, string sqlState)
            : base(message)
        {
            strSqlState = sqlState;
        }

        public MaxDBException(string message, string sqlState, Exception innerException)
            : base(message, innerException)
        {
            strSqlState = sqlState;
        }

        public MaxDBException(string message, string sqlState, int vendorCode)
            : base(message)
        {
            strSqlState = sqlState;
            iErrorCode = vendorCode;
        }

        public MaxDBException(string message, string sqlState, int vendorCode, Exception innerException)
            : base(message, innerException)
        {
            strSqlState = sqlState;
            iErrorCode = vendorCode;
        }

        public MaxDBException(string message, string sqlState, int vendorCode, int errorPosition)
            : base(message)
        {
            strSqlState = sqlState;
            iErrorCode = vendorCode;
            iErrorPosition = errorPosition;
        }

        public MaxDBException(string message, string sqlState, int vendorCode, int errorPosition, Exception innerException)
            : base(message, innerException)
        {
            strSqlState = sqlState;
            iErrorCode = vendorCode;
            iErrorPosition = errorPosition;
        }

        [SecurityPermissionAttribute(SecurityAction.Demand, SerializationFormatter = true)]
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "info"));

            base.GetObjectData(info, context);
            info.AddValue("VendorCode", ErrorCode);
            info.AddValue("ErrorPos", ErrorPos);
            info.AddValue("SqlState", SqlState);
        }

#if NET20
        public override int ErrorCode
#else
        public int ErrorCode
#endif // NEt20
        {
            get
            {
                return iErrorCode;
            }
        }

        public int ErrorPos
        {
            get
            {
                return iErrorPosition;
            }
        }

        public string SqlState
        {
            get
            {
                return strSqlState;
            }
        }

        public virtual bool IsConnectionReleasing
        {
            get
            {
                switch (iErrorCode)
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

#if !SAFE
		public static void ThrowException(string message, IntPtr errorHandler)
		{
			ThrowException(message, errorHandler, null);
		}

		public static void ThrowException(string message, IntPtr errorHandler, Exception innerException)
		{
			throw new MaxDBException(message + ": " + UnsafeNativeMethods.SQLDBC_ErrorHndl_getErrorText(errorHandler), 
				UnsafeNativeMethods.SQLDBC_ErrorHndl_getSQLState(errorHandler), UnsafeNativeMethods.SQLDBC_ErrorHndl_getErrorCode(errorHandler),
				innerException);
		}
#endif
    }

    [Serializable]
    public class DatabaseException : MaxDBException
    {
        public DatabaseException()
            : base()
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
    public class MaxDBConnectionException : MaxDBException
    {
        public MaxDBConnectionException()
            : base()
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
            : base((ex == null ? string.Empty : ex.Message), "08000", (ex == null ? -1 : ex.ErrorCode))
        {
        }

        protected MaxDBConnectionException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public override bool IsConnectionReleasing
        {
            get
            {
                return true;
            }
        }
    }

    [Serializable]
    public class MaxDBTimeoutException : DatabaseException
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
            :
            base(message, innerException)
        {
        }

        protected MaxDBTimeoutException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public override bool IsConnectionReleasing
        {
            get
            {
                return true;
            }
        }
    }

    [Serializable]
    public class MaxDBConversionException : MaxDBException
    {
        public MaxDBConversionException()
            : base()
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
    public class MaxDBValueOverflowException : MaxDBException
    {
        public MaxDBValueOverflowException()
            : base()
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

        public MaxDBValueOverflowException(string typeName, int colIndex)
            : base(MaxDBMessages.Extract(MaxDBError.VALUEOVERFLOW, colIndex.ToString((CultureInfo.InvariantCulture))))
        {
        }

        protected MaxDBValueOverflowException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }

    [Serializable]
    public class StreamIOException : IOException
    {
        private DataException mSqlException;

        public StreamIOException()
            : base()
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
            : base()
        {
            mSqlException = sqlEx;
        }

        protected StreamIOException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public DataException SqlException
        {
            get
            {
                return mSqlException;
            }
        }

        [SecurityPermissionAttribute(SecurityAction.Demand, SerializationFormatter = true)]
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
                throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "info"));

            base.GetObjectData(info, context);
            info.AddValue("mSqlException", mSqlException);
        }

    }

    [Serializable]
    public class InvalidColumnException : DataException
    {
        public InvalidColumnException()
            : base()
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
