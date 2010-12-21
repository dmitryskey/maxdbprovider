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
#if !SAFE
using MaxDB.Data.Utilities;
#endif
using System.Runtime.Serialization;
using System.Security.Permissions;
using System.Globalization;
using System.Data.Common;
#if !SAFE
using System.Runtime.InteropServices;
#endif

namespace MaxDB.Data
{
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
	///		Dim mySelectQuery As String = "SELECT column1 FROM table1"
	///		Dim myConnection As New MaxDBConnection ("Data Source=localhost;Database=Sample;")
	///		Dim myCommand As New MaxDBCommand(mySelectQuery, myConnection)
	///
	///		Try
	///			myCommand.Connection.Open()
	///		Catch e As MaxDBException
	///			MessageBox.Show( e.Message )
	///		End Try
	///	End Sub
	/// </code>
	/// <code lang="C#">
	/// public void ShowException() 
	/// {
	///		string mySelectQuery = "SELECT column1 FROM table1";
	///		MaxDBConnection myConnection =
	///			new MaxDBConnection("Data Source=localhost;Database=Sample;");
	///		MaxDBCommand myCommand = new MaxDBCommand(mySelectQuery,myConnection);
	///
	///		try 
	///		{
	///			myCommand.Connection.Open();
	///		}
	///		catch (MaxDBException e) 
	///		{
	///			MessageBox.Show( e.Message );
	///		}
	///	}
	///	</code>
	/// </example>
	[Serializable]
	public class MaxDBException :
#if NET20
		DbException
#else
		DataException
#endif // NET20
	{
		private readonly int iErrorPosition = -10899;
		private readonly string strSqlState;
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
			: base(message)
		{
			strSqlState = sqlState;
		}

		internal MaxDBException(string message, string sqlState, Exception innerException)
			: base(message, innerException)
		{
			strSqlState = sqlState;
		}

		internal MaxDBException(string message, string sqlState, int vendorCode)
			: base(message)
		{
			strSqlState = sqlState;
			iErrorCode = vendorCode;
		}

		internal MaxDBException(string message, string sqlState, int vendorCode, Exception innerException)
			: base(message, innerException)
		{
			strSqlState = sqlState;
			iErrorCode = vendorCode;
		}

		internal MaxDBException(string message, string sqlState, int vendorCode, int errorPosition)
			: base(message)
		{
			strSqlState = sqlState;
			iErrorCode = vendorCode;
			iErrorPosition = errorPosition;
		}

		internal MaxDBException(string message, string sqlState, int vendorCode, int errorPosition, Exception innerException)
			: base(message, innerException)
		{
			strSqlState = sqlState;
			iErrorCode = vendorCode;
			iErrorPosition = errorPosition;
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
				throw new MaxDBException(MaxDBMessages.Extract(MaxDBError.PARAMETER_NULL, "info"));

			base.GetObjectData(info, context);
			info.AddValue("VendorCode", ErrorCode);
			info.AddValue("ErrorPos", ErrorPos);
			info.AddValue("SqlState", SqlState);
		}

		/// <summary>
		/// Error code
		/// </summary>
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

		/// <summary>
		/// Error position
		/// </summary>
		public int ErrorPos
		{
			get
			{
				return iErrorPosition;
			}
		}

		/// <summary>
		/// SQL state
		/// </summary>
		public string SqlState
		{
			get
			{
				return strSqlState;
			}
		}

		/// <summary>
		/// Check whether connection is releasing
		/// </summary>
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
		/// <summary>
		/// Throw new <b>MaxDBException</b> generated by message text and SQLDBC error handler.
		/// </summary>
		/// <param name="message">The exception message text.</param>
		/// <param name="errorHandler">The SQLDBC error handler.</param>
		public static void ThrowException(string message, IntPtr errorHandler)
		{
			ThrowException(message, errorHandler, null);
		}

		/// <summary>
		/// Throw new <b>MaxDBException</b> generated by message text, SQLDBC error handler and inner exception.
		/// </summary>
		/// <param name="message">The exception message text.</param>
		/// <param name="errorHandler">The SQLDBC error handler.</param>
		/// <param name="innerException">The inner exception object.</param>
		[SecurityPermissionAttribute(SecurityAction.Demand, SerializationFormatter = true)]
		public static void ThrowException(string message, IntPtr errorHandler, Exception innerException)
		{
			throw new MaxDBException(message + ": " + Marshal.PtrToStringAnsi(UnsafeNativeMethods.SQLDBC_ErrorHndl_getErrorText(errorHandler)),
				Marshal.PtrToStringAnsi(UnsafeNativeMethods.SQLDBC_ErrorHndl_getSQLState(errorHandler)), UnsafeNativeMethods.SQLDBC_ErrorHndl_getErrorCode(errorHandler),
				innerException);
		}
#endif
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

		public override bool IsConnectionReleasing
		{
			get
			{
				return true;
			}
		}
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

#if SAFE
		public MaxDBValueOverflowException(string type, int colIndex)
			: base(MaxDBMessages.Extract(MaxDBError.VALUEOVERFLOW, type + " " + colIndex.ToString(CultureInfo.InvariantCulture)))
		{
		}
#endif // SAFE

		protected MaxDBValueOverflowException(SerializationInfo info, StreamingContext context)
			: base(info, context)
		{
		}
	}

	[Serializable]
	internal class StreamIOException : IOException
	{
		private DataException mSqlException;

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
