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
using MaxDBDataProvider.MaxDBProtocol;

namespace MaxDBDataProvider
{
	/// <summary>
	/// Summary description for MaxDBException.
	/// </summary>
	[Serializable]
	public class MaxDBException : DataException
	{
		private int m_detailErrorCode = -708;

		public MaxDBException() : base()
		{
		}

		public MaxDBException(string message) : base(message)
		{
		}

		public MaxDBException(string message, Exception innerException) : base(message, innerException)
		{
		}

		public MaxDBException(string message, int rc, Exception innerException) : base(message, innerException)
		{
			m_detailErrorCode = rc;
		}

		public int DetailErrorCode
		{
			get
			{
				return m_detailErrorCode;
			}
		}
	}

	public class PartNotFound : Exception 
	{
		public PartNotFound() : base() 
		{
		}
	}

	[Serializable]
	public class MaxDBCommunicationException : DataException
	{
		public MaxDBCommunicationException(int code) : base(CommError.ErrorText[code])
		{
		}
	}

	[Serializable]
	public class MaxDBSQLException : DataException
	{
		private int m_errPos = -10899;
		private string m_sqlState;
		private int m_vendorCode;

		public MaxDBSQLException() : base()
		{
		}

		public MaxDBSQLException(string message) : base(message)
		{
		}

		public MaxDBSQLException(string message, string sqlState) : base(message)
		{
			m_sqlState = sqlState;
		}

		public MaxDBSQLException(string message, string sqlState, int vendorCode) : base(message)
		{
			m_sqlState = sqlState;
			m_vendorCode = vendorCode;
		}

		public MaxDBSQLException(string message, string sqlState, int vendorCode, int errpos) : base(message)
		{
			m_sqlState = sqlState;
			m_vendorCode = vendorCode;
			m_errPos = errpos;
		}

		public int VendorCode
		{
			get
			{
				return m_vendorCode;
			}
		}

		public int ErrorPos
		{
			get
			{
				return m_errPos;
			}
		}

		public string SQLState
		{
			get
			{
				return m_sqlState;
			}
		}

		public virtual bool IsConnectionReleasing
		{
			get
			{
				switch(m_vendorCode) 
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

	public class DatabaseException : MaxDBSQLException 
	{
		public DatabaseException(string message, string sqlState, int vendorCode, int errpos) : base((errpos > 1) 
			? MaxDBMessages.Extract(MaxDBMessages.ERROR_DATABASEEXCEPTION, vendorCode.ToString(), errpos.ToString(), message)
			: MaxDBMessages.Extract(MaxDBMessages.ERROR_DATABASEEXCEPTION_WOERRPOS, vendorCode.ToString(), message),
			sqlState, vendorCode, errpos)
		{     
		}
	}

	[Serializable]
	public class MaxDBConnectionException : MaxDBSQLException 
	{
		public MaxDBConnectionException(MaxDBException ex) : base("[" + ex.DetailErrorCode + "] " + ex.Message, "08000", ex.DetailErrorCode, 0)
		{
		}
    
		public MaxDBConnectionException(MaxDBSQLException ex) : base(ex.Message, "08000", ex.VendorCode) 
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
		public MaxDBTimeoutException() : base(MaxDBMessages.Extract(MaxDBMessages.ERROR_TIMEOUT), "08000", 700, 0)
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

	public class MaxDBConversionException : MaxDBSQLException
	{
		public MaxDBConversionException(string msg) : base(msg)
		{
		}
	}

	public class MaxDBValueOverflowException : MaxDBException
	{
		public MaxDBValueOverflowException(string typeName, int colIndex) : base(MaxDBMessages.Extract(MaxDBMessages.ERROR_VALUEOVERFLOW, colIndex.ToString()))
		{
		}
	}

	public class StreamIOException : IOException 
	{
		private DataException m_sqlException;

		public StreamIOException(DataException sqlEx) : base()
		{
			m_sqlException = sqlEx;
		}

		public DataException SqlException 
		{
			get
			{
				return m_sqlException;
			}
		}
	}

	public class InvalidColumnException : DataException 
	{
		public InvalidColumnException(int columnIndex) :
			base(MaxDBMessages.Extract(MaxDBMessages.ERROR_INVALID_COLUMNINDEX, columnIndex))
		{
		}

		public InvalidColumnException(string columnName) :
			base(MaxDBMessages.Extract(MaxDBMessages.ERROR_INVALID_COLUMNNAME, columnName))
		{
		}
	}
}
