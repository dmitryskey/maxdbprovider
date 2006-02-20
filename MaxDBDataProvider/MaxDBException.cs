using System;
using System.Data;
using System.IO;
using MaxDBDataProvider.MaxDBProtocol;

namespace MaxDBDataProvider
{
	/// <summary>
	/// Summary description for MaxDBException.
	/// </summary>
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

	public class CommunicationException : DataException
	{
		public CommunicationException(int code) : base(CommError.ErrorText[code])
		{
		}
	}

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

		public virtual bool isConnectionReleasing
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
			? MessageTranslator.Translate(MessageKey.ERROR_DATABASEEXCEPTION, vendorCode.ToString(), errpos.ToString(), message)
			: MessageTranslator.Translate(MessageKey.ERROR_DATABASEEXCEPTION_WOERRPOS, vendorCode.ToString(), message),
			sqlState, vendorCode, errpos)
		{     
		}
	}

	public class ConnectionException : MaxDBSQLException 
	{
		public ConnectionException(MaxDBException ex) : base("[" + ex.DetailErrorCode + "] " + ex.Message, "08000", ex.DetailErrorCode, 0)
		{
		}
    
		public ConnectionException(MaxDBSQLException ex) : base(ex.Message, "08000", ex.VendorCode) 
		{
		}

		public override bool isConnectionReleasing
		{
			get
			{
				return true;
			}
		}
	}

	public class TimeoutException : DatabaseException
	{
		public TimeoutException() : base(MessageTranslator.Translate(MessageKey.ERROR_TIMEOUT), "08000", 700, 0)
		{
		}

		public override bool isConnectionReleasing
		{
			get
			{
				return true;
			}
		}
	}

	public class ObjectIsClosedException : DataException 
	{
		public ObjectIsClosedException() : base(MessageTranslator.Translate(MessageKey.ERROR_OBJECTISCLOSED)) 
		{
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
		public MaxDBValueOverflowException(string typeName, int colIndex) : base(MessageTranslator.Translate(MessageKey.ERROR_VALUEOVERFLOW, colIndex.ToString()))
		{
		}
	}

	public class StreamIOException : IOException 
	{
		private DataException sqlException;

		public StreamIOException(DataException sqlEx) : base()
		{
			this.sqlException = sqlEx;
		}

		public DataException SqlException 
		{
			get
			{
				return sqlException;
			}
		}
	}

	public class InvalidColumnException : DataException 
	{
		public InvalidColumnException(int columnIndex) :
			base(MessageTranslator.Translate(MessageKey.ERROR_INVALIDCOLUMNINDEX, columnIndex))
		{
		}

		public InvalidColumnException(string columnName) :
			base(MessageTranslator.Translate(MessageKey.ERROR_INVALIDCOLUMNNAME, columnName))
		{
		}
	}
}
