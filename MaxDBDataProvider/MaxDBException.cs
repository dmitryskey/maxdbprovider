using System;

namespace MaxDBDataProvider
{
	/// <summary>
	/// Summary description for MaxDBException.
	/// </summary>
	public class MaxDBException : SystemException
	{
		public MaxDBException() : base()
		{
		}

		public MaxDBException(string message) : base(message)
		{
		}

		public MaxDBException(string message, Exception innerException) : base(message, innerException)
		{
		}
	}
}
