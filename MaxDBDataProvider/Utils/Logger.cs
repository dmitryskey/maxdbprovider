using System;
using System.Text;

namespace MaxDBDataProvider
{
	/// <summary>
	/// Summary description for Logger.
	/// </summary>
	public class Logger
	{
		public static string ToHexString(byte[] array)
		{
			if (array != null)
			{
				StringBuilder result = new StringBuilder(array.Length * 2);
				foreach(byte val in array)
					result.Append(val.ToString("X2"));

				return result.ToString();
			}
			else
				return "NULL";
		}
	}
}
