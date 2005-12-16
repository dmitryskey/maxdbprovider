using System;

namespace MaxDBDataProvider.MaxDBProtocol
{
	/// <summary>
	/// Summary description for MaxDBParseInfo.
	/// </summary>
	public class MaxDBParseInfo
	{
		public string sqlCmd;
		public bool cached; // flag is set to true if command is in parseinfo cache 
		public int functionCode;

		public MaxDBParseInfo()
		{
			//
			// TODO: Add constructor logic here
			//
		}
	}
}
