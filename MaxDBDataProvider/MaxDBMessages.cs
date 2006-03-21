using System;
using System.Resources;
using System.Text;

namespace MaxDBDataProvider
{
	#region "Message keys"

	public struct MessageKey
	{
		public const string 

			// Connection is not opened
			ERROR_CONNECTIONNOTOPENED = "error.connection.notopened",

			// Value overflow.
			ERROR_VALUEOVERFLOW = "error.valueoverflow",

			// Database exception (with error position).
			ERROR_DATABASEEXCEPTION = "error.databaseexception",

			// Database exception (without error position).
			ERROR_DATABASEEXCEPTION_WOERRPOS = "error.databaseexception.woerrpos",

			// Invalid column index.
			ERROR_INVALIDCOLUMNINDEX = "error.invalidcolumnindex",

			// Invalid column name.
			ERROR_INVALIDCOLUMNNAME = "error.invalidcolumnname",

			// An object is closed but shouldn't.
			ERROR_OBJECTISCLOSED = "error.objectisclosed",

			// A time out.
			ERROR_TIMEOUT = "error.timeout",

			// No longdata packet.
			ERROR_LONGDATAEXPECTED = "error.longdata.expected",

			// Invalid startposition for long data. 
			ERROR_INVALID_STARTPOSITION = "error.invalid.startposition",

			// SQL -> .NET type conversion.
			ERROR_CONVERSIONSQLNET = "error.conversion.sqlnet",

			// .NET -> SQL type conversion.
			ERROR_CONVERSIONNETSQL = "error.conversion.netsql",

			// Data -> any type conversion.
			ERROR_CONVERSIONDATA = "error.conversion.data",

			// VDN Number -> BigDecimal conversion.
			ERROR_CONVERSIONVDNnumber = "error.conversion.VDNnumber",

			// VDN Number -> Special Null value.
			ERROR_CONVERSIONSpecialNullValue = "error.conversion.SpecialNullValue",

			// Unsupported blob navigation.
			ERROR_MOVEBACKWARDINBLOB = "error.movebackwardinblob",

			// Try to read ASCII data from LONG column.
			ERROR_ASCIIREADFROMLONG = "error.asciireadfromlong",

			// Try to read binary data from LONG column.
			ERROR_BINARYREADFROMLONG = "error.binaryreadfromlong",

			// Try to put ASCII data into LONG column.
			ERROR_ASCIIPUTTOLONG = "error.asciiputtolong",

			// Try to put binary data into LONG column.
			ERROR_BINARYPUTTOLONG = "error.binaryputtolong",

			// Call of cancel occured.
			ERROR_STATEMENT_CANCELLED="error.statement.cancelled",

			// Try to execute null statement.
			ERROR_SQLSTATEMENT_NULL = "error.sqlstatement.null",

			// Try to execute too long statement.
			ERROR_SQLSTATEMENT_TOOLONG = "error.sqlstatement.toolong",

			// IN or OUT param missing.
			ERROR_MISSINGINOUT = "error.missinginout",

			// A statement executed as update delivered a result set.
			ERROR_SQLSTATEMENT_RESULTSET = "error.sqlstatement.resultset",

			// A statement assumed to be a procedure call is not one.
			ERROR_SQLSTATEMENT_NOPROCEDURE = "error.sqlstatement.noprocedure",

			// Column index not found.
			ERROR_COLINDEX_NOTFOUND = "error.colindex.notfound",

			// User name missing.
			ERROR_NOUSER = "error.nouser",
 
			// Password missing.
			ERROR_NOPASSWORD = "error.nopassword",

			// Password invalid.
			ERROR_INVALIDPASSWORD = "error.invalidpassword",

			// Try to get record at position < first.
			ERROR_RESULTSET_BEFOREFIRST = "error.resultset.beforefirst",

			// Try to get record at position > last.
			ERROR_RESULTSET_AFTERLAST = "error.resultset.afterlast",

			// Try to retrieve unnamed parameter by name.
			ERROR_UNNAMED_PARAMETER = "error.unnamed.parameter",

			// No column names delivered from kernel.
			ERROR_NO_COLUMNNAMES = "error.no.columnnames",

			// Connection field is null.
			ERROR_INTERNAL_CONNECTIONNULL = "error.internal.connectionnull",

			// No more input expected at this place.
			ERROR_INTERNAL_UNEXPECTEDINPUT = "error.internal.unexpectedinput",

			// No more output expected at this place.
			ERROR_INTERNAL_UNEXPECTEDOUTPUT = "error.internal.unexpectedoutput",

			// Internal error: parse id is null.
			ERROR_INTERNAL_INVALIDPARSEID = "error.internal.invalidParseid",

			// getObject function failed
			ERROR_GETOBJECT_FAILED = "error.getobject.failed",

			// Runtime: connect to host failed.
			ERROR_HOST_CONNECT = "error.host.connect",

			// Runtime: execution failed.
			ERROR_EXEC_FAILED = "error.exec.failed",
    
			// Runtime: receive of connect failed.
			ERROR_RECV_CONNECT = "error.recv.connect",

			// Runtime: receive garbled reply
			ERROR_REPLY_GARBLED = "error.connectreply.garbled",

			// Runtime: reconnect on admin session unsupported
			ERROR_ADMIN_RECONNECT = "error.admin.reconnect",

			// Runtime: chunk overflow in read
			ERROR_CHUNKOVERFLOW = "error.chunkoverflow",

			// Reading from a stream resulted in an IOException
			ERROR_STREAM_IOEXCEPTION = "error.stream.ioexception",

			// Column nullable unknown
			ERROR_DBNULL_UNKNOWN = "error.dbnull.unknown",

			// Unsupported database features
			ERROR_SPECIAL_NUMBER_UNSUPPORTED = "error.special.number.unsupported",
			ERROR_OMS_UNSUPPORTED = "error.oms.unsupported",
			ERROR_TABLEDIRECT_UNSUPPORTED = "error.tabledirect.unsupported",
	
			// Streams
			ERROR_CONVERSION_STRINGSTREAM  = "error.stream.conversion.string",
			ERROR_CONVERSION_BYTESTREAM = "error.stream.conversion.bytes",
			ERROR_STREAM_ISATEND = "error.stream.isatend",
			ERROR_CONVERSION_STRUCTURETYPE = "error.conversion.structuretype",	
			ERROR_STRUCTURE_ARRAYWRONGLENTGH = "error.structure.arraywronglength",
			ERROR_STRUCT_ELEMENT_NULL = "error.structure.element.null",
			ERROR_STRUCT_ELEMENT_CONVERSION = "error.structure.element.conversion",
			ERROR_STRUCT_ELEMENT_OVERFLOW = "error.structure.element.overflow",

			// Connection
			ERROR_CONNECTION_WRONGSERVERCHALLENGERECEIVED = "error.connection.wrongserverchallengereceived",
			ERROR_CONNECTION_CHALLENGERESPONSENOTSUPPORTED = "error.connection.challengeresponsenotsupported",

			// communication errors
			COMMERROR_OK = "commerror.ok",
			COMMERROR_CONNECTDOWN = "commerror.connectiondown",
			COMMERROR_TASKLIMIT = "commerror.tasklimit",
			COMMERROR_TIMEOUT = "commerror.timeout",
			COMMERROR_CRASH = "commerror.crash",
			COMMERROR_RESTARTREQUIRED = "commerror.restartrequired",
			COMMERROR_SHUTDOWN = "commerror.shutdown",
			COMMERROR_SENDLINEDOWN = "commerror.sendlinedown",
			COMMERROR_RECVLINEDOWN = "commerror.recvlinedown",
			COMMERROR_PACKETLIMIT = "commerror.packetlimit",
			COMMERROR_RELEASED = "commerror.released",
			COMMERROR_WOULDBLOCK = "commerror.wouldblock",
			COMMERROR_UNKNOWNREQUEST = "commerror.unknownrequest",
			COMMERROR_SERVERDBUNKNOWN = "commerror.serverdbunknown",
			
			// the rest
			ERROR = "error",
			UNKNOWNTYPE = "unknowntype";
	}

	#endregion

	#region "Message translator class"

	public class MessageTranslator
	{
		private static ResourceManager rm = new ResourceManager("MaxDBMessages", typeof(MessageTranslator).Assembly);

		public static string Translate(string key)
		{
			return Translate(key, null);
		}

		public static string Translate(string key, object o1)
		{
			return Translate(key, new object[]{ o1 });
		}

		public static string Translate(string key, object o1, object o2)
		{
			return Translate(key, new object[]{ o1, o2 });
		}

		public static string Translate(string key, object o1, object o2, object o3)
		{
			return Translate(key, new object[]{ o1, o2, o3 });
		}

		public static string Translate(string key, object[] args) 
		{
			try 
			{
				// retrieve text and format it
				string msg = rm.GetString(key);
				if (args != null)
					return string.Format(msg, args);
				else
					return msg;
			} 
			catch(MissingManifestResourceException) 
			{
				StringBuilder result = new StringBuilder("No message available for key ");
				result.Append(key);
				// if arguments given append them
				if(args == null || args.Length==0) 
					result.Append(".");
				else 
				{
					result.Append(", arguments [");
					for(int i=0; i< args.Length - 1; i++) 
					{
						result.Append(args[i].ToString());
						result.Append(", ");
					}
					result.Append(args[args.Length-1].ToString());
					result.Append("].");
				}

				return result.ToString();
			} 
		}
	}

	#endregion
}
