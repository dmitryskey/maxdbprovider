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
using System.Resources;
using System.Text;
using System.Globalization;

namespace MaxDB.Data
{
	#region "Message translator class"

	internal struct MaxDBError
	{
		public const string

			// Connection is not opened
			CONNECTION_NOTOPENED = "connection_notopened",

			// Value overflow.
			VALUEOVERFLOW = "valueoverflow",

			// Database exception (with error position).
			DATABASEEXCEPTION = "databaseexception",

			// Database exception (without error position).
			DATABASEEXCEPTION_WOERRPOS = "databaseexception_woerrpos",

			// Invalid column index.
			INVALID_COLUMNINDEX = "invalid_columnindex",

			// Invalid column name.
			INVALID_COLUMNNAME = "invalid_columnname",

			// Invalid column name buffer.
			COLUMNNAME_BUFFER = "columnname_buffer",

			// An object is closed but shouldn't.
			OBJECTISCLOSED = "objectisclosed",

			// A time out.
			TIMEOUT = "timeout",

			// No longdata packet.
			LONGDATAEXPECTED = "longdata_expected",

			// Invalid startposition for long data. 
			INVALID_STARTPOSITION = "invalid_startposition",

			// SQL -> .NET type conversion.
			CONVERSIONSQLNET = "conversion_sqlnet",

			// .NET -> SQL type conversion.
			CONVERSIONNETSQL = "conversion_netsql",

			// Data -> any type conversion.
			CONVERSIONDATA = "conversion_data",

			// VDN Number -> BigDecimal conversion.
			CONVERSIONVDNnumber = "conversion_VDNnumber",

			// VDN Number -> Special Null value.
			CONVERSIONSpecialNullValue = "conversion_SpecialNullValue",

			// Unsupported blob navigation.
			MOVEBACKWARDINBLOB = "movebackwardinblob",

			// Try to read ASCII data from LONG column_
			ASCIIREADFROMLONG = "asciireadfromlong",

			// Try to read binary data from LONG column.
			BINARYREADFROMLONG = "binaryreadfromlong",

			// Try to put ASCII data into LONG column.
			ASCIIPUTTOLONG = "asciiputtolong",

			// Try to put binary data into LONG column.
			BINARYPUTTOLONG = "binaryputtolong",

			// Call of cancel occured.
			STATEMENT_CANCELLED = "statement_cancelled",

			// Try to execute null statement.
			SQLSTATEMENT_NULL = "sqlstatement_null",

			// Column value is null.
			COLUMNVALUE_NULL = "columnvalue_null",

			// Try to use null data adapter
			ADAPTER_NULL = "adapter_null",

			// Try to use null select command
			SELECT_NULL = "select_null",

			// Base table is not found
			BASETABLE_NOTFOUND = "basetable_notfound",

			// Try to execute too long statement.
			SQLSTATEMENT_TOOLONG = "sqlstatement_toolong",

			// IN or OUT param missing.
			MISSINGINOUT = "missinginout",

			// Statement in batch generated result set.
			BATCHRESULTSET = "batchresultset",

			// Statement in batch generated result set.
			BATCHRESULTSET_WITHNUMBER = "batchresultset_withnumber",

			// Procedure call in batch contained OUT/INOUT.
			BATCHPROCOUT = "batchprocout",

			// Procedure call in batch contained OUT/INOUT.
			BATCHMISSINGIN = "batchmissingin",

			// A statement executed as update delivered a result set.
			SQLSTATEMENT_RESULTSET = "sqlstatement_resultset",

			// SQL command doesn't return a result set.
			SQLCOMMAND_NORESULTSET = "sqlcommand_noresultset",

			// A statement assumed to be a procedure call is not one.
			SQLSTATEMENT_NOPROCEDURE = "sqlstatement_noprocedure",

			// Column index not found.
			COLINDEX_NOTFOUND = "colindex_notfound",

			// Column name not found.
			COLNAME_NOTFOUND = "colname_notfound",

			// User name missing.
			NOUSER = "nouser",

			// Password missing.
			NOPASSWORD = "nopassword",

			// Password invalid.
			INVALIDPASSWORD = "invalidpassword",

			//No data found
			NODATA_FOUND = "nodata_found",

			//Invalid data type
			INVALID_DATATYPE = "invalid_datatype",

			//Unknown data type
			UNKNOWN_DATATYPE = "unknown_datatype",

			// Try to get record at position < first.
			RESULTSET_BEFOREFIRST = "resultset_beforefirst",

			// Try to get record at position > last.
			RESULTSET_AFTERLAST = "resultset_afterlast",

			// Try to retrieve unnamed parameter by name.
			UNNAMED_PARAMETER = "unnamed_parameter",

			// No column names delivered from kernel.
			NO_COLUMNNAMES = "no_columnnames",

			// Connection field is null.
			INTERNAL_CONNECTIONNULL = "internal_connectionnull",

			// Cant not set isolation level.
			CONNECTION_ISOLATIONLEVEL = "connection_isolationlevel",

			// No more input expected at this place.
			INTERNAL_UNEXPECTEDINPUT = "internal_unexpectedinput",

			// No more output expected at this place.
			INTERNAL_UNEXPECTEDOUTPUT = "internal_unexpectedoutput",

			// Internal error: parse id is null.
			INTERNAL_INVALIDPARSEID = "internal_invalidParseid",

			// getObject function failed
			GETOBJECT_FAILED = "getobject_failed",

			//Fetch operation delivered no data part.
			FETCH_NODATAPART = "fetch_nodatapart",

			//Fetch operation 
			FETCH_DATA_FAILED = "fetch_data_failed",

			// Runtime: connect to host failed.
			HOST_CONNECT_FAILED = "host_connect_failed",

			// Runtime: execution failed.
			EXEC_FAILED = "exec_failed",

			// Runtime: receive of connect failed.
			RECV_CONNECT = "recv_connect",

			// Runtime: receive garbled reply
			REPLY_GARBLED = "connectreply_garbled",

			// Runtime: reconnect on admin session unsupported
			ADMIN_RECONNECT = "admin_reconnect",

			// Runtime: chunk overflow in read
			CHUNKOVERFLOW = "chunkoverflow",

			// Reading from a stream resulted in an IOException
			STREAM_IOEXCEPTION = "stream_ioexception",

			// Column nullable unknown
			DBNULL_UNKNOWN = "dbnull_unknown",

			// Output parameter value truncated
			PARAMETER_TRUNC = "parameter_truncated",

			// Parameter is null
			PARAMETER_NULL = "parameter_null",

			// Index is out if range
			INDEX_OUTOFRANGE = "index_outofrange",

			// Unsupported database features
			SPECIAL_NUMBER_UNSUPPORTED = "special_number_unsupported",
			OMS_UNSUPPORTED = "oms_unsupported",
			TABLEDIRECT_UNSUPPORTED = "tabledirect_unsupported",

			// Streams
			CONVERSION_STRINGSTREAM = "streamconversion_string",
			CONVERSION_BYTESTREAM = "streamconversion_bytes",
			STREAM_ISATEND = "stream_isatend",
			CONVERSION_STRUCTURETYPE = "conversion_structuretype",
			STRUCTURE_ARRAYWRONGLENTGH = "structure_arraywronglength",
			STRUCT_ELEMENT_NULL = "structure_element_null",
			STRUCT_ELEMENT_CONVERSION = "structure_element_conversion",
			STRUCT_ELEMENT_OVERFLOW = "structure_element_overflow",

			// Connection
			CONNECTION_WRONGSERVERCHALLENGERECEIVED = "connection_wrongserverchallengereceived",
			CONNECTION_CHALLENGERESPONSENOTSUPPORTED = "connection_challengeresponsenotsupported",
			SSL_CERTIFICATE = "ssl_certificate",

			// communication errors
			COMMOK = "commok",
			COMMCONNECTDOWN = "commconnectiondown",
			COMMTASKLIMIT = "commtasklimit",
			COMMTIMEOUT = "commtimeout",
			COMMCRASH = "commcrash",
			COMMRESTARTREQUIRED = "commrestartrequired",
			COMMSHUTDOWN = "commshutdown",
			COMMSENDLINEDOWN = "commsendlinedown",
			COMMRECVLINEDOWN = "commrecvlinedown",
			COMMPACKETLIMIT = "commpacketlimit",
			COMMRELEASED = "commreleased",
			COMMWOULDBLOCK = "commwouldblock",
			COMMUNKNOWNREQUEST = "communknownrequest",
			COMMSERVERDBUNKNOWN = "commserverdbunknown",

			// big integer messages
			BIGINT_OVERFLOW = "bigint_overflow",
			BIGINT_UNDERFLOW = "bigint_underflow",
			BIGINT_RADIX_OVERFLOW = "bigint_radix_overflow",

			HASH_CHANGE_KEY = "hash_change_key",
			POOL_NOT_FOUND = "pool_not_found",

			// the rest
			ERROR = "error",
			UNKNOWNTYPE = "unknowntype";
	}

	internal class MaxDBMessages
	{
		private static ResourceManager rm = new ResourceManager("MaxDBMessages", typeof(MaxDBMessages).Assembly);

		private MaxDBMessages()
		{
		}

		public static string Extract(string key)
		{
			return Extract(key, null);
		}

		public static string Extract(string key, object o1)
		{
			return Extract(key, new object[] { o1 });
		}

		public static string Extract(string key, object o1, object o2)
		{
			return Extract(key, new object[] { o1, o2 });
		}

		public static string Extract(string key, object o1, object o2, object o3)
		{
			return Extract(key, new object[] { o1, o2, o3 });
		}

		public static string Extract(string key, object[] args)
		{
			try
			{
				// retrieve text and format it
				string msg = rm.GetString(key);
				if (args != null)
					return string.Format(CultureInfo.InvariantCulture, msg, args);
				else
					return msg;
			}
			catch (MissingManifestResourceException)
			{
				StringBuilder result = new StringBuilder("No message available for key ");
				result.Append(key);
				// if arguments given append them
				if (args == null || args.Length == 0)
					result.Append(".");
				else
				{
					result.Append(", arguments [");
					for (int i = 0; i < args.Length - 1; i++)
					{
						result.Append(args[i].ToString());
						result.Append(", ");
					}
					result.Append(args[args.Length - 1].ToString());
					result.Append("].");
				}

				return result.ToString();
			}
		}
	}

	#endregion
}
