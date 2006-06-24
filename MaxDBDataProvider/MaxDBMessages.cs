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

namespace MaxDB.Data
{
	#region "Message translator class"

	public class MaxDBMessages
	{
		private static ResourceManager rm = new ResourceManager("MaxDBMessages", typeof(MaxDBMessages).Assembly);

		public const string 

			// Connection is not opened
			ERROR_CONNECTION_NOTOPENED = "error_connection_notopened",

			// Value overflow.
			ERROR_VALUEOVERFLOW = "error_valueoverflow",

			// Database exception (with error position).
			ERROR_DATABASEEXCEPTION = "error_databaseexception",

			// Database exception (without error position).
			ERROR_DATABASEEXCEPTION_WOERRPOS = "error_databaseexception_woerrpos",

			// Invalid column index.
			ERROR_INVALID_COLUMNINDEX = "error_invalid_columnindex",

			// Invalid column name.
			ERROR_INVALID_COLUMNNAME = "error_invalid_columnname",

			// Invalid column name buffer.
			ERROR_COLUMNNAME_BUFFER = "error_columnname_buffer",

			// An object is closed but shouldn't.
			ERROR_OBJECTISCLOSED = "error_objectisclosed",

			// A time out.
			ERROR_TIMEOUT = "error_timeout",

			// No longdata packet.
			ERROR_LONGDATAEXPECTED = "error_longdata_expected",

			// Invalid startposition for long data. 
			ERROR_INVALID_STARTPOSITION = "error_invalid_startposition",

			// SQL -> .NET type conversion.
			ERROR_CONVERSIONSQLNET = "error_conversion_sqlnet",

			// .NET -> SQL type conversion.
			ERROR_CONVERSIONNETSQL = "error_conversion_netsql",

			// Data -> any type conversion.
			ERROR_CONVERSIONDATA = "error_conversion_data",

			// VDN Number -> BigDecimal conversion.
			ERROR_CONVERSIONVDNnumber = "error_conversion_VDNnumber",

			// VDN Number -> Special Null value.
			ERROR_CONVERSIONSpecialNullValue = "error_conversion_SpecialNullValue",

			// Unsupported blob navigation.
			ERROR_MOVEBACKWARDINBLOB = "error_movebackwardinblob",

			// Try to read ASCII data from LONG column_
			ERROR_ASCIIREADFROMLONG = "error_asciireadfromlong",

			// Try to read binary data from LONG column.
			ERROR_BINARYREADFROMLONG = "error_binaryreadfromlong",

			// Try to put ASCII data into LONG column.
			ERROR_ASCIIPUTTOLONG = "error_asciiputtolong",

			// Try to put binary data into LONG column.
			ERROR_BINARYPUTTOLONG = "error_binaryputtolong",

			// Call of cancel occured.
			ERROR_STATEMENT_CANCELLED="error_statement_cancelled",

			// Try to execute null statement.
			ERROR_SQLSTATEMENT_NULL = "error_sqlstatement_null",

			// Column value is null.
			ERROR_COLUMNVALUE_NULL = "error_columnvalue_null",

			// Try to use null data adapter
			ERROR_ADAPTER_NULL = "error_adapter_null",

			// Try to use null select command
			ERROR_SELECT_NULL = "error_select_null",

			// Base table is not found
			ERROR_BASETABLE_NOTFOUND = "error_basetable_notfound",

			// Try to execute too long statement.
			ERROR_SQLSTATEMENT_TOOLONG = "error_sqlstatement_toolong",

			// IN or OUT param missing.
			ERROR_MISSINGINOUT = "error_missinginout",

            // Statement in batch generated result set.
            ERROR_BATCHRESULTSET="error_batchresultset",

            // Statement in batch generated result set.
            ERROR_BATCHRESULTSET_WITHNUMBER="error_batchresultset_withnumber",

            // Procedure call in batch contained OUT/INOUT.
            ERROR_BATCHPROCOUT="error_batchprocout",

            // Procedure call in batch contained OUT/INOUT.
            ERROR_BATCHMISSINGIN="error_batchmissingin",

			// A statement executed as update delivered a result set.
			ERROR_SQLSTATEMENT_RESULTSET = "error_sqlstatement_resultset",

			// SQL command doesn't return a result set.
			ERROR_SQLCOMMAND_NORESULTSET = "error_sqlcommand_noresultset",

			// A statement assumed to be a procedure call is not one.
			ERROR_SQLSTATEMENT_NOPROCEDURE = "error_sqlstatement_noprocedure",

			// Column index not found.
			ERROR_COLINDEX_NOTFOUND = "error_colindex_notfound",

			// Column name not found.
			ERROR_COLNAME_NOTFOUND = "error_colname_notfound",

			// User name missing.
			ERROR_NOUSER = "error_nouser",
 
			// Password missing.
			ERROR_NOPASSWORD = "error_nopassword",

			// Password invalid.
			ERROR_INVALIDPASSWORD = "error_invalidpassword",

			//No data found
			ERROR_NODATA_FOUND = "error_nodata_found",

			//Invalid data type
			ERROR_INVALID_DATATYPE = "error_invalid_datatype",

			//Unknown data type
			ERROR_UNKNOWN_DATATYPE = "error_unknown_datatype",

			// Try to get record at position < first.
			ERROR_RESULTSET_BEFOREFIRST = "error_resultset_beforefirst",

			// Try to get record at position > last.
			ERROR_RESULTSET_AFTERLAST = "error_resultset_afterlast",

			// Try to retrieve unnamed parameter by name.
			ERROR_UNNAMED_PARAMETER = "error_unnamed_parameter",

			// No column names delivered from kernel.
			ERROR_NO_COLUMNNAMES = "error_no_columnnames",

			// Connection field is null.
			ERROR_INTERNAL_CONNECTIONNULL = "error_internal_connectionnull",

			// Cant not set isolation level.
			ERROR_CONNECTION_ISOLATIONLEVEL = "error_connection_isolationlevel",

			// No more input expected at this place.
			ERROR_INTERNAL_UNEXPECTEDINPUT = "error_internal_unexpectedinput",

			// No more output expected at this place.
			ERROR_INTERNAL_UNEXPECTEDOUTPUT = "error_internal_unexpectedoutput",

			// Internal error: parse id is null.
			ERROR_INTERNAL_INVALIDPARSEID = "error_internal_invalidParseid",

			// getObject function failed
			ERROR_GETOBJECT_FAILED = "error_getobject_failed",

			//Fetch operation delivered no data part.
			ERROR_FETCH_NODATAPART = "error_fetch_nodatapart",

			//Fetch operation error_
			ERROR_FETCH_DATA = "error_fetch_data",

			// Runtime: connect to host failed.
			ERROR_HOST_CONNECT = "error_host_connect",

			// Runtime: execution failed.
			ERROR_EXEC_FAILED = "error_exec_failed",
    
			// Runtime: receive of connect failed.
			ERROR_RECV_CONNECT = "error_recv_connect",

			// Runtime: receive garbled reply
			ERROR_REPLY_GARBLED = "error_connectreply_garbled",

			// Runtime: reconnect on admin session unsupported
			ERROR_ADMIN_RECONNECT = "error_admin_reconnect",

			// Runtime: chunk overflow in read
			ERROR_CHUNKOVERFLOW = "error_chunkoverflow",

			// Reading from a stream resulted in an IOException
			ERROR_STREAM_IOEXCEPTION = "error_stream_ioexception",

			// Column nullable unknown
			ERROR_DBNULL_UNKNOWN = "error_dbnull_unknown",

			// Output parameter value truncated
			ERROR_PARAM_TRUNC = "error_param_trunc",

			// Unsupported database features
			ERROR_SPECIAL_NUMBER_UNSUPPORTED = "error_special_number_unsupported",
			ERROR_OMS_UNSUPPORTED = "error_oms_unsupported",
			ERROR_TABLEDIRECT_UNSUPPORTED = "error_tabledirect_unsupported",
	
			// Streams
			ERROR_CONVERSION_STRINGSTREAM  = "error_stream_conversion_string",
			ERROR_CONVERSION_BYTESTREAM = "error_stream_conversion_bytes",
			ERROR_STREAM_ISATEND = "error_stream_isatend",
			ERROR_CONVERSION_STRUCTURETYPE = "error_conversion_structuretype",	
			ERROR_STRUCTURE_ARRAYWRONGLENTGH = "error_structure_arraywronglength",
			ERROR_STRUCT_ELEMENT_NULL = "error_structure_element_null",
			ERROR_STRUCT_ELEMENT_CONVERSION = "error_structure_element_conversion",
			ERROR_STRUCT_ELEMENT_OVERFLOW = "error_structure_element_overflow",

			// Connection
			ERROR_CONNECTION_WRONGSERVERCHALLENGERECEIVED = "error_connection_wrongserverchallengereceived",
			ERROR_CONNECTION_CHALLENGERESPONSENOTSUPPORTED = "error_connection_challengeresponsenotsupported",
            ERROR_SSL_CERTIFICATE = "error_ssl_certificate",

			// communication errors
			COMMERROR_OK = "commerror_ok",
			COMMERROR_CONNECTDOWN = "commerror_connectiondown",
			COMMERROR_TASKLIMIT = "commerror_tasklimit",
			COMMERROR_TIMEOUT = "commerror_timeout",
			COMMERROR_CRASH = "commerror_crash",
			COMMERROR_RESTARTREQUIRED = "commerror_restartrequired",
			COMMERROR_SHUTDOWN = "commerror_shutdown",
			COMMERROR_SENDLINEDOWN = "commerror_sendlinedown",
			COMMERROR_RECVLINEDOWN = "commerror_recvlinedown",
			COMMERROR_PACKETLIMIT = "commerror_packetlimit",
			COMMERROR_RELEASED = "commerror_released",
			COMMERROR_WOULDBLOCK = "commerror_wouldblock",
			COMMERROR_UNKNOWNREQUEST = "commerror_unknownrequest",
			COMMERROR_SERVERDBUNKNOWN = "commerror_serverdbunknown",
			
			// the rest
			ERROR = "error",
			UNKNOWNTYPE = "unknowntype";


		public static string Extract(string key)
		{
			return Extract(key, null);
		}

		public static string Extract(string key, object o1)
		{
			return Extract(key, new object[]{ o1 });
		}

		public static string Extract(string key, object o1, object o2)
		{
			return Extract(key, new object[]{ o1, o2 });
		}

		public static string Extract(string key, object o1, object o2, object o3)
		{
			return Extract(key, new object[]{ o1, o2, o3 });
		}

		public static string Extract(string key, object[] args) 
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
