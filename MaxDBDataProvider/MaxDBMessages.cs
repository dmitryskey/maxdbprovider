//-----------------------------------------------------------------------------------------------
// <copyright file="MaxDBMessages.cs" company="2005-2019 Dmitry S. Kataev, 2002-2003 SAP AG">
// Copyright (c) 2005-2019 Dmitry S. Kataev, 2002-2003 SAP AG. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------------------------------
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

namespace MaxDB.Data
{
    using System.Collections.Generic;
    using System.Globalization;
    using System.Resources;
    using System.Text;

    #region "Message translator class"

    internal struct MaxDBError
    {
        public const string

// Connection is not opened
CONNECTION_NOTOPENED = "connection_notopened";
        public const string

// Value overflow.
VALUEOVERFLOW = "valueoverflow";
        public const string

// Database exception (with error position).
DATABASEEXCEPTION = "databaseexception";
        public const string

// Database exception (without error position).
DATABASEEXCEPTION_WOERRPOS = "databaseexception_woerrpos";
        public const string

// Invalid column index.
INVALID_COLUMNINDEX = "invalid_columnindex";
        public const string

// Invalid column name.
INVALID_COLUMNNAME = "invalid_columnname";
        public const string

// Invalid column name buffer.
COLUMNNAME_BUFFER = "columnname_buffer";
        public const string

// An object is closed but shouldn't.
OBJECTISCLOSED = "objectisclosed";
        public const string

// A time out.
TIMEOUT = "timeout";
        public const string

// No longdata packet.
LONGDATAEXPECTED = "longdata_expected";
        public const string

// Invalid startposition for long data.
INVALID_STARTPOSITION = "invalid_startposition";
        public const string

// SQL -> .NET type conversion.
CONVERSIONSQLNET = "conversion_sqlnet";
        public const string

// .NET -> SQL type conversion.
CONVERSIONNETSQL = "conversion_netsql";
        public const string

// Data -> any type conversion.
CONVERSIONDATA = "conversion_data";
        public const string

// VDN Number -> BigDecimal conversion.
CONVERSIONVDNnumber = "conversion_VDNnumber";
        public const string

// VDN Number -> Special Null value.
CONVERSIONSpecialNullValue = "conversion_SpecialNullValue";
        public const string

// Unsupported blob navigation.
MOVEBACKWARDINBLOB = "movebackwardinblob";
        public const string

// Try to read ASCII data from LONG column_
ASCIIREADFROMLONG = "asciireadfromlong";
        public const string

// Try to read binary data from LONG column.
BINARYREADFROMLONG = "binaryreadfromlong";
        public const string

// Try to put ASCII data into LONG column.
ASCIIPUTTOLONG = "asciiputtolong";
        public const string

// Try to put binary data into LONG column.
BINARYPUTTOLONG = "binaryputtolong";
        public const string

// Call of cancel occured.
STATEMENT_CANCELLED = "statement_cancelled";
        public const string

// Try to execute null statement.
SQLSTATEMENT_NULL = "sqlstatement_null";
        public const string

// Column value is null.
COLUMNVALUE_NULL = "columnvalue_null";
        public const string

// Try to use null data adapter
ADAPTER_NULL = "adapter_null";
        public const string

// Try to use null select command
SELECT_NULL = "select_null";
        public const string

// Base table is not found
BASETABLE_NOTFOUND = "basetable_notfound";
        public const string

// Try to execute too long statement.
SQLSTATEMENT_TOOLONG = "sqlstatement_toolong";
        public const string

// IN or OUT param missing.
MISSINGINOUT = "missinginout";
        public const string

// Statement in batch generated result set.
BATCHRESULTSET = "batchresultset";
        public const string

// Statement in batch generated result set.
BATCHRESULTSET_WITHNUMBER = "batchresultset_withnumber";
        public const string

// Procedure call in batch contained OUT/INOUT.
BATCHPROCOUT = "batchprocout";
        public const string

// Procedure call in batch contained OUT/INOUT.
BATCHMISSINGIN = "batchmissingin";
        public const string

// A statement executed as update delivered a result set.
SQLSTATEMENT_RESULTSET = "sqlstatement_resultset";
        public const string

// SQL command doesn't return a result set.
SQLCOMMAND_NORESULTSET = "sqlcommand_noresultset";
        public const string

// A statement assumed to be a procedure call is not one.
SQLSTATEMENT_NOPROCEDURE = "sqlstatement_noprocedure";
        public const string

// Column index not found.
COLINDEX_NOTFOUND = "colindex_notfound";
        public const string

// Column name not found.
COLNAME_NOTFOUND = "colname_notfound";
        public const string

// User name missing.
NOUSER = "nouser";
        public const string

// Password missing.
NOPASSWORD = "nopassword";
        public const string

// Password invalid.
INVALIDPASSWORD = "invalidpassword";
        public const string

// No data found
NODATA_FOUND = "nodata_found";
        public const string

// Invalid data type
INVALID_DATATYPE = "invalid_datatype";
        public const string

// Unknown data type
UNKNOWN_DATATYPE = "unknown_datatype";
        public const string

// Try to get record at position < first.
RESULTSET_BEFOREFIRST = "resultset_beforefirst";
        public const string

// Try to get record at position > last.
RESULTSET_AFTERLAST = "resultset_afterlast";
        public const string

// Try to retrieve unnamed parameter by name.
UNNAMED_PARAMETER = "unnamed_parameter";
        public const string

// No column names delivered from kernel.
NO_COLUMNNAMES = "no_columnnames";
        public const string

// Connection field is null.
INTERNAL_CONNECTIONNULL = "internal_connectionnull";
        public const string

// Cant not set isolation level.
CONNECTION_ISOLATIONLEVEL = "connection_isolationlevel";
        public const string

// No more input expected at this place.
INTERNAL_UNEXPECTEDINPUT = "internal_unexpectedinput";
        public const string

// No more output expected at this place.
INTERNAL_UNEXPECTEDOUTPUT = "internal_unexpectedoutput";
        public const string

// Internal error: parse id is null.
INTERNAL_INVALIDPARSEID = "internal_invalidParseid";
        public const string

// getObject function failed
GETOBJECT_FAILED = "getobject_failed";
        public const string

// Fetch operation delivered no data part.
FETCH_NODATAPART = "fetch_nodatapart";
        public const string

// Fetch operation
FETCH_DATA_FAILED = "fetch_data_failed";
        public const string

// Runtime: connect to host failed.
HOST_CONNECT_FAILED = "host_connect_failed";
        public const string

// Runtime: execution failed.
EXEC_FAILED = "exec_failed";
        public const string

// Runtime: receive of connect failed.
RECV_CONNECT = "recv_connect";
        public const string

// Runtime: receive garbled reply
REPLY_GARBLED = "connectreply_garbled";
        public const string

// Runtime: reconnect on admin session unsupported
ADMIN_RECONNECT = "admin_reconnect";
        public const string

// Runtime: chunk overflow in read
CHUNKOVERFLOW = "chunkoverflow";
        public const string

// Reading from a stream resulted in an IOException
STREAM_IOEXCEPTION = "stream_ioexception";
        public const string

// Column nullable unknown
DBNULL_UNKNOWN = "dbnull_unknown";
        public const string

// Output parameter value truncated
PARAMETER_TRUNC = "parameter_truncated";
        public const string

// Parameter is null
PARAMETER_NULL = "parameter_null";
        public const string

// Index is out if range
INDEX_OUTOFRANGE = "index_outofrange";
        public const string

// Unsupported database features
SPECIAL_NUMBER_UNSUPPORTED = "special_number_unsupported";
        public const string
OMS_UNSUPPORTED = "oms_unsupported";
        public const string
TABLEDIRECT_UNSUPPORTED = "tabledirect_unsupported";
        public const string

// Streams
CONVERSION_STRINGSTREAM = "streamconversion_string";
        public const string
CONVERSION_BYTESTREAM = "streamconversion_bytes";
        public const string
STREAM_ISATEND = "stream_isatend";
        public const string
CONVERSION_STRUCTURETYPE = "conversion_structuretype";
        public const string
STRUCTURE_ARRAYWRONGLENTGH = "structure_arraywronglength";
        public const string
STRUCT_ELEMENT_NULL = "structure_element_null";
        public const string
STRUCT_ELEMENT_CONVERSION = "structure_element_conversion";
        public const string
STRUCT_ELEMENT_OVERFLOW = "structure_element_overflow";
        public const string

// Connection
CONNECTION_WRONGSERVERCHALLENGERECEIVED = "connection_wrongserverchallengereceived";
        public const string
CONNECTION_CHALLENGERESPONSENOTSUPPORTED = "connection_challengeresponsenotsupported";
        public const string
SSL_CERTIFICATE = "ssl_certificate";
        public const string

// communication errors
COMMOK = "commok";
        public const string
COMMCONNECTDOWN = "commconnectiondown";
        public const string
COMMTASKLIMIT = "commtasklimit";
        public const string
COMMTIMEOUT = "commtimeout";
        public const string
COMMCRASH = "commcrash";
        public const string
COMMRESTARTREQUIRED = "commrestartrequired";
        public const string
COMMSHUTDOWN = "commshutdown";
        public const string
COMMSENDLINEDOWN = "commsendlinedown";
        public const string
COMMRECVLINEDOWN = "commrecvlinedown";
        public const string
COMMPACKETLIMIT = "commpacketlimit";
        public const string
COMMRELEASED = "commreleased";
        public const string
COMMWOULDBLOCK = "commwouldblock";
        public const string
COMMUNKNOWNREQUEST = "communknownrequest";
        public const string
COMMSERVERDBUNKNOWN = "commserverdbunknown";
        public const string

// big integer messages
BIGINT_OVERFLOW = "bigint_overflow";
        public const string
BIGINT_UNDERFLOW = "bigint_underflow";
        public const string
BIGINT_RADIX_OVERFLOW = "bigint_radix_overflow";
        public const string

HASH_CHANGE_KEY = "hash_change_key";
        public const string
POOL_NOT_FOUND = "pool_not_found";
        public const string

// the rest
ERROR = "error";
        public const string
UNKNOWNTYPE = "unknowntype";
    }

    internal class MaxDBMessages
    {
        private static ResourceManager rm = new ResourceManager("MaxDBMessages", typeof(MaxDBMessages).Assembly);

        public static string Extract(string key, object o1 = null, object o2 = null, object o3 = null)
        {
            var args = new List<object>();

            if (o1 != null)
            {
                args.Add(o1);

                if (o2 != null)
                {
                    args.Add(o2);

                    if (o3 != null)
                    {
                        args.Add(o3);
                    }
                }
            }

            return Extract(key, args.ToArray());
        }

        public static string Extract(string key, object[] args)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(key) || key == "0")
                {
                    return null;
                }

                // retrieve text and format it
                string msg = rm.GetString(key);
                return args != null ? string.Format(CultureInfo.InvariantCulture, msg, args) : msg;
            }
            catch (MissingManifestResourceException)
            {
                var result = new StringBuilder("No message available for key ");
                result.Append(key);

                // if arguments given append them
                if (args == null || args.Length == 0)
                {
                    result.Append(".");
                }
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
