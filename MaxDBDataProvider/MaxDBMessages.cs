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
CONNECTIONNOTOPENED = "connection_notopened";

        public const string

// Value overflow.
VALUEOVERFLOW = "valueoverflow";

        public const string

// Database exception (with error position).
DATABASEEXCEPTION = "databaseexception";

        public const string

// Database exception (without error position).
DATABASEEXCEPTIONWOERRPOS = "databaseexception_woerrpos";

        public const string

// Invalid column index.
INVALIDCOLUMNINDEX = "invalid_columnindex";

        public const string

// Invalid column name.
INVALIDCOLUMNNAME = "invalid_columnname";

        public const string

// Invalid column name buffer.
COLUMNNAMEBUFFER = "columnname_buffer";

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
INVALIDSTARTPOSITION = "invalid_startposition";

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
STATEMENTCANCELLED = "statement_cancelled";

        public const string

// Try to execute null statement.
SQLSTATEMENTNULL = "sqlstatement_null";

        public const string

// Column value is null.
COLUMNVALUENULL = "columnvalue_null";

        public const string

// Try to use null data adapter
ADAPTERNULL = "adapter_null";

        public const string

// Try to use null select command
SELECTNULL = "select_null";

        public const string

// Base table is not found
BASETABLENOTFOUND = "basetable_notfound";

        public const string

// Try to execute too long statement.
SQLSTATEMENTTOOLONG = "sqlstatement_toolong";

        public const string

// IN or OUT param missing.
MISSINGINOUT = "missinginout";

        public const string

// Statement in batch generated result set.
BATCHRESULTSET = "batchresultset";

        public const string

// Statement in batch generated result set.
BATCHRESULTSETWITHNUMBER = "batchresultset_withnumber";

        public const string

// Procedure call in batch contained OUT/INOUT.
BATCHPROCOUT = "batchprocout";

        public const string

// Procedure call in batch contained OUT/INOUT.
BATCHMISSINGIN = "batchmissingin";

        public const string

// A statement executed as update delivered a result set.
SQLSTATEMENTRESULTSET = "sqlstatement_resultset";

        public const string

// SQL command doesn't return a result set.
SQLCOMMANDNORESULTSET = "sqlcommand_noresultset";

        public const string

// A statement assumed to be a procedure call is not one.
SQLSTATEMENTNOPROCEDURE = "sqlstatement_noprocedure";

        public const string

// Column index not found.
COLINDEXNOTFOUND = "colindex_notfound";

        public const string

// Column name not found.
COLNAMENOTFOUND = "colname_notfound";

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
NODATAFOUND = "nodata_found";

        public const string

// Invalid data type
INVALIDDATATYPE = "invalid_datatype";

        public const string

// Unknown data type
UNKNOWNDATATYPE = "unknown_datatype";

        public const string

// Try to get record at position < first.
RESULTSETBEFOREFIRST = "resultset_beforefirst";

        public const string

// Try to get record at position > last.
RESULTSETAFTERLAST = "resultset_afterlast";

        public const string

// Try to retrieve unnamed parameter by name.
UNNAMEDPARAMETER = "unnamed_parameter";

        public const string

// No column names delivered from kernel.
NOCOLUMNNAMES = "no_columnnames";

        public const string

// Connection field is null.
INTERNALCONNECTIONNULL = "internal_connectionnull";

        public const string

// Cant not set isolation level.
CONNECTIONISOLATIONLEVEL = "connection_isolationlevel";

        public const string

// No more input expected at this place.
INTERNALUNEXPECTEDINPUT = "internal_unexpectedinput";

        public const string

// No more output expected at this place.
INTERNALUNEXPECTEDOUTPUT = "internal_unexpectedoutput";

        public const string

// Internal error: parse id is null.
INTERNALINVALIDPARSEID = "internal_invalidParseid";

        public const string

// getObject function failed
GETOBJECTFAILED = "getobject_failed";

        public const string

// Fetch operation delivered no data part.
FETCHNODATAPART = "fetch_nodatapart";

        public const string

// Fetch operation
FETCHDATAFAILED = "fetch_data_failed";

        public const string

// Runtime: connect to host failed.
HOSTCONNECTFAILED = "host_connect_failed";

        public const string

// Runtime: execution failed.
EXECFAILED = "exec_failed";

        public const string

// Runtime: receive of connect failed.
RECVCONNECT = "recv_connect";

        public const string

// Runtime: receive garbled reply
REPLYGARBLED = "connectreply_garbled";

        public const string

// Runtime: reconnect on admin session unsupported
ADMINRECONNECT = "admin_reconnect";

        public const string

// Runtime: chunk overflow in read
CHUNKOVERFLOW = "chunkoverflow";

        public const string

// Reading from a stream resulted in an IOException
STREAMIOEXCEPTION = "stream_ioexception";

        public const string

// Column nullable unknown
DBNULLUNKNOWN = "dbnull_unknown";

        public const string

// Output parameter value truncated
PARAMETERTRUNC = "parameter_truncated";

        public const string

// Parameter is null
PARAMETERNULL = "parameter_null";

        public const string

// Index is out if range
INDEXOUTOFRANGE = "index_outofrange";

        public const string

// Unsupported database features
SPECIALNUMBERUNSUPPORTED = "special_number_unsupported";

        public const string
OMSUNSUPPORTED = "oms_unsupported";

        public const string
TABLEDIRECTUNSUPPORTED = "tabledirect_unsupported";

        public const string

// Streams
CONVERSIONSTRINGSTREAM = "streamconversion_string";

        public const string
CONVERSIONBYTESTREAM = "streamconversion_bytes";

        public const string
STREAMISATEND = "stream_isatend";

        public const string
CONVERSIONSTRUCTURETYPE = "conversion_structuretype";

        public const string
STRUCTUREARRAYWRONGLENTGH = "structure_arraywronglength";

        public const string
STRUCTELEMENTNULL = "structure_element_null";

        public const string
STRUCTELEMENTCONVERSION = "structure_element_conversion";

        public const string
STRUCTELEMENTOVERFLOW = "structure_element_overflow";

        public const string

// Connection
CONNECTIONWRONGSERVERCHALLENGERECEIVED = "connection_wrongserverchallengereceived";

        public const string
CONNECTIONCHALLENGERESPONSENOTSUPPORTED = "connection_challengeresponsenotsupported";

        public const string
SSLCERTIFICATE = "ssl_certificate";

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
BIGINTOVERFLOW = "bigint_overflow";

        public const string
BIGINTUNDERFLOW = "bigint_underflow";

        public const string
BIGINTRADIXOVERFLOW = "bigint_radix_overflow";

        public const string

HASHCHANGEKEY = "hash_change_key";

        public const string
POOLNOTFOUND = "pool_not_found";

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
