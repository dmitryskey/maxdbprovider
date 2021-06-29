//-----------------------------------------------------------------------------------------------
// <copyright file="MaxDBMessages.cs" company="2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG">
// Copyright © 2005-2021 Dmitry S. Kataev, 2002-2003 SAP AG. All rights reserved.
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

        // Runtime: connect to host failed.
        public const string
HOSTCONNECTFAILED = "host_connect_failed";

        // Runtime: execution failed.
        public const string
EXECFAILED = "exec_failed";

        // Runtime: receive of connect failed.
        public const string
RECVCONNECT = "recv_connect";

        // Runtime: receive garbled reply
        public const string
REPLYGARBLED = "connectreply_garbled";

        // Runtime: reconnect on admin session unsupported
        public const string
ADMINRECONNECT = "admin_reconnect";

        // Runtime: chunk overflow in read
        public const string
CHUNKOVERFLOW = "chunkoverflow";

        // Reading from a stream resulted in an IOException
        public const string
STREAMIOEXCEPTION = "stream_ioexception";

        // Column nullable unknown
        public const string
DBNULLUNKNOWN = "dbnull_unknown";

        // Output parameter value truncated
        public const string
PARAMETERTRUNC = "parameter_truncated";

        // Parameter is null
        public const string
PARAMETERNULL = "parameter_null";

        // Index is out if range
        public const string
INDEXOUTOFRANGE = "index_outofrange";

        // Unsupported database features
        public const string
SPECIALNUMBERUNSUPPORTED = "special_number_unsupported";

        public const string
OMSUNSUPPORTED = "oms_unsupported";

        public const string
TABLEDIRECTUNSUPPORTED = "tabledirect_unsupported";

        // Streams
        public const string
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

        // Connection
        public const string
STRUCTELEMENTOVERFLOW = "structure_element_overflow";

        public const string
CONNECTIONWRONGSERVERCHALLENGERECEIVED = "connection_wrongserverchallengereceived";

        public const string
CONNECTIONCHALLENGERESPONSENOTSUPPORTED = "connection_challengeresponsenotsupported";

        public const string
SSLCERTIFICATE = "ssl_certificate";

        // communication errors
        public const string
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

        // big integer messages
        public const string
BIGINTOVERFLOW = "bigint_overflow";

        public const string
BIGINTUNDERFLOW = "bigint_underflow";

        public const string
BIGINTRADIXOVERFLOW = "bigint_radix_overflow";

        public const string HASHCHANGEKEY = "hash_change_key";

        public const string POOLNOTFOUND = "pool_not_found";

        // the rest
        public const string
ERROR = "error";

        public const string
UNKNOWNTYPE = "unknowntype";
    }

    internal class MaxDBMessages
    {
        private static readonly ResourceManager rm = new ResourceManager("MaxDB.Data.MaxDBMessages", typeof(MaxDBMessages).Assembly);

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
                    result.Append('.');
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
