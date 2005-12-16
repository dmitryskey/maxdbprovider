using System;

namespace MaxDBDataProvider.MaxDBProtocol
{
	#region "Offsets"

	internal class HeaderOffset
	{
		public const byte 
			ActSendLen        =      0,   // INT4
			ProtocolID        =      4,   // INT1
			MessClass         =      5,   // INT1
			RTEFlags          =      6,   // INT1
			ResidualPackets   =      7,   // INT1
			SenderRef         =      8,   // INT4
			ReceiverRef       =     12,   // INT4
			RTEReturnCode     =     16,   // INT2
			Filler            =     18,   // INT2
			MaxSendLen        =     20,   // INT4
			END               =     24;
	}

	internal class ConnectPacketOffset
	{
		public const int 
			MessCode	     =	   0,   // C2
			ConnectLength   =	   2,   // INT2
			ServiceType     =	   4,   // INT1
			OSType          =	   5,   // INT1
			Filler1         =	   6,   // INT1
			Filler2         =	   7,   // INT1
			MaxSegmentSize  =	   8,   // INT4
			MaxDataLen      =	  12,   // INT4
			PacketSize      =	  16,   // INT4
			MinReplySize    =	  20,   // INT4
			ServerDB        =	  24,   // C8
			ClientDB        =	  32,   // C8
			VarPart			=	  40,   // C256
			END             =	 296;
	}

	//
	// offsets of vsp001::tsp1_packet
	//
	internal class PacketHeaderOffset
	{
		public const byte 
			MessCode		=	0,  // enum1
			MessSwap		=	1,  // enum1
			Filler1			=   2,  // int2
			ApplVersion		=   4,  // c5
			Appl			=   9,  // c3
			VarpartSize		=   12, // int4
			VarpartLen		=   16, // int4
			Filler2			=   20, // int2
			NoOfSegm		=   22, // int2
			Filler3			=   24, // c8
			Segment			=   32;
	}

	//
	// offsets of vsp001::tsp1_segment
	//
	internal class SegmentHeaderOffset
	{
		// common header
		public const byte 
			Len					= 0,  // int4
			Offs				= 4,  // int4
			NoOfParts			= 8,  // int2
			OwnIndex			= 10, // int2
			SegmKind			= 12, // enum1
			// request segment
			MessType			= 13, // enum1
			SqlMode				= 14, // enum1
			Producer			= 15, // enum1
			CommitImmediateley	= 16, // bool
			IgnoreCostwarning	= 17, // bool
			Prepare				= 18, // bool
			WithInfo			= 19, // bool
			MassCmd				= 20, // bool
			ParsingAgain		= 21, // bool
			CommandOptions		= 22, // enum1
			// reply segment
			SqlState			= 13, // c5
			Returncode			= 18, // int2
			ErrorPos			= 20, // int4
			ExternWarning		= 24, // set2
			InternWarning		= 26, // set2
			FunctionCode		= 28, // int2
			TraceLevel			= 30, // int1
			Part				= 40;
	}

	//
	// offsets of vsp001::tsp1_part
	//
	internal class PartHeaderOffset 
	{
		public const byte 
			PartKind	= 0,   // enum1
			Attributes	= 1,   // set1
			ArgCount	= 2,   // int2
			SegmOffs	= 4,   // int4
			BufLen		= 8,   // int4
			BufSize		= 12,  // int4
			Data		= 16;
	}

	#endregion

	#region "Message keys"

	public struct MessageKey
	{
		/**
		 * An RTEException was thrown in a connect operation.
		 */
		public const string ERROR_CONNECTRTEEXCEPTION = "error.connect.rteexception";

		/**
		 * The transaction isolation level set as option is invalid.
		 */
		public const string ERROR_INVALIDTRANSACTIONISOLATION = "error.invalid.transactionisolation";

		/*
		 * The transport option was set to an invalid value.
		 */
		public const string ERROR_INVALIDTRANSPORT = "error.invalid.transport";

		/**
		 * Native support library was not initialised.
		 */
		public const string ERROR_LIBRARYNOTLOADED = "error.library.notloaded";

		/**
		 * Value overflow.
		 */
		public const string ERROR_VALUEOVERFLOW = "error.valueoverflow";

		/**
		 * Database exception (with error position).
		 */
		public const string ERROR_DATABASEEXCEPTION = "error.databaseexception";

		/**
		 * Database exception (without error position).
		 */
		public const string ERROR_DATABASEEXCEPTION_WOERRPOS = "error.databaseexception.woerrpos";

		/**
		 * Invalid argument value.
		 */
		public const string ERROR_INVALIDARGUMENTVALUE = "error.invalid.argumentvalue";

		/**
		 * Invalid argument value, correct examples are given.
		 */
		public const string ERROR_INVALIDARGUMENTVALUE_WEXAMPLE = "error.invalid.argumentvalue.wexample";

		/**
		 * Invalid column index.
		 */
		public const string ERROR_INVALIDCOLUMNINDEX = "error.invalidcolumnindex";

		/**
		 * Invalid column name.
		 */
		public const string ERROR_INVALIDCOLUMNNAME = "error.invalidcolumnname";

		/**
		 * Something is not yet there.
		 */
		public const string ERROR_NOTIMPLEMENTED = "error.notimplemented";

		/**
		 * Stream for LONG input could not be resetted.
		 */
		public const string ERROR_RESET_STREAM = "error.reset.stream";

		/**
		* An object is closed but shouldn't.
		*/
		public const string ERROR_OBJECTISCLOSED = "error.objectisclosed";

		/**
		 * A time out.
		 */
		public const string ERROR_TIMEOUT = "error.timeout";

		/**
		 * Restart required.
		 */
		public const string ERROR_RESTARTREQUIRED = "error.restart.required";

		/**
		 * Invariant: messswap <= 0
		 */
		public const string ERROR_INVARIANT_MESSSWAP = "error.invariant.messswap";

		/**
		 * No longdata packet.
		 */
		public const string ERROR_LONGDATAEXPECTED = "error.longdata.expected";

		/**
		 * Invalid startposition for long data. 
		 */
		public const string ERROR_INVALID_STARTPOSITION = "error.invalid.startposition";

		/**
		 * SQL -> .NET type conversion.
		 */
		public const string ERROR_CONVERSIONSQLNET = "error.conversion.sqlnet";

		/**
		 * .NET -> SQL type conversion.
		 */
		public const string ERROR_CONVERSIONNETSQL = "error.conversion.netsql";

		/**
		 * Data -> any type conversion.
		 */
		public const string ERROR_CONVERSIONDATA = "error.conversion.data";

		/**
		 * VDN Number -> BigDecimal conversion.
		 */
		public const string ERROR_CONVERSIONVDNnumber = "error.conversion.VDNnumber";

		/**
		 * VDN Number -> Special Null value.
		 */
		public const string ERROR_CONVERSIONSpecialNullValue = "error.conversion.SpecialNullValue";

		/**
		 * Unsupported blob navigation.
		 */
		public const string ERROR_MOVEBACKWARDINBLOB = "error.movebackwardinblob";

		/**
		 * Try to read ASCII data from LONG column.
		 */
		public const string ERROR_ASCIIREADFROMLONG = "error.asciireadfromlong";

		/**
		 * Try to read binary data from LONG column.
		 */
		public const string ERROR_BINARYREADFROMLONG = "error.binaryreadfromlong";

		/**
		 * Try to put ASCII data into LONG column.
		 */
		public const string ERROR_ASCIIPUTTOLONG = "error.asciiputtolong";


		/**
		 * No data type translator.
		 */
		public const string ERROR_NOTRANSLATOR = "error.notranslator";

		/**
		 * Try to put binary data into LONG column.
		 */
		public const string ERROR_BINARYPUTTOLONG = "error.binaryputtolong";

		/**
		 * Call of addBatch on prepared statement (not ok).
		 */
		public const string ERROR_PREPAREDSTATEMENT_ADDBATCH = "error.addbatch.preparedstatement";

		/**
		 * Try to execute null statement.
		 */
		public const string ERROR_SQLSTATEMENT_NULL = "error.sqlstatement.null";

		/**
		 * Try to execute too long statement.
		 */
		public const string ERROR_SQLSTATEMENT_TOOLONG = "error.sqlstatement.toolong";

		/**
		 * IN or OUT param missing.
		 */
		public const string ERROR_MISSINGINOUT = "error.missinginout";

		/**
		 * Statement in batch generated result set.
		 */
		public const string ERROR_BATCHRESULTSET = "error.batchresultset";

		/**
		 * Statement in batch generated result set.
		 */
		public const string ERROR_BATCHRESULTSET_WITHNUMBER = "error.batchresultset.withnumber";

		/**
		 * Procedure call in batch contained OUT/INOUT.
		 */
		public const string ERROR_BATCHPROCOUT = "error.batchprocout";

		/**
		 * Procedure call in batch contained OUT/INOUT.
		 */
		public const string ERROR_BATCHMISSINGIN = "error.batchmissingin";

		/**
		 * A statement executed as query delivered a row count.
		 */
		public const string ERROR_SQLSTATEMENT_ROWCOUNT = "error.sqlstatement.rowcount";

		/**
		 * A statement executed as update delivered a result set.
		 */
		public const string ERROR_SQLSTATEMENT_RESULTSET = "error.sqlstatement.resultset";

		/**
		 * A statement assumed to be a procedure call is not one.
		 */
		public const string ERROR_SQLSTATEMENT_NOPROCEDURE = "error.sqlstatement.noprocedure";

		/**
		 * Column index not found.
		 */
		public const string ERROR_COLINDEX_NOTFOUND = "error.colindex.notfound";

		/**
		 * User name missing.
		 */
		public const string ERROR_NOUSER = "error.nouser";

		/**
		 * Password missing.
		 */
		public const string ERROR_NOPASSWORD = "error.nopassword";

		/**
		 * Password invalid.
		 */
		public const string ERROR_INVALIDPASSWORD = "error.invalidpassword";

		/**
		 * Savepoint for auto-commit mode session.
		 */
		public const string ERROR_CONNECTION_AUTOCOMMIT = "error.connection.autocommit";

		/**
		 * Argument to row is 0.
		 */
		public const string ERROR_ROW_ISNULL = "error.row.isnull";

		/**
		 * Try to get record at position < first.
		 */
		public const string ERROR_RESULTSET_BEFOREFIRST = "error.resultset.beforefirst";

		/**
		 * Try to get record at position > last.
		 */
		public const string ERROR_RESULTSET_AFTERLAST = "error.resultset.afterlast";

		/**
		 * Try to fetch <0 or >maxrows items
		 */
		public const string ERROR_INVALID_FETCHSIZE = "error.invalid.fetchsize";

		/**
		 * Try to set a field size of too less or to much
		 */
		public const string ERROR_INVALID_MAXFIELDSIZE = "error.invalid.maxfieldsize";

		/**
		 * Try to set maxrows to less than 0.
		 */
		public const string ERROR_INVALID_MAXROWS = "error.invalid.maxrows";

		/**
		 * Try to set query timeout < 0.
		 */
		public const string ERROR_INVALID_QUERYTIMEOUT = "error.invalid.querytimeout";

		/**
		 * Try to update r/o result set.
		 */
		public const string ERROR_RESULTSET_NOTUPDATABLE = "error.resultset.notupdatable";

		/**
		 * Try to retrieve named savepoint by index.
		 */
		public const string ERROR_NAMED_SAVEPOINT = "error.named.savepoint";

		/**
		 * Try to retrieve unnamed savepoint by name.
		 */
		public const string ERROR_UNNAMED_SAVEPOINT = "error.unnamed.savepoint";

		/**
		 * Try to use something as save point.
		 */
		public const string ERROR_NO_SAVEPOINTSAPDB = "error.nosavepointsapdb";

		/**
		 * Try to use released savepoint
		 */
		public const string ERROR_SAVEPOINT_RELEASED = "error.savepoint.released";

		/**
		 * Parse id part not found.
		 */
		public const string ERROR_PARSE_NOPARSEID = "error.parse.noparseid";

		/**
		 * No column names delivered from kernel.
		 */
		public const string ERROR_NO_COLUMNNAMES = "error.no.columnnames";

		/**
		 * Fieldinfo part not found.
		 */
		public const string ERROR_PARSE_NOFIELDINFO = "error.parse.nofieldinfo";

		/**
		 * Data part not found after execute.
		 */
		public const string ERROR_NODATAPART = "error.nodatapart";

		/**
		 * Statement assigned for updatable is not
		 */
		public const string ERROR_NOTUPDATABLE = "error.notupdatable";

		/**
		 * Call statement did not deliver output data (DB Procedure stopped).
		 */
		public const string ERROR_NOOUTPARAMDATA = "error.nooutparamdata";

		/**
		 * Unknown kind of part.
		 */
		public const string WARNING_PART_NOTHANDLED="warning.part.nothandled";

		/**
		 * SQL Exception while updating result set.
		 */
		public const string ERROR_INTERNAL_PREPAREHELPER = "error.internal.preparehelper";

		/**
		 * Connection field is null.
		 */
		public const string ERROR_INTERNAL_CONNECTIONNULL = "error.internal.connectionnull";

		/**
		 * No more input expected at this place.
		 */
		public const string ERROR_INTERNAL_UNEXPECTEDINPUT = "error.internal.unexpectedinput";

		/**
		 * No more output expected at this place.
		 */
		public const string ERROR_INTERNAL_UNEXPECTEDOUTPUT = "error.internal.unexpectedoutput";

		/**
		 * Internal JDBC error: parse id is null.
		 */
		public const string ERROR_INTERNAL_INVALIDPARSEID = "error.internal.invalidParseid";

		/**
		 * No updatable columns found.
		 */
		public const string ERROR_NOCOLUMNS_UPDATABLE = "error.nocolumns.updatable";

		/**
		 * Runtime: unknown host.
		 */
		public const string ERROR_UNKNOWN_HOST = "error.unknown.host";

		/**
		 * Runtime: connect to host failed.
		 */
		public const string ERROR_HOST_CONNECT = "error.host.connect";

		/**
		 * Runtime: execution failed.
		 */
		public const string ERROR_EXEC_FAILED = "error.exec.failed";
    
		/**
		 * Runtime: connect to host failed.
		 */
		public const string ERROR_HOST_NICONNECT = "error.host.niconnect";
       
		/**
		 * Runtime: connect to host failed.
		 */
		public const string ERROR_WRONG_CONNECT_URL = "error.host.wrongconnecturl";
		/**
		 * Runtime: load ni libraries failed.
		 */
		public const string ERROR_LOAD_NILIBRARY = "error.host.niloadlibrary";
		/**
		 * Runtime: receive of connect failed.
		 */
		public const string ERROR_RECV_CONNECT = "error.recv.connect";

		/**
		 * Runtime: receive garbled reply
		 */
		public const string ERROR_REPLY_GARBLED = "error.connectreply.garbled";

		/**
		 * Runtime: connect reply receive failed
		 */
		public const string ERROR_CONNECT_RECVFAILED = "error.connect.receivefailed";

		/**
		 * Runtime: data receive failed
		 */
		public const string ERROR_DATA_RECVFAILED = "error.data.receivefailed";

		/**
		 * Runtime: data receive failed (w reason)
		 */
		public const string ERROR_DATA_RECVFAILED_REASON = "error.data.receivefailed.reason";

		/**
		 * Runtime: reconnect on admin session unsupported
		 */
		public const string ERROR_ADMIN_RECONNECT = "error.admin.reconnect";

		/**
		 * Runtime: invalid swapping
		 */
		public const string ERROR_INVALID_SWAPPING = "error.invalid.swapping";

		/**
		 * Runtime: send: getoutputstream failed (IO exception)
		 */
		public const string ERROR_SEND_GETOUTPUTSTREAM = "error.send.getoutputstream";

		/**
		 * Runtime: send: write failed (IO exception)
		 */
		public const string ERROR_SEND_WRITE = "error.send.write";

		/**
		 * Runtime: chunk overflow in read
		 */
		public const string ERROR_CHUNKOVERFLOW = "error.chunkoverflow";

		/**
		 * Util/printf: too few arguments for formatting.
		 */
		public const string ERROR_FORMAT_TOFEWARGS = "error.format.toofewargs";

		/**
		 * Util/printf: format string must start with '%'
		 */
		public const string ERROR_FORMAT_PERCENT = "error.format.percent";

		/**
		 * Util/printf: number object is not a number
		 */
		public const string ERROR_FORMAT_NOTANUMBER = "error.format.notanumber";

		/**
		 * Util/printf: unknown format spec
		 */
		public const string ERROR_FORMAT_UNKNOWN = "error.format.unknown";

		/**
		 * Util/printf: number object is not a number
		 */
		public const string ERROR_FORMAT_TOOMANYARGS = "error.format.toomanyargs";

		/**
		 * No result set.
		 */
		public const string WARNING_EMPTY_RESULTSET="warning.emptyresultset";

		/**
		 * <code>executeQuery(String)</code> called on <code>PreparedStatement</code>.
		 */
		public const string ERROR_EXECUTEQUERY_PREPAREDSTATEMENT = "error.executequery.preparedstatement";

		/**
		 * <code>executeUpdate(String)</code> called on <code>PreparedStatement</code>.
		 */
		public const string ERROR_EXECUTEUPDATE_PREPAREDSTATEMENT = "error.executeupdate.preparedstatement";

		/**
		 * Internal: <code>maxRows</code> inside result but end of result unknown (should not happen).
		 */
		public const string ERROR_ASSERTION_MAXROWS_IN_RESULT = "error.assertion.maxrowsinresult";

		/**
		 * An illegal operation for a FORWARD_ONLY result set.
		 */
		public const string ERROR_RESULTSET_FORWARDONLY = "error.resultset.forwardonly";

		/**
		 * <code>cancelRowUpdates</code> called while on insert row.
		 */
		public const string ERROR_CANCELUPDATES_INSERTROW = "error.cancelupdates.insertrow";

		/**
		 * <code>deleteRow</code> called while on insert row.
		 */
		public const string ERROR_DELETEROW_INSERTROW = "error.deleterow.insertrow";

		/**
		 * <code>deleteRow</code> called while on insert row.
		 */
		public const string ERROR_UPDATEROW_INSERTROW = "error.updaterow.insertrow";

		/**
		 * <code>insertRow</code> called while not on insert row.
		 */
		public const string ERROR_INSERTROW_INSERTROW = "error.insertrow.insertrow";

		/**
		 * <code>deleteRow</code> called while not positioned at a row
		 */
		public const string ERROR_DELETEROW_NOROW = "error.deleterow.norow";

		/**
		 * <code>updateRow</code> called while not positioned at a row
		 */
		public const string ERROR_UPDATEROW_NOROW = "error.updaterow.norow";

		/**
		 * Reading from a stream resulted in an IOException
		 */
		public const string ERROR_STREAM_IOEXCEPTION = "error.stream.ioexception";

		/**
		 * Problem in access to DatabaseMetaData.
		 */
		public const string ERROR_NOMETADATA = "error.nometadata";

		// jdbcext messages
		public const string ERROR_DATABASE_NOT_SET = "error.database.notset";
		public const string ERROR_CONNECTION_INVALIDATED = "error.connection.invalidated";
		public const string ERROR_COMMIT_XASESSION = "error.commit.xasession";
		public const string ERROR_ROLLBACK_XASESSION = "error.rollback.xasession";
		public const string ERROR_AUTOCOMMIT_XASESSION = "error.autocommit.xasession";

		public const string ERROR_MANDATORY_PROPERTY_NOTFOUND = "error.mandatory.property.notfound";
		public const string ERROR_NOT_STRINGREFADDR = "error.not.stringrefaddr";
		public const string ERROR_NOT_STRING_CONTENT = "error.not.string.content";
		public const string ERROR_NOT_NULL_CONTENT = "error.not.null.content";

		public const string ERROR_JNDILOOKUP_FAILED = "error.connection.JNDILookup";

		// unsupported database features
		public const string ERROR_ARRAY_UNSUPPORTED = "error.array.unsupported";
		public const string ERROR_TIMEZONE_UNSUPPORTED = "error.timezone.unsupported";
		public const string ERROR_REF_UNSUPPORTED = "error.ref.unsupported";
		public const string ERROR_URL_UNSUPPORTED = "error.url.unsupported";
		public const string ERROR_MEMORYRESULT_METHOD_UNSUPPORTED = "error.memoryresult.method.unsupported";
		public const string ERROR_AUTOGENKEYS_RETRIEVAL_UNSUPPORTED = "error.autogenkeys.retrieval.unsupported";
		public const string ERROR_SPECIAL_NUMBER_UNSUPPORTED = "error.special.number.unsupported";

		// unimplemented todos
		public const string ERROR_SETBYTES_NOTIMPLEMENTED = "error.setbytes.notimplemented";
		public const string ERROR_SETBINARYSTREAM_NOTIMPLEMENTED = "error.setbinarystream.notimplemented";
		public const string ERROR_TRUNCATE_NOTIMPLEMENTED = "error.truncate.notimplemented";
		public const string ERROR_SETSTRING_NOTIMPLEMENTED = "error.setstring.notimplemented";
		public const string ERROR_SETASCIISTREAM_NOTIMPLEMENTED = "error.setasciistream.notimplemented";
		public const string ERROR_SETCHARACTERSTREAM_NOTIMPLEMENTED = "error.setcharacterstream.notimplemented";
		public const string ERROR_GETSUBSTRING_NOTIMPLEMENTED = "error.getsubstring.notimplemented";
		public const string ERROR_POSITION_NOTIMPLEMENTED = "error.position.notimplemented";
		public const string ERROR_GETBYTES_NOTIMPLEMENTED = "error.getbytes.notimplemented";
		public const string ERROR_GETOBJECT_NOTIMPLEMENTED = "error.getobject.notimplemented";
		public const string ERROR_PREPARESTATEMENT_NOTIMPLEMENTED = "error.preparestatement.notimplemented";
		public const string ERROR_GETCOLUMNCLASSNAME_NOTIMPLEMENTED = "error.getcolumnclassname.notimplemented";
		public const string ERROR_MEMORYRESULT_METHOD_NOTIMPLEMENTED = "error.memoryresult.method.notimplemented";
		public const string ERROR_CODESET_UNSUPPORTED = "error.codeset.unsupported";
	
		// communication errors
		public const string COMMERROR_OK="commerror.ok";
		public const string COMMERROR_CONNECTDOWN="commerror.connectiondown";
		public const string COMMERROR_TASKLIMIT="commerror.tasklimit";
		public const string COMMERROR_TIMEOUT="commerror.timeout";
		public const string COMMERROR_CRASH="commerror.crash";
		public const string COMMERROR_RESTARTREQUIRED="commerror.restartrequired";
		public const string COMMERROR_SHUTDOWN="commerror.shutdown";
		public const string COMMERROR_SENDLINEDOWN="commerror.sendlinedown";
		public const string COMMERROR_RECVLINEDOWN="commerror.recvlinedown";
		public const string COMMERROR_PACKETLIMIT="commerror.packetlimit";
		public const string COMMERROR_RELEASED="commerror.released";
		public const string COMMERROR_WOULDBLOCK="commerror.wouldblock";
		public const string COMMERROR_UNKNOWNREQUEST="commerror.unknownrequest";
		public const string COMMERROR_SERVERDBUNKNOWN="commerror.serverdbunknown";

		// OMS Streams
		public const string ERROR_STREAM_ODDSIZE = "error.stream.oddsize";
		public const string ERROR_CONVERSION_STRINGSTREAM  = "error.stream.conversion.string";
		public const string ERROR_STREAM_SOURCEREAD = "error.stream.sourceread";
		public const string ERROR_CONVERSION_BYTESTREAM = "error.stream.conversion.bytes";
		public const string ERROR_STREAM_EOF = "error.stream.eof";
		public const string ERROR_STREAM_NODATA = "error.stream.nodata";
		public const string ERROR_STREAM_UNKNOWNTYPE = "error.stream.unknowntype";
		public const string ERROR_STREAM_ISATEND = "error.stream.isatend";
		public const string ERROR_CONVERSION_STRUCTURETYPE = "error.conversion.structuretype";	
		public const string ERROR_STRUCTURE_INCOMPLETE = "error.structure.incomplete";
		public const string ERROR_STRUCTURE_COMPLETE = "error.structure.complete";				
		public const string ERROR_STRUCTURE_ARRAYWRONGLENTGH = "error.structure.arraywronglength";
		public const string ERROR_STRUCT_ELEMENT_NULL = "error.structure.element.null";
		public const string ERROR_STRUCT_ELEMENT_CONVERSION = "error.structure.element.conversion";
		public const string ERROR_STRUCT_ELEMENT_OVERFLOW = "error.structure.element.overflow";
		public const string ERROR_STREAM_BLOB_UNSUPPORTED = "error.stream.blob.unsupported";
		public const string ERROR_INVALID_BLOB_POSITION = "error.invalid.blob.position";	
		// LONG in DBPROCs
		public const string ERROR_UNKNOWN_PROCEDURELONG = "error.unknown.procedurelong";
		public const string ERROR_MESSAGE_NOT_AVAILABLE = "error.message.notavailable";

		// Connection
		public const string ERROR_CONNECTION_WRONGSERVERCHALLENGERECEIVED = "error.connection.wrongserverchallengereceived";
		public const string ERROR_CONNECTION_CHALLENGERESPONSENOTSUPPORTED = "error.connection.challengeresponsenotsupported";
		// the rest
		public const string ERROR="error";
		public const string UNKNOWNTYPE="unknowntype";
		public const string COMMERROR="commerror";
		public const string INPUTPOS="inputpos";

	}

	#endregion

	//
	// copy of vsp001::tsp1_segment_kind
	//
	internal class SegmKind 
	{
		public const byte 
			Nil                =   0,
			Cmd                =   1,
			Return             =   2,
			Proccall           =   3,
			Procreply          =   4,
			LastSegmentKind    =   5;
	}

	internal class SegmentCmdOption 
	{
		public const byte 
			selfetch_off         =   1,
			scrollable_cursor_on =   2;
	}

	//
	// copy of vsp001::tsp1_sqlmode
	//
	public class SqlMode 
	{
		public const byte 
			Nil               =   0,
			SessionSqlmode    =   1,
			Internal          =   2,
			Ansi              =   3,
			Db2               =   4,
			Oracle            =   5,
			SAPR3			  =   6;
	}

	public class SqlModeName
	{
		public static string[] Value = {"NULL", "SESSION", "INTERNAL", "ANSI", "DB2", "ORACLE", "SAPR3"};
	}

	//
	// copy of vsp001::tsp1_producer
	//
	internal class Producer 
	{
		public const byte 
			Nil             =   0,
			UserCmd         =   1,
			InternalCmd     =   2,
			Kernel          =   3,
			Installation    =   4;
	}

	//
	// copy of vsp001::tsp1_cmd_mess_type.
	//
	internal class CmdMessType 
	{
		public const byte 
			Nil                     =   0,
			CmdLowerBound           =   1,
			Dbs                     =   2,
			Parse                   =   3,
			Getparse                =   4,
			Syntax                  =   5,
			Cfill1                  =   6,
			Cfill2                  =   7,
			Cfill3                  =   8,
			Cfill4                  =   9,
			Cfill5                  =  10,
			CmdUpperBound           =  11,
			NoCmdLowerBound         =  12,
			Execute                 =  13,
			Getexecute              =  14,
			Putval                  =  15,
			Getval                  =  16,
			Load                    =  17,
			Unload                  =  18,
			Ncfill1                 =  19,
			Ncfill2                 =  20,
			Ncfill3                 =  21,
			Ncfill4                 =  22,
			Ncfill5                 =  23,
			NoCmdUpperBound         =  24,
			Hello                   =  25,
			UtilLowerBound          =  26,
			Utility                 =  27,
			Incopy                  =  28,
			Ufill1                  =  29,
			Outcopy                 =  30,
			DiagOutcopy             =  31,
			Ufill3                  =  32,
			Ufill4                  =  33,
			Ufill5                  =  34,
			Ufill6                  =  35,
			Ufill7                  =  36,
			UtilUpperBound          =  37,
			SpecialsLowerBound      =  38,
			Switch                  =  39,
			Switchlimit             =  40,
			Buflength               =  41,
			Minbuf                  =  42,
			Maxbuf                  =  43,
			StateUtility            =  44,
			Sfill2                  =  45,
			Sfill3                  =  46,
			Sfill4                  =  47,
			Sfill5                  =  48,
			SpecialsUpperBound      =  49,
			WaitForEvent            =  50,
			ProcservLowerBound      =  51,
			ProcservCall            =  52,
			ProcservReply           =  53,
			ProcservFill1           =  54,
			ProcservFill2           =  55,
			ProcservFill3           =  56,
			ProcservFill4           =  57,
			ProcservFill5           =  58,
			ProcservUpperBound      =  59,
			LastCmdMessType         =  60;
	}

	//
	// copy of vsp001::tsp1_part_kind
	//
	internal class PartKind 
	{
		public const byte Nil                     =   0;
		public const byte ApplParameterDescription =  1;
		public const byte ColumnNames             =   2;
		public const byte Command                 =   3;
		public const byte ConvTablesReturned      =   4;
		public const byte Data                    =   5;
		public const byte Errortext               =   6;
		public const byte GetInfo                 =   7;
		public const byte Modulname               =   8;
		public const byte Page                    =   9;
		public const byte Parsid                  =  10;
		public const byte ParsidOfSelect          =  11;
		public const byte ResultCount             =  12;
		public const byte ResultTableName         =  13;
		public const byte ShortInfo               =  14;
		public const byte UserInfoReturned        =  15;
		public const byte Surrogate               =  16;
		public const byte Bdinfo                  =  17;
		public const byte Longdata                =  18;
		public const byte Tablename               =  19;
		public const byte SessionInfoReturned     =  20;
		public const byte OutputColsNoParameter   =  21;
		public const byte Key                     =  22;
		public const byte Serial                  =  23;
		public const byte RelativePos             =  24;
		public const byte AbapIStream             =  25;
		public const byte AbapOStream             =  26;
		public const byte AbapInfo                =  27;
		public const byte CheckpointInfo          =  28;
		public const byte Procid                  =  29;
		public const byte Long_Demand             =  30;
		public const byte MessageList             =  31;
		public const byte Vardata_Shortinfo       =  32;
		public const byte Vardata                 =  33;
		public const byte Feature                 =  34;
		public const byte Clientid                =  35;
	}

	internal class Feature 
	{
		public const byte 
			MultipleDropParseid   =  1,
			SpaceOption           =  2,
			VariableInput         =  3,
			OptimizedStreams      =  4,
			CheckScrollableOption =  5;
	}

	//
	// copy of vsp001::tsp1_part_attributes.
	//
	// The _E-values can be used to build a set by ORing them
	//
	public class PartAttributes 
	{
		public const byte LastPacket              =   0;
		public const byte NextPacket              =   1;
		public const byte FirstPacket             =   2;
		public const byte Fill3                   =   3;
		public const byte Fill4                   =   4;
		public const byte Fill5                   =   5;
		public const byte Fill6                   =   6;
		public const byte Fill7                   =   7;
		public const byte LastPacket_Ext          =   1;
		public const byte NextPacket_Ext          =   2;
		public const byte FirstPacket_Etx         =   4;
	}

	//
	// copy of gsp00::tsp00_LongDescBlock and related constants
	//
	public class LongDesc 
	{
		// tsp00_LdbChange
		public const byte UseTermchar = 0;
		public const byte UseConversion = 1;
		public const byte UseToAscii = 2;
		public const byte UseUCS2_Swap = 3;

		// tsp00_ValMode
		public const byte DataPart = 0;
		public const byte AllData = 1;
		public const byte LastData = 2;
		public const byte NoData = 3;
		public const byte NoMoreData = 4;
		public const byte LastPutval = 5;
		public const byte DataTrunc = 6;
		public const byte Close = 7;
		public const byte Error = 8;
		public const byte StartposInvalid = 9;

		// infoset
		public const byte ExTrigger = 1;
		public const byte WithLock = 2;
		public const byte NoCLose = 4;
		public const byte NewRec = 8;
		public const byte IsComment = 16;
		public const byte IsCatalog = 32;
		public const byte Unicode = 64;

		// state
		public const byte StateUseTermChar   = 1; 
		public const byte StateStream        = 1; 
		public const byte StateUseConversion = 2;
		public const byte StateUseToAscii    = 4;
		public const byte StateUseUcs2Swap   = 8;
		public const byte StateShortScol     = 16;
		public const byte StateFirstInsert   = 32;
		public const byte StateCopy          = 64;
		public const byte StateFirstCall     = 128;
    
		// tsp00_LongDescBlock = RECORD
		public const byte Descriptor = 0;   // c8
		public const byte Tabid = 8;        // c8
		public const byte MaxLen = 16;      // c4
		public const byte InternPos = 20;   // i4
		public const byte Infoset = 24;     // set1
		public const byte State = 25;   // bool
		public const byte unused1 = 26;     // c1
		public const byte Valmode = 27;     // i1
		public const byte Valind = 28;      // i2
		public const byte unused = 30;      // i2
		public const byte Valpos = 32;      // i4;
		public const byte Vallen = 36;      // i4;
		public const byte Size = 40;
	}

	internal class Packet
	{
		//
		// indicators for fields with variable length
		//   
		public const byte MaxOneByteLength   = 245;
		public const byte Ignored            = 250;
		public const byte SpecialNull        = 251;
		public const byte BlobDescription    = 252;
		public const byte DefaultValue       = 253;
		public const byte NullValue          = 254;
		public const byte TwiByteLength      = 255;
    
		// 
		// property names used to identify fields
		///
		public const string MaxPasswordLenTag  = "maxpasswordlen";
	}

	internal class GCMode
	{
		/**
		 * Control flag for garbage collection on execute. If this is
		 * set, old cursors/parse ids are sent <i>together with</i> the current
		 * statement for being dropped.
		 */
		public const int GC_ALLOWED = 1;

		/**
		 * Control flag for garbage collection on execute. If this is
		 * set, old cursors/parse ids are sent <i>after</i> the current
		 * statement for being dropped.
		 */
		public const int GC_DELAYED = 2;

		/**
		 * Control flag for garbage collection on execute. If this is
		 * set, nothing is done to drop cursors or parse ids.
		 */
		public const int GC_NONE    = 3;
	}

	public class FunctionCode 
	{
		public const int Nil          =   0;
		public const int CreateTable  =   1;
		public const int SetRole      =   2;
		public const int Insert       =   3;
		public const int Select       =   4;
		public const int Update       =   5;
		public const int Delete       =   9;
		public const int Explain       =  27;
		public const int DBProcExecute =  34;

		public const int FetchFirst   = 206;
		public const int FetchLast    = 207;
		public const int FetchNext    = 208;
		public const int FetchPrev    = 209;
		public const int FetchPos     = 210;
		public const int FetchSame    = 211;
		public const int FetchRelative    = 247;

		public const int Show         = 216;
		public const int Describe     = 224;
		public const int Select_into  = 244;
		public const int DBProcWithResultSetExecute = 248;
		public const int MSelect      = 1004;
		public const int MDelete      = 1009;

		public const int MFetchFirst  = 1206;
		public const int MFetchLast   = 1207;
		public const int MFetchNext   = 1208;
		public const int MFetchPrev   = 1209;
		public const int MFetchPos    = 1210;
		public const int MFetchSame   = 1211;
		public const int MSelect_into = 1244;
		public const int MFetchRelative   = 1247;

		/*
		 * copy of application codes coded in the 11th byte of parseid
		 */
		public const int none                     =   0;
		public const int command_executed         =   1;
		public const int use_adbs                 =   2;
		public const int release_found            =  10;
		public const int fast_select_dir_possible =  20;
		public const int not_allowed_for_program  =  30;
		public const int close_found              =  40;
		public const int describe_found           =  41;
		public const int fetch_found              =  42;
		public const int mfetch_found             =  43;
		public const int mass_select_found        =  44;
		public const int reuse_mass_select_found  =  46;
		public const int mass_command             =  70;
		public const int mselect_found            = 114;
		public const int for_upd_mselect_found    = 115;
		public const int reuse_mselect_found      = 116;
		public const int reuse_upd_mselect_found  = 117;

		public static int[] massCmdAppCodes  = 
			{
				mfetch_found, mass_select_found, reuse_mass_select_found,
				mass_command, mselect_found, for_upd_mselect_found,
				reuse_mselect_found, reuse_upd_mselect_found
			};
	}

	internal class Ports
	{
		public const int Default        =   7210;
		public const int DefaultSecure  =   7270;
		public const int DefaultNI      =   7269;
	}

	internal class CommError
	{
		public static string[] ErrorText = 
			{
				"OK",
				"Connection down, session released",
				"Tasklimit",
				"Timeout",
				"Crash",
				"Restart required",
				"Shutdown",
				"Send line down",
				"Receive line down",
				"Packet limit",
				"Released",
				"Would block",
				"Unknown Request",
				"Server or DB unknown"
			};
	}

	internal class RTEReturnCodes
	{
		// rte return codes
		public const byte SQLOK                   =      0;
		public const byte SQLNOTOK                =      1;
		public const byte SQLTASKLIMIT            =      2;
		public const byte SQLTIMEOUT              =      3;
		public const byte SQLCRASH                =      4;
		public const byte SQLSTART_REQUIRED       =      5;
		public const byte SQLSHUTDOWN             =      6;
		public const byte SQLSEND_LINE_DOWN       =      7;
		public const byte SQLRECEIVE_LINE_DOWN    =      8;
		public const byte SQLPACKETLIMIT          =      9;
		public const byte SQLRELEASED             =     10;
		public const byte SQLWOULDBLOCK           =     11;
		public const byte SQLUNKNOWN_REQUEST      =     12;
		public const byte SQLSERVER_DB_UNKNOWN    =     13;
	}

	internal class RSQLTypes
	{
		// request/reply types
		public const byte RTE_PROT_TCP          =      3;
		public const byte INFO_REQUEST_KEEP_ALIVE  =     50;
		public const byte INFO_REQUEST          =     51;
		public const byte INFO_REPLY            =     52;
		public const byte USER_CONN_REQUEST     =     61;
		public const byte USER_CONN_REPLY       =     62;
		public const byte USER_DATA_REQUEST     =     63;
		public const byte USER_DATA_REPLY       =     64;
		public const byte USER_CANCEL_REQUEST   =     65;
		public const byte USER_RELEASE_REQUEST  =     66;
		public const byte KERN_CONN_REQUEST     =     71;
		public const byte KERN_CONN_REPLY       =     72;
		public const byte KERN_DATA_REQUEST     =     73;
		public const byte KERN_DATA_REPLY       =     74;
		public const byte KERN_RELEASE_REQUEST  =     76;
		public const byte DUMP_REQUEST          =     81;
		public const byte CTRL_CONN_REQUEST     =     91;
		public const byte CTRL_CONN_REPLY       =     92;
		public const byte CTRL_CANCEL_REQUEST   =     93;
		public const byte NORMAL                =      0;
	}

	internal class SwapMode
	{
		public const byte NotSwapped  = 1;
		public const byte Swapped	  =	2;
	}

	internal class SQLType
	{
		// user types
		public const byte USER           = 0;
		public const byte ASYNC_USER     = 1;
		public const byte UTILITY        = 2;
		public const byte DISTRIBUTION   = 3;
		public const byte CONTROL        = 4;
		public const byte EVENT          = 5;
	}

	internal class ArgType
	{
		//geo03.h
		public const byte PORT_NO         = 0x50;   // = P
		public const byte REM_PID         = 0x49;   // = I
		public const byte ACKNOWLEDGE     = 0x52;   // = R
		public const byte NODE            = 0x3E;   // = N
		public const byte DBROOT          = 0x64;   // = d
		public const byte SERVERPGM       = 0x70;   // = p
		public const byte AUTH_ALLOW      = 0x61;   // = a
		public const byte OMIT_REPLY_PART = 0x72;   // = r
	}

	/// <summary>
	/// Summary description for Consts.
	/// </summary>
	internal class Consts
	{
		// some constants
		public const byte ASCIIClient              =      0;
		public const byte UnicodeSwapClient        =     19;
		public const byte RSQL_WIN32               =     13;
		public const byte RSQL_JAVA                = RSQL_WIN32;

		public const int AlignValue				   = 8;

		public const int ReserveFor2ndSegment = 8192; //8kB reserve size in order packet if more than 1 segment will be used
		public const int ReserveForReply = SegmentHeaderOffset.Part - PartHeaderOffset.Data + 200;
		public const int defaultmaxNumberOfSegm = 6; //default maximum number of segments for a request packet

		// other connect header constants
		public const int DBNameSize        =      8;
		public const int MinSize           =     64;   // for Unix vserver

		public const int reserveForReply = SegmentHeaderOffset.Part - PartHeaderOffset.Data + 200;

		public const string AppID="ADO";
		public const string ApplVers="10100";
	}
}
