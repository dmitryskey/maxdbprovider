using System;
using System.Text;

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
			END             =	 296,
		// other connect header constants
			DBNameSize        =      8,
			MinSize           =     64;   // for Unix vserver
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
			VarPartSize		=   12, // int4
			VarPartLen		=   16, // int4
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
			ReturnCode			= 18, // int2
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

			/**
			 * Call of cancel occured.
			*/
			ERROR_STATEMENT_CANCELLED="error.statement.cancelled",

			/**
			 * Try to execute null statement.
			 */
			ERROR_SQLSTATEMENT_NULL = "error.sqlstatement.null",

			/**
			 * Try to execute too long statement.
			 */
			ERROR_SQLSTATEMENT_TOOLONG = "error.sqlstatement.toolong",

			/**
			 * IN or OUT param missing.
			 */
			ERROR_MISSINGINOUT = "error.missinginout",

			/**
			 * Statement in batch generated result set.
			 */
			ERROR_BATCHRESULTSET = "error.batchresultset",

			/**
			 * Statement in batch generated result set.
			 */
			ERROR_BATCHRESULTSET_WITHNUMBER = "error.batchresultset.withnumber",

			/**
			 * Procedure call in batch contained OUT/INOUT.
			 */
			ERROR_BATCHPROCOUT = "error.batchprocout",

			/**
			 * Procedure call in batch contained OUT/INOUT.
			 */
			ERROR_BATCHMISSINGIN = "error.batchmissingin",

			/**
			 * A statement executed as query delivered a row count.
			 */
			ERROR_SQLSTATEMENT_ROWCOUNT = "error.sqlstatement.rowcount",

			/**
			 * A statement executed as update delivered a result set.
			 */
			ERROR_SQLSTATEMENT_RESULTSET = "error.sqlstatement.resultset",

			/**
			 * A statement assumed to be a procedure call is not one.
			 */
			ERROR_SQLSTATEMENT_NOPROCEDURE = "error.sqlstatement.noprocedure",

			/**
			 * Column index not found.
			 */
			ERROR_COLINDEX_NOTFOUND = "error.colindex.notfound",

			/**
			 * User name missing.
			 */
			ERROR_NOUSER = "error.nouser",

			/**
			 * Password missing.
			 */
			ERROR_NOPASSWORD = "error.nopassword",

			/**
			 * Password invalid.
			 */
			ERROR_INVALIDPASSWORD = "error.invalidpassword",

			/**
			 * Savepoint for auto-commit mode session.
			 */
			ERROR_CONNECTION_AUTOCOMMIT = "error.connection.autocommit",

			/**
			 * Argument to row is 0.
			 */
			ERROR_ROW_ISNULL = "error.row.isnull",

			/**
			 * Try to get record at position < first.
			 */
			ERROR_RESULTSET_BEFOREFIRST = "error.resultset.beforefirst",

			/**
			 * Try to get record at position > last.
			 */
			ERROR_RESULTSET_AFTERLAST = "error.resultset.afterlast",

			/**
			 * Try to fetch <0 or >maxrows items
			 */
			ERROR_INVALID_FETCHSIZE = "error.invalid.fetchsize",

			/**
			 * Try to set a field size of too less or to much
			 */
			ERROR_INVALID_MAXFIELDSIZE = "error.invalid.maxfieldsize",

			/**
			 * Try to set maxrows to less than 0.
			 */
			ERROR_INVALID_MAXROWS = "error.invalid.maxrows",

			/**
			 * Try to set query timeout < 0.
			 */
			ERROR_INVALID_QUERYTIMEOUT = "error.invalid.querytimeout",

			/**
			 * Try to update r/o result set.
			 */
			ERROR_RESULTSET_NOTUPDATABLE = "error.resultset.notupdatable",

			/**
			 * Try to retrieve named savepoint by index.
			 */
			ERROR_NAMED_SAVEPOINT = "error.named.savepoint",

			/**
			 * Try to retrieve unnamed savepoint by name.
			 */
			ERROR_UNNAMED_SAVEPOINT = "error.unnamed.savepoint",

			/**
			 * Try to use something as save point.
			 */
			ERROR_NO_SAVEPOINTSAPDB = "error.nosavepointsapdb",

			/**
			 * Try to use released savepoint
			 */
			ERROR_SAVEPOINT_RELEASED = "error.savepoint.released",

			/**
			 * Parse id part not found.
			 */
			ERROR_PARSE_NOPARSEID = "error.parse.noparseid",

			/**
			 * No column names delivered from kernel.
			 */
			ERROR_NO_COLUMNNAMES = "error.no.columnnames",

			/**
			 * Fieldinfo part not found.
			 */
			ERROR_PARSE_NOFIELDINFO = "error.parse.nofieldinfo",

			/**
			 * Data part not found after execute.
			 */
			ERROR_NODATAPART = "error.nodatapart",

			/**
			 * Statement assigned for updatable is not
			 */
			ERROR_NOTUPDATABLE = "error.notupdatable",

			/**
			 * Call statement did not deliver output data (DB Procedure stopped).
			 */
			ERROR_NOOUTPARAMDATA = "error.nooutparamdata",

			/**
			 * Unknown kind of part.
			 */
			WARNING_PART_NOTHANDLED="warning.part.nothandled",

			/**
			 * SQL Exception while updating result set.
			 */
			ERROR_INTERNAL_PREPAREHELPER = "error.internal.preparehelper",

			/**
			 * Connection field is null.
			 */
			ERROR_INTERNAL_CONNECTIONNULL = "error.internal.connectionnull",

			/**
			 * No more input expected at this place.
			 */
			ERROR_INTERNAL_UNEXPECTEDINPUT = "error.internal.unexpectedinput",

			/**
			 * No more output expected at this place.
			 */
			ERROR_INTERNAL_UNEXPECTEDOUTPUT = "error.internal.unexpectedoutput",

			/**
			 * Internal JDBC error: parse id is null.
			 */
			ERROR_INTERNAL_INVALIDPARSEID = "error.internal.invalidParseid",

			/**
			 * No updatable columns found.
			 */
			ERROR_NOCOLUMNS_UPDATABLE = "error.nocolumns.updatable",

			/**
			 * Runtime: unknown host.
			 */
			ERROR_UNKNOWN_HOST = "error.unknown.host",

			/**
			 * Runtime: connect to host failed.
			 */
			ERROR_HOST_CONNECT = "error.host.connect",

			/**
			 * Runtime: execution failed.
			 */
			ERROR_EXEC_FAILED = "error.exec.failed",
    
			/**
			 * Runtime: connect to host failed.
			 */
			ERROR_HOST_NICONNECT = "error.host.niconnect",
       
			/**
			 * Runtime: connect to host failed.
			 */
			ERROR_WRONG_CONNECT_URL = "error.host.wrongconnecturl",
			/**
			 * Runtime: load ni libraries failed.
			 */
			ERROR_LOAD_NILIBRARY = "error.host.niloadlibrary",
			/**
			 * Runtime: receive of connect failed.
			 */
			ERROR_RECV_CONNECT = "error.recv.connect",

			/**
			 * Runtime: receive garbled reply
			 */
			ERROR_REPLY_GARBLED = "error.connectreply.garbled",

			/**
			 * Runtime: connect reply receive failed
			 */
			ERROR_CONNECT_RECVFAILED = "error.connect.receivefailed",

			/**
			 * Runtime: data receive failed
			 */
			ERROR_DATA_RECVFAILED = "error.data.receivefailed",

			/**
			 * Runtime: data receive failed (w reason)
			 */
			ERROR_DATA_RECVFAILED_REASON = "error.data.receivefailed.reason",

			/**
			 * Runtime: reconnect on admin session unsupported
			 */
			ERROR_ADMIN_RECONNECT = "error.admin.reconnect",

			/**
			 * Runtime: invalid swapping
			 */
			ERROR_INVALID_SWAPPING = "error.invalid.swapping",

			/**
			 * Runtime: send: getoutputstream failed (IO exception)
			 */
			ERROR_SEND_GETOUTPUTSTREAM = "error.send.getoutputstream",

			/**
			 * Runtime: send: write failed (IO exception)
			 */
			ERROR_SEND_WRITE = "error.send.write",

			/**
			 * Runtime: chunk overflow in read
			 */
			ERROR_CHUNKOVERFLOW = "error.chunkoverflow",

			/**
			 * Util/printf: too few arguments for formatting.
			 */
			ERROR_FORMAT_TOFEWARGS = "error.format.toofewargs",

			/**
			 * Util/printf: format string must start with '%'
			 */
			ERROR_FORMAT_PERCENT = "error.format.percent",

			/**
			 * Util/printf: number object is not a number
			 */
			ERROR_FORMAT_NOTANUMBER = "error.format.notanumber",

			/**
			 * Util/printf: unknown format spec
			 */
			ERROR_FORMAT_UNKNOWN = "error.format.unknown",

			/**
			 * Util/printf: number object is not a number
			 */
			ERROR_FORMAT_TOOMANYARGS = "error.format.toomanyargs",

			/**
			 * No result set.
			 */
			WARNING_EMPTY_RESULTSET="warning.emptyresultset",

			/**
			 * <code>executeQuery(String)</code> called on <code>PreparedStatement</code>.
			 */
			ERROR_EXECUTEQUERY_PREPAREDSTATEMENT = "error.executequery.preparedstatement",

			/**
			 * <code>executeUpdate(String)</code> called on <code>PreparedStatement</code>.
			 */
			ERROR_EXECUTEUPDATE_PREPAREDSTATEMENT = "error.executeupdate.preparedstatement",

			/**
			 * Internal: <code>maxRows</code> inside result but end of result unknown (should not happen).
			 */
			ERROR_ASSERTION_MAXROWS_IN_RESULT = "error.assertion.maxrowsinresult",

			/**
			 * An illegal operation for a FORWARD_ONLY result set.
			 */
			ERROR_RESULTSET_FORWARDONLY = "error.resultset.forwardonly",

			/**
			 * <code>cancelRowUpdates</code> called while on insert row.
			 */
			ERROR_CANCELUPDATES_INSERTROW = "error.cancelupdates.insertrow",

			/**
			 * <code>deleteRow</code> called while on insert row.
			 */
			ERROR_DELETEROW_INSERTROW = "error.deleterow.insertrow",

			/**
			 * <code>deleteRow</code> called while on insert row.
			 */
			ERROR_UPDATEROW_INSERTROW = "error.updaterow.insertrow",

			/**
			 * <code>insertRow</code> called while not on insert row.
			 */
			ERROR_INSERTROW_INSERTROW = "error.insertrow.insertrow",

			/**
			 * <code>deleteRow</code> called while not positioned at a row
			 */
			ERROR_DELETEROW_NOROW = "error.deleterow.norow",

			/**
			 * <code>updateRow</code> called while not positioned at a row
			 */
			ERROR_UPDATEROW_NOROW = "error.updaterow.norow",

			/**
			 * Reading from a stream resulted in an IOException
			 */
			ERROR_STREAM_IOEXCEPTION = "error.stream.ioexception",

			/**
			 * Problem in access to DatabaseMetaData.
			 */
			ERROR_NOMETADATA = "error.nometadata",

			// Column nullable unknown
			ERROR_DBNULL_UNKNOWN = "error.dbnull.unknown",

			// jdbcext messages
			ERROR_DATABASE_NOT_SET = "error.database.notset",
			ERROR_CONNECTION_INVALIDATED = "error.connection.invalidated",
			ERROR_COMMIT_XASESSION = "error.commit.xasession",
			ERROR_ROLLBACK_XASESSION = "error.rollback.xasession",
			ERROR_AUTOCOMMIT_XASESSION = "error.autocommit.xasession",

			ERROR_MANDATORY_PROPERTY_NOTFOUND = "error.mandatory.property.notfound",
			ERROR_NOT_STRINGREFADDR = "error.not.stringrefaddr",
			ERROR_NOT_STRING_CONTENT = "error.not.string.content",
			ERROR_NOT_NULL_CONTENT = "error.not.null.content",

			ERROR_JNDILOOKUP_FAILED = "error.connection.JNDILookup",

			// unsupported database features
			ERROR_ARRAY_UNSUPPORTED = "error.array.unsupported",
			ERROR_TIMEZONE_UNSUPPORTED = "error.timezone.unsupported",
			ERROR_REF_UNSUPPORTED = "error.ref.unsupported",
			ERROR_URL_UNSUPPORTED = "error.url.unsupported",
			ERROR_MEMORYRESULT_METHOD_UNSUPPORTED = "error.memoryresult.method.unsupported",
			ERROR_AUTOGENKEYS_RETRIEVAL_UNSUPPORTED = "error.autogenkeys.retrieval.unsupported",
			ERROR_SPECIAL_NUMBER_UNSUPPORTED = "error.special.number.unsupported",
			ERROR_OMS_UNSUPPORTED = "error.oms.unsupported",
	
			// communication errors
			COMMERROR_OK="commerror.ok",
			COMMERROR_CONNECTDOWN="commerror.connectiondown",
			COMMERROR_TASKLIMIT="commerror.tasklimit",
			COMMERROR_TIMEOUT="commerror.timeout",
			COMMERROR_CRASH="commerror.crash",
			COMMERROR_RESTARTREQUIRED="commerror.restartrequired",
			COMMERROR_SHUTDOWN="commerror.shutdown",
			COMMERROR_SENDLINEDOWN="commerror.sendlinedown",
			COMMERROR_RECVLINEDOWN="commerror.recvlinedown",
			COMMERROR_PACKETLIMIT="commerror.packetlimit",
			COMMERROR_RELEASED="commerror.released",
			COMMERROR_WOULDBLOCK="commerror.wouldblock",
			COMMERROR_UNKNOWNREQUEST="commerror.unknownrequest",
			COMMERROR_SERVERDBUNKNOWN="commerror.serverdbunknown",

			// OMS Streams
			ERROR_STREAM_ODDSIZE = "error.stream.oddsize",
			ERROR_CONVERSION_STRINGSTREAM  = "error.stream.conversion.string",
			ERROR_STREAM_SOURCEREAD = "error.stream.sourceread",
			ERROR_CONVERSION_BYTESTREAM = "error.stream.conversion.bytes",
			ERROR_STREAM_EOF = "error.stream.eof",
			ERROR_STREAM_NODATA = "error.stream.nodata",
			ERROR_STREAM_UNKNOWNTYPE = "error.stream.unknowntype",
			ERROR_STREAM_ISATEND = "error.stream.isatend",
			ERROR_CONVERSION_STRUCTURETYPE = "error.conversion.structuretype",	
			ERROR_STRUCTURE_INCOMPLETE = "error.structure.incomplete",
			ERROR_STRUCTURE_COMPLETE = "error.structure.complete",				
			ERROR_STRUCTURE_ARRAYWRONGLENTGH = "error.structure.arraywronglength",
			ERROR_STRUCT_ELEMENT_NULL = "error.structure.element.null",
			ERROR_STRUCT_ELEMENT_CONVERSION = "error.structure.element.conversion",
			ERROR_STRUCT_ELEMENT_OVERFLOW = "error.structure.element.overflow",
			ERROR_STREAM_BLOB_UNSUPPORTED = "error.stream.blob.unsupported",
			ERROR_INVALID_BLOB_POSITION = "error.invalid.blob.position",	
			// LONG in DBPROCs
			ERROR_UNKNOWN_PROCEDURELONG = "error.unknown.procedurelong",
			ERROR_MESSAGE_NOT_AVAILABLE = "error.message.notavailable",

			// Connection
			ERROR_CONNECTION_WRONGSERVERCHALLENGERECEIVED = "error.connection.wrongserverchallengereceived",
			ERROR_CONNECTION_CHALLENGERESPONSENOTSUPPORTED = "error.connection.challengeresponsenotsupported",
			// the rest
			ERROR="error",
			UNKNOWNTYPE="unknowntype",
			COMMERROR="commerror",
			INPUTPOS="inputpos";

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
		public static readonly string[] Value = {"NULL", "SESSION", "INTERNAL", "ANSI", "DB2", "ORACLE", "SAPR3"};
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
			GetExecute              =  14,
			PutValue                  =  15,
			GetValue                  =  16,
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
		public const byte 
			Nil                     =   0,
			ApplParameterDescription =  1,
			ColumnNames             =   2,
			Command                 =   3,
			ConvTablesReturned      =   4,
			Data                    =   5,
			ErrorText               =   6,
			GetInfo                 =   7,
			Modulname               =   8,
			Page                    =   9,
			Parsid                  =  10,
			ParsidOfSelect          =  11,
			ResultCount             =  12,
			ResultTableName         =  13,
			ShortInfo               =  14,
			UserInfoReturned        =  15,
			Surrogate               =  16,
			Bdinfo                  =  17,
			LongData                =  18,
			TableName               =  19,
			SessionInfoReturned     =  20,
			OutputColsNoParameter   =  21,
			Key                     =  22,
			Serial                  =  23,
			RelativePos             =  24,
			AbapIStream             =  25,
			AbapOStream             =  26,
			AbapInfo                =  27,
			CheckpointInfo          =  28,
			Procid                  =  29,
			Long_Demand             =  30,
			MessageList             =  31,
			Vardata_ShortInfo       =  32,
			Vardata                 =  33,
			Feature                 =  34,
			Clientid                =  35;
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
		public const byte 
			LastPacket              =   0,
			NextPacket              =   1,
			FirstPacket             =   2,
			Fill3                   =   3,
			Fill4                   =   4,
			Fill5                   =   5,
			Fill6                   =   6,
			Fill7                   =   7,
			LastPacket_Ext          =   1,
			NextPacket_Ext          =   2,
			FirstPacket_Ext         =   4;
	}

	//
	// copy of gsp00::tsp00_LongDescBlock and related constants
	//
	public class LongDesc 
	{
		// tsp00_LdbChange
		public const byte 
			UseTermchar = 0,
			UseConversion = 1,
			UseToAscii = 2,
			UseUCS2_Swap = 3,

			// tsp00_ValMode
			DataPart = 0,
			AllData = 1,
			LastData = 2,
			NoData = 3,
			NoMoreData = 4,
			LastPutval = 5,
			DataTrunc = 6,
			Close = 7,
			Error = 8,
			StartposInvalid = 9,

			// infoset
			ExTrigger = 1,
			WithLock = 2,
			NoCLose = 4,
			NewRec = 8,
			IsComment = 16,
			IsCatalog = 32,
			Unicode = 64,

			// state
			StateUseTermChar   = 1, 
			StateStream        = 1, 
			StateUseConversion = 2,
			StateUseToAscii    = 4,
			StateUseUcs2Swap   = 8,
			StateShortScol     = 16,
			StateFirstInsert   = 32,
			StateCopy          = 64,
			StateFirstCall     = 128,
    
			// tsp00_LongDescBlock = RECORD
			Descriptor = 0,   // c8
			Tabid = 8,        // c8
			MaxLen = 16,      // c4
			InternPos = 20,   // i4
			Infoset = 24,     // set1
			State = 25,		  // bool
			unused1 = 26,     // c1
			ValMode = 27,     // i1
			ValInd = 28,      // i2
			unused = 30,      // i2
			ValPos = 32,      // i4
			ValLen = 36,      // i4
			Size = 40;
	}

	internal class Packet
	{
		//
		// indicators for fields with variable length
		//   
		public const byte 
			MaxOneByteLength   = 245,
			Ignored            = 250,
			SpecialNull        = 251,
			BlobDescription    = 252,
			DefaultValue       = 253,
			NullValue          = 254,
			TwiByteLength      = 255;
    
		// 
		// property names used to identify fields
		///
		public const string MaxPasswordLenTag  = "maxpasswordlen";
	}

	internal class GCMode
	{
		public const int 
			/**
			 * Control flag for garbage collection on execute. If this is
			 * set, old cursors/parse ids are sent <i>together with</i> the current
			 * statement for being dropped.
			 */
			GC_ALLOWED = 1,

			/**
			 * Control flag for garbage collection on execute. If this is
			 * set, old cursors/parse ids are sent <i>after</i> the current
			 * statement for being dropped.
			 */
			GC_DELAYED = 2,

			/**
			 * Control flag for garbage collection on execute. If this is
			 * set, nothing is done to drop cursors or parse ids.
			 */
			GC_NONE    = 3;
	}

	public class FunctionCode 
	{
		public const int 
			Nil          =   0,
			CreateTable  =   1,
			SetRole      =   2,
			Insert       =   3,
			Select       =   4,
			Update       =   5,
			Delete       =   9,
			Explain       =  27,
			DBProcExecute =  34,

			FetchFirst   = 206,
			FetchLast    = 207,
			FetchNext    = 208,
			FetchPrev    = 209,
			FetchPos     = 210,
			FetchSame    = 211,
			FetchRelative    = 247,

			Show         = 216,
			Describe     = 224,
			Select_into  = 244,
			DBProcWithResultSetExecute = 248,
			MSelect      = 1004,
			MDelete      = 1009,

			MFetchFirst  = 1206,
			MFetchLast   = 1207,
			MFetchNext   = 1208,
			MFetchPrev   = 1209,
			MFetchPos    = 1210,
			MFetchSame   = 1211,
			MSelect_into = 1244,
			MFetchRelative   = 1247,

			// copy of application codes coded in the 11th byte of parseid
			none                     =   0,
			command_executed         =   1,
			use_adbs                 =   2,
			release_found            =  10,
			fast_select_dir_possible =  20,
			not_allowed_for_program  =  30,
			close_found              =  40,
			describe_found           =  41,
			fetch_found              =  42,
			mfetch_found             =  43,
			mass_select_found        =  44,
			reuse_mass_select_found  =  46,
			mass_command             =  70,
			mselect_found            = 114,
			for_upd_mselect_found    = 115,
			reuse_mselect_found      = 116,
			reuse_upd_mselect_found  = 117;

		public static readonly int[] massCmdAppCodes  = 
			{
				mfetch_found, 
				mass_select_found, 
				reuse_mass_select_found,
				mass_command, 
				mselect_found, 
				for_upd_mselect_found,
				reuse_mselect_found, 
				reuse_upd_mselect_found
			};
	}

	internal class Ports
	{
		public const int 
			Default        =   7210,
			DefaultSecure  =   7270,
			DefaultNI      =   7269;
	}

	internal class CommError
	{
		public static readonly string[] ErrorText = 
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
		public const byte 
			SQLOK                   =      0,
			SQLNOTOK                =      1,
			SQLTASKLIMIT            =      2,
			SQLTIMEOUT              =      3,
			SQLCRASH                =      4,
			SQLSTART_REQUIRED       =      5,
			SQLSHUTDOWN             =      6,
			SQLSEND_LINE_DOWN       =      7,
			SQLRECEIVE_LINE_DOWN    =      8,
			SQLPACKETLIMIT          =      9,
			SQLRELEASED             =     10,
			SQLWOULDBLOCK           =     11,
			SQLUNKNOWN_REQUEST      =     12,
			SQLSERVER_DB_UNKNOWN    =     13;
	}

	internal class RSQLTypes
	{
		// request/reply types
		public const byte 
			RTE_PROT_TCP          =      3,
			INFO_REQUEST_KEEP_ALIVE  =     50,
			INFO_REQUEST          =     51,
			INFO_REPLY            =     52,
			USER_CONN_REQUEST     =     61,
			USER_CONN_REPLY       =     62,
			USER_DATA_REQUEST     =     63,
			USER_DATA_REPLY       =     64,
			USER_CANCEL_REQUEST   =     65,
			USER_RELEASE_REQUEST  =     66,
			KERN_CONN_REQUEST     =     71,
			KERN_CONN_REPLY       =     72,
			KERN_DATA_REQUEST     =     73,
			KERN_DATA_REPLY       =     74,
			KERN_RELEASE_REQUEST  =     76,
			DUMP_REQUEST          =     81,
			CTRL_CONN_REQUEST     =     91,
			CTRL_CONN_REPLY       =     92,
			CTRL_CANCEL_REQUEST   =     93,
			NORMAL                =      0;
	}

	internal class SwapMode
	{
		public const byte 
			NotSwapped	=	1,
			Swapped		=	2;
	}

	internal class SQLType
	{
		// user types
		public const byte 
			USER           =	0,
			ASYNC_USER     =	1,
			UTILITY        =	2,
			DISTRIBUTION   =	3,
			CONTROL        =	4,
			EVENT          =	5;
	}

	internal class ArgType
	{
		//geo03.h
		public const byte 
			PORT_NO         = 	0x50,   // = P
			REM_PID         = 	0x49,   // = I
			ACKNOWLEDGE     = 	0x52,   // = R
			NODE            = 	0x3E,   // = N
			DBROOT          = 	0x64,   // = d
			SERVERPGM       = 	0x70,   // = p
			AUTH_ALLOW      = 	0x61,   // = a
			OMIT_REPLY_PART = 	0x72;   // = r
	}

	internal class DataType 
	{
		public const byte
			MIN       		= 0,            
			FIXED     		= MIN, 
			FLOAT     		= 1,            
			CHA       		= 2,            
			CHE       		= 3,            
			CHB       		= 4,            
			ROWID     		= 5,            
			STRA      		= 6,            
			STRE      		= 7,            
			STRB      		= 8,            
			STRDB     		= 9,            
			DATE      		= 10,           
			TIME      		= 11,           
			VFLOAT    		= 12,           
			TIMESTAMP 		= 13,           
			UNKNOWN   		= 14,           
			NUMBER    		= 15,           
			NONUMBER  		= 16,           
			DURATION  		= 17,           
			DBYTEEBCDIC		= 18,         
			LONGA     		= 19,           
			LONGE     		= 20,           
			LONGB     		= 21,           
			LONGDB    		= 22,           
			BOOLEAN   		= 23,           
			UNICODE   		= 24,           
			DTFILLER1 		= 25,           
			DTFILLER2 		= 26,           
			DTFILLER3 		= 27,           
			DTFILLER4 		= 28,           
			SMALLINT  		= 29,           
			INTEGER   		= 30,           
			VARCHARA  		= 31,           
			VARCHARE  		= 32,           
			VARCHARB  		= 33,           
			STRUNI    		= 34,           
			LONGUNI   		= 35,           
			VARCHARUNI		= 36,          
			UDT				= 37,           
			ABAPTABHANDLE	= 38,       
			DWYDE			= 39,           
			MAX				= DWYDE;

		public static readonly string[] stringValues = 
		{
			"FIXED",
			"FLOAT",
			"CHAR ASCII",
			"CHAR EBCDIC",
			"CHAR BYTE",
			"ROWID",
			"STRA",
			"STRE",
			"STRB",
			"STRDB",
			"DATE",
			"TIME",
			"VFLOAT",
			"TIMESTAMP",
			"UNKNOWN",
			"NUMBER",
			"NONUMBER",
			"DURATION",
			"DBYTEEBCDIC",
			"LONG ASCII",
			"LONG EBCDIC",
			"LONG BYTE",
			"LONGDB",
			"BOOLEAN",
			"UNICODE",
			"DTFILLER1",
			"DTFILLER2",
			"DTFILLER3",
			"DTFILLER4",
			"SMALLINT",
			"INTEGER",
			"VARCHAR ASCII",
			"VARCHAR EBCDIC",
			"VARCHAR BYTE",
			"STRUNI",
			"LONG UNICODE",
			"VARCHAR UNICODE",
			"UDT",
			"ABAP STREAM",
			"DWYDE"
		};
	}

	//copies of tsp1_param_opt_type, tsp1_param_io_type, tsp1_param_info
	internal class ParamInfo 
	{
		// param modes, declared as set values
		public const int Mandatory		= 1;
		public const int Optional		= 2;
		public const int Default		= 4;
		public const int EscapeChar		= 8;
		// param io types
		public const int Input			= 0;
		public const int Output			= 1;
		public const int InOut			= 2;
		// layout of tsp1_param_info
		public const int ModeOffset		= 0;     // Set 1
		public const int IOTypeOffset	= 1;   // enum 1
		public const int DataTypeOffset = 2; // enum1
		public const int FracOffset		= 3;     // int1
		public const int LengthOffset	= 4;   // int2
		public const int InOutLenOffset = 6; // int2
		public const int BufPosOffset	= 8;   // int4
		public const int ParamNoOffset  = 8;    // int2
		public const int ReadOnlyOffset = 10;   // int1
		public const int SerialOffset   = 11;   // int1
		// The size of tsp1_param_info 
		public const int END			= 12;
	}

	internal class Vsp00Consts
	{
		public const int  KnlIdentifier  = 32;
		public const byte DefinedBinary  = 0;
		public const byte DefinedUnicode = 1;
		public const byte DefinedAscii   = (byte) ' ';
		public const byte UndefByte      = (byte) 0xff;
		public const byte NormalSwap     = 1;
		public const byte FullSwap       = 2;
	}

	internal class StreamHandle 
	{
		public const int Header_Length = 20;	
		public const int RowDef    = 0;
		public const int TabHandle = 4;
	
		public const int TabHandle_Reserved  =  4;
		public const int TabHandle_Rowsize   =  8;
		public const int TabHandle_ColCount  = 10;
		public const int TabHandle_RowCount  = 12;
		public const int TabHandle_ABAPTabID = 16;
		public const int TabHandle_ColDesc_0 = 20;
	
		public const int ColDesc_InOut     = 0;
		public const int ColDesc_ABAPType  = 1;
		public const int ColDesc_Dec       = 2;
		public const int ColDesc_Length    = 4;
		public const int ColDesc_Offset    = 6;
	
		public const int ColDesc_Size      = 8;	
	
		public const int StreamType_Bool   = 0;
		public const int StreamType_Int1   = 1;
		public const int StreamType_UInt1  = 2;
		public const int StreamType_Int2   = 3;
		public const int StreamType_UInt2  = 4;
		public const int StreamType_Int4   = 5;
		public const int StreamType_UInt4  = 6;
		public const int StreamType_Int8   = 7;
		public const int StreamType_UInt8  = 8;
		public const int StreamType_Float  = 9;
		public const int StreamType_Double = 10;
		public const int StreamType_Int    = 12;
		public const int StreamType_UInt   = 13;
		public const int StreamType_Char   = 14;
		public const int StreamType_WChar  = 15;
		public const int StreamType_UDT    = 16;
		public const int StreamType_WYDE   = 17;
	
		public const int Stream_IN    = 0;
		public const int Stream_OUT   = 1;
		public const int Stream_INOUT = 2;
	}

	internal enum FetchType
	{
		FIRST			= 1,	// The fetch operation type of a FETCH FIRST.
		LAST			= 2,	// The fetch operation type of a FETCH LAST.
		ABSOLUTE_UP		= 3,	// The fetch operation type of a FETCH ABSOLUTE with an argument >1.
		ABSOLUTE_DOWN	= 4,	// The fetch operation type of a FETCH ABSOLUTE with an argument <1.
		RELATIVE_UP		= 5,	// The fetch operation type of a FETCH RELATIVE with an argument >1.
		RELATIVE_DOWN	= 6		// The fetch operation type of a FETCH RELATIVE with an argument <1.
	}

	internal enum PositionType
	{
		BEFORE_FIRST  = 1,	// Constant indicating that the current position is <i>before the first row
		INSIDE        =	2,	// Constant indicating that the current position is at the result set.
		AFTER_LAST    = 3,	// Constant indicating that the current position is behind the last row.
		NOT_AVAILABLE =	4	// Constant indicating that the current position is not available.
	}

	/// <summary>
	/// Summary description for Consts.
	/// </summary>
	internal class Consts
	{
		// some constants
		public const byte ASCIIClient              =      0;
		public const byte UnicodeSwapClient        =     19;
		public const byte UnicodeClient			   =     20;
		public const byte RSQL_DOTNET              =     13;

		public static readonly int unicodeWidth = System.Text.Encoding.Unicode.GetByteCount(" ");
		public const int fillBufSize = 1024;
		public const string blankChar = " ";
		public static readonly byte[] zeroBytes = new byte [fillBufSize];
		public static readonly byte[] blankBytes = new byte[fillBufSize];
		public static readonly byte[] blankUnicodeBytes = new byte[fillBufSize * unicodeWidth];
		public static readonly byte[] blankBigEndianUnicodeBytes = new byte[fillBufSize * unicodeWidth];

		public const int AlignValue	= 8;

		public const int ReserveFor2ndSegment = 8192; //8kB reserve size in order packet if more than 1 segment will be used
		public const int ReserveForReply = SegmentHeaderOffset.Part - PartHeaderOffset.Data + 200;
		public const int defaultmaxNumberOfSegm = 6; //default maximum number of segments for a request packet

		public const int reserveForReply = SegmentHeaderOffset.Part - PartHeaderOffset.Data + 200;

		public const string AppID = "ADO";//"ODB"; 
		public const string ApplVers = "10100";//"70400";
		public static readonly bool IsLittleEndian = BitConverter.IsLittleEndian;
		public const string Cursor_Prefix = "ADONET_CURSOR_";//"JDBC_CURSOR_";

		static Consts()
		{
			for (int i = 0; i < fillBufSize; i += unicodeWidth) 
			{
				zeroBytes[i] = zeroBytes [i + 1] = 0;
				blankBytes[i] = blankBytes[i + 1] = Encoding.ASCII.GetBytes(blankChar)[0];
				Encoding.Unicode.GetBytes(blankChar).CopyTo(blankUnicodeBytes, 0);
				Encoding.BigEndianUnicode.GetBytes(blankChar).CopyTo(blankBigEndianUnicodeBytes, 0);
			}
		}
	}
}
