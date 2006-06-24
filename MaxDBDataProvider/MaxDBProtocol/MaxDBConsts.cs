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
using System.Text;

namespace MaxDB.Data.MaxDBProtocol
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
			Offset				= 4,  // int4
			NoOfParts			= 8,  // int2
			OwnIndex			= 10, // int2
			SegmKind			= 12, // enum1
			// request segment
			MessType			= 13, // enum1
			SqlMode				= 14, // enum1
			Producer			= 15, // enum1
			CommitImmediately	= 16, // bool
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
			SegmOffset	= 4,   // int4
			BufLen		= 8,   // int4
			BufSize		= 12,  // int4
			Data		= 16;
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
			LongDemand             =  30,
			MessageList             =  31,
			Vardata_ShortInfo       =  32,
			Vardata                 =  33,
			Feature                 =  34,
			Clientid                =  35;

		public static readonly string[] Name = new string[]{
										 "Nil",
										 "ApplParameterDescription",
										 "ColumnNames",
										 "Command",
										 "ConvTablesReturned",
										 "Data",
										 "ErrorText",
										 "GetInfo",
										 "ModulName",
										 "Page",
										 "Parsid",
										 "ParsidOfSelect",
										 "ResultCount",
										 "ResultTableName",
										 "ShortInfo",
										 "UserInfoReturned",
										 "Surrogate",
										 "Bdinfo",
										 "LongData",
										 "TableName",
										 "SessionInfoReturned",
										 "OutputColsNoParameter",
										 "Key",
										 "Serial",
										 "RelativePos",
										 "AbapIStream",
										 "AbapOStream",
										 "AbapInfo",
										 "CheckpointInfo",
										 "Procid",
										 "LongDemand",
										 "MessageList",
										 "VardataShortinfo",
										 "Vardata",
										 "Feature",
										 "Clientid"};
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
	internal class PartAttributes 
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

	/// <summary>
	/// copy of gsp00::tsp00_LongDescBlock and related constants
	/// </summary>
	internal class LongDesc 
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

	internal class FunctionCode 
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

		public static bool IsQuery(int code)
		{
			return (code == FunctionCode.Select || code == FunctionCode.Show ||	
				code == FunctionCode.DBProcWithResultSetExecute || code == FunctionCode.Explain); 
		}
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
				MaxDBMessages.Extract(MaxDBMessages.COMMERROR_OK),
				MaxDBMessages.Extract(MaxDBMessages.COMMERROR_CONNECTDOWN),
				MaxDBMessages.Extract(MaxDBMessages.COMMERROR_TASKLIMIT),
				MaxDBMessages.Extract(MaxDBMessages.COMMERROR_TIMEOUT),
				MaxDBMessages.Extract(MaxDBMessages.COMMERROR_CRASH),
				MaxDBMessages.Extract(MaxDBMessages.COMMERROR_RESTARTREQUIRED),
				MaxDBMessages.Extract(MaxDBMessages.COMMERROR_SHUTDOWN),
				MaxDBMessages.Extract(MaxDBMessages.COMMERROR_SENDLINEDOWN),
				MaxDBMessages.Extract(MaxDBMessages.COMMERROR_RECVLINEDOWN),
				MaxDBMessages.Extract(MaxDBMessages.COMMERROR_PACKETLIMIT),
				MaxDBMessages.Extract(MaxDBMessages.COMMERROR_RELEASED),
				MaxDBMessages.Extract(MaxDBMessages.COMMERROR_WOULDBLOCK),
				MaxDBMessages.Extract(MaxDBMessages.COMMERROR_UNKNOWNREQUEST),
				MaxDBMessages.Extract(MaxDBMessages.COMMERROR_SERVERDBUNKNOWN)
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
			RTE_PROT_TCP				=      3,
			INFO_REQUEST_KEEP_ALIVE		=     50,
			INFO_REQUEST          		=     51,
			INFO_REPLY            		=     52,
			USER_CONN_REQUEST     		=     61,
			USER_CONN_REPLY       		=     62,
			USER_DATA_REQUEST     		=     63,
			USER_DATA_REPLY       		=     64,
			USER_CANCEL_REQUEST   		=     65,
			USER_RELEASE_REQUEST  		=     66,
			KERN_CONN_REQUEST     		=     71,
			KERN_CONN_REPLY       		=     72,
			KERN_DATA_REQUEST     		=     73,
			KERN_DATA_REPLY       		=     74,
			KERN_RELEASE_REQUEST  		=     76,
			DUMP_REQUEST          		=     81,
			CTRL_CONN_REQUEST     		=     91,
			CTRL_CONN_REPLY       		=     92,
			CTRL_CANCEL_REQUEST   		=     93,
			NORMAL                		=      0;
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

		public static readonly string[] StrValues = 
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
		public const int 
            Mandatory		= 1,
		    Optional		= 2,
		    Default		    = 4,
		    EscapeChar		= 8,
		    // param io types
		    Input			= 0,
		    Output			= 1,
		    InOut			= 2,
		    // layout of tsp1_param_info
		    ModeOffset		= 0,  // Set 1
		    IOTypeOffset	= 1,  // enum 1
		    DataTypeOffset  = 2,  // enum1
		    FracOffset		= 3,  // int1
		    LengthOffset	= 4,  // int2
		    InOutLenOffset  = 6,  // int2
		    BufPosOffset	= 8,  // int4
		    ParamNoOffset   = 8,  // int2
		    ReadOnlyOffset  = 10, // int1
		    SerialOffset    = 11, // int1
		    // The size of tsp1_param_info 
		    END			    = 12;
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
        public const int 
            Header_Length = 20,
            RowDef = 0,
            TabHandle = 4,

            TabHandle_Reserved = 4,
            TabHandle_Rowsize = 8,
            TabHandle_ColCount = 10,
            TabHandle_RowCount = 12,
            TabHandle_ABAPTabID = 16,
            TabHandle_ColDesc_0 = 20,

            ColDesc_InOut = 0,
            ColDesc_ABAPType = 1,
            ColDesc_Dec = 2,
            ColDesc_Length = 4,
            ColDesc_Offset = 6,

            ColDesc_Size = 8,

            StreamType_Bool = 0,
            StreamType_Int1 = 1,
            StreamType_UInt1 = 2,
            StreamType_Int2 = 3,
            StreamType_UInt2 = 4,
            StreamType_Int4 = 5,
            StreamType_UInt4 = 6,
            StreamType_Int8 = 7,
            StreamType_UInt8 = 8,
            StreamType_Float = 9,
            StreamType_Double = 10,
            StreamType_Int = 12,
            StreamType_UInt = 13,
            StreamType_Char = 14,
            StreamType_WChar = 15,
            StreamType_UDT = 16,
            StreamType_WYDE = 17,

            Stream_IN = 0,
            Stream_OUT = 1,
            Stream_INOUT = 2;
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

    internal class ConnectionStringParams
    {
        public const string
            DATA_SOURCE = "DATA SOURCE",
            INITIAL_CATALOG = "INITIAL CATALOG",
            USER_ID = "USER ID",
            PASSWORD = "PASSWORD",
            TIMEOUT = "TIMEOUT",
            SPACE_OPTION = "SPACE OPTION",
            CACHE = "CACHE",
            CACHE_LIMIT = "CACHE LIMIT",
            CACHE_SIZE = "CACHE SIZE",
            ENCRYPT = "ENCRYPT",
            MODE = "MODE",
            SSL_HOST = "SSL HOST";
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

		public static readonly int UnicodeWidth = System.Text.Encoding.Unicode.GetByteCount(" ");
		public const int FillBufSize = 1024;
		public const string BlankChar = " ";
		public static readonly byte[] ZeroBytes = new byte [FillBufSize];
		public static readonly byte[] BlankBytes = new byte[FillBufSize];
		public static readonly byte[] BlankUnicodeBytes = new byte[FillBufSize * UnicodeWidth];

		public const int AlignValue	= 8;

		public const int ReserveFor2ndSegment = 8192; //8kB reserve size in order packet if more than 1 segment will be used
		public const int ReserveForReply = SegmentHeaderOffset.Part - PartHeaderOffset.Data + 200;
		public const int DefaultMaxNumberOfSegm = 6; //default maximum number of segments for a request packet
        public const int ResultCountSize = 6;

		public const string AppID = "ODB";
		public const string AppVersion = "70400";//"10100";
		public static readonly bool IsLittleEndian = BitConverter.IsLittleEndian;
		public const string CursorPrefix = "ADONET_CURSOR_";
		public const string TimeStampFormat = "yyyy-MM-dd hh:mm:ss.ffffff";

		static Consts()
		{
			for (int i = 0; i < FillBufSize; i += UnicodeWidth) 
			{
				ZeroBytes[i] = ZeroBytes [i + 1] = 0;
				BlankBytes[i] = BlankBytes[i + 1] = Encoding.ASCII.GetBytes(BlankChar)[0];
                if (IsLittleEndian)
				    Encoding.Unicode.GetBytes(BlankChar).CopyTo(BlankUnicodeBytes, i);
                else
				    Encoding.BigEndianUnicode.GetBytes(BlankChar).CopyTo(BlankUnicodeBytes, i);
			}
		}

		public static string ToHexString(byte[] array, int offset, int length)
		{
			if (array != null)
			{
				StringBuilder result = new StringBuilder((array.Length - offset) * 2);
				for(int i = offset; i < array.Length && i < length; i++)
					result.Append(array[i].ToString("X2"));

				return result.ToString();
			}
			else
				return "NULL";
		}

		public static string ToHexString(byte[] array, int offset)
		{
			return ToHexString(array, offset, array.Length);
		}

		public static string ToHexString(byte[] array)
		{
			return ToHexString(array, 0, array.Length);
		}
	}
}
